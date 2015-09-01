#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <string.h>
#include <sys/statfs.h>

#define __USE_GNU
#include <fcntl.h>

#include "s3nbd.h"

struct chunk_entry {
  time_t atime;
  char name[17];
};

char buf1[CHUNKSIZE], buf2[CHUNKSIZE];

void syncer_sync_chunk (int dir_fd, char *name, int evict)
{
  int fd, storedir_fd, store_fd, equal;
  struct flock flk;
  struct stat st;

  fd = openat(dir_fd, name, (evict ? O_RDWR : O_RDONLY) | O_NOATIME);
  if (fd < 0) {
    warn("open(): %s", name);
    goto ERROR;
  }

  flk.l_type = (evict ? F_WRLCK : F_RDLCK);
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = CHUNKSIZE;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0) {
    warn("cannot lock %s", name);
    goto ERROR1;
  }

  if (fstatat(dir_fd, name, &st, 0) != 0) {
    warn("fstatat(): %s", name);
    goto ERROR1;
  }

  if (st.st_size != CHUNKSIZE) {
    warnx("%s: filesize != CHUNKSIZE", name);
    goto ERROR1;
  }

  if (read(fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
    warn("read()");
    goto ERROR1;
  }

  if ((storedir_fd = open("/var/tmp/s3nbd2", O_RDONLY|O_DIRECTORY)) < 0) {
    warn("open()");
    goto ERROR1;
  }

  store_fd = openat(storedir_fd, name, O_RDONLY);
  if (store_fd < 0) {
    equal = 0;
  } else if (read(store_fd, buf2, sizeof(buf2)) != sizeof(buf2)) {
    warn("read()");
    goto ERROR3;
  } else {
    equal = (memcmp(buf1, buf2, sizeof(buf1)) == 0);
  }

  if (!equal && (evict != 1)) {
    if ((store_fd >= 0) && (close(store_fd) < 0)) {
      warn("close()");
      goto ERROR2;
    }

    store_fd = openat(storedir_fd, name, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
    if (store_fd < 0) {
      warn("openat()");
      goto ERROR2;
    }

    if (write(store_fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
      warn("write()");
      goto ERROR3;
    }

    printf("synced %s\n", name);
  }

  if ((equal && (evict == 1)) || (evict == 2)) {
    if (unlinkat(dir_fd, name, 0) != 0) {
      warn("unlinkat()");
      goto ERROR3;
    }
    printf("evicted %s\n", name);
    *name = '\0';
  }

ERROR3:
  if ((store_fd >= 0) && (close(store_fd) < 0))
    warn("close()");

ERROR2:
  if (close(storedir_fd) < 0)
    warn("close()");

ERROR1:
  if (close(fd) < 0)
    warn("close(): %s", name);

ERROR:
  return;
}

int open_cache_dir (char *dirname)
{
  int dir_fd;

  dir_fd = open(dirname, O_RDONLY|O_DIRECTORY);
  if (dir_fd < 0)
    err(1, "open()");

  return dir_fd;
}

size_t read_cache_dir (int dir_fd, struct chunk_entry **chunks)
{
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  size_t num_chunks = 0, size_chunks = 0;

  dir = fdopendir(dir_fd);
  if (dir == NULL)
    err(1, "fdopendir()");

  for (errno = 0; (entry = readdir(dir)) != NULL; errno = 0) {
    if ((entry->d_type != DT_REG) || (strlen(entry->d_name) != 16))
      continue;

    if (fstatat(dir_fd, entry->d_name, &st, 0) != 0) {
      if (errno == ENOENT)
        continue;

      err(1, "fstatat(): %s", entry->d_name);
    }

    if (st.st_size != CHUNKSIZE)
      continue;

    if (num_chunks >= size_chunks) {
      size_chunks += 4096;
      *chunks = realloc(*chunks, sizeof(chunks[0]) * size_chunks);
      if (*chunks == NULL)
        errx(1, "realloc() failed");
    }

    (*chunks)[num_chunks].atime = st.st_atim.tv_sec;
    strncpy((*chunks)[num_chunks].name, entry->d_name,
            sizeof((*chunks)[0].name));
    num_chunks++;
  }

  if (errno != 0)
    err(1, "readdir()");

  return num_chunks;
}

int eviction_needed (int dir_fd, unsigned int max_used_pct)
{
  struct statfs fs;
  unsigned int min_free_pct = 100 - max_used_pct;

  if (fstatfs(dir_fd, &fs) != 0) {
    warn("fstatfs()");
    return 0;
  }

  return ((fs.f_bavail * 100 / fs.f_blocks < min_free_pct) ||
          (fs.f_ffree * 100 / fs.f_files < min_free_pct));
}

int compare_atimes (const void *a0, const void *b0)
{
  struct chunk_entry *a = (struct chunk_entry*) a0;
  struct chunk_entry *b = (struct chunk_entry*) b0;

  if (a->atime == b->atime) return 0;
  return (a->atime < b->atime ? -1 : 1);
}

int main (int argc, char **argv)
{
  int dir_fd;
  size_t num_chunks, i;
  long l;
  struct chunk_entry *chunks = NULL;
  enum { SYNCER, EVICTOR } mode;
  unsigned int min_used_pct, max_used_pct;

  switch (argc) {
    case 1:
      mode = SYNCER;
      break;
    case 3:
      mode = EVICTOR;
      max_used_pct = atoi(argv[1]);
      min_used_pct = atoi(argv[2]);
      if ((min_used_pct <= max_used_pct) && (max_used_pct <= 100))
        break;
      /* fall-thru */
    default:
      errx(1, "Usage: syncer [<max_used_pct> <min_used_pct>]");
  }

  dir_fd = open_cache_dir("/var/tmp/s3nbd");
  num_chunks = read_cache_dir(dir_fd, &chunks);
  qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

  if (mode == SYNCER) {
    for (l = num_chunks - 1; l >= 0; l--)
      syncer_sync_chunk(dir_fd, chunks[l].name, 0);
  } else if (eviction_needed(dir_fd, max_used_pct)) {
    for (i = 0; (i < num_chunks) && eviction_needed(dir_fd, min_used_pct); i++)
      syncer_sync_chunk(dir_fd, chunks[i].name, 1);
    for (i = 0; (i < num_chunks) && eviction_needed(dir_fd, min_used_pct); i++)
      if (chunks[i].name[0] != '\0')
        syncer_sync_chunk(dir_fd, chunks[i].name, 2);
  }

  free(chunks);
  close(dir_fd);

  return 0;
}
