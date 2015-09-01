#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <string.h>

#define __USE_GNU
#include <fcntl.h>

#include "s3nbd.h"

struct chunk_entry {
  time_t atime;
  char name[17];
};

char buf1[CHUNKSIZE], buf2[CHUNKSIZE];

int _syncer_sync_chunk (int dir_fd, char *name, int copy)
{
  int fd, storedir_fd, store_fd, flags, result = -1;
  struct flock flk;
  struct stat st;

  fd = openat(dir_fd, name, O_RDONLY|O_NOATIME);
  if (fd < 0) {
    warn("open(): %s", name);
    goto ERROR;
  }

  flk.l_type = F_RDLCK;
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

  if ((storedir_fd = open("/var/tmp/s3nbd2", O_RDONLY|O_DIRECTORY)) < 0) {
    warn("open()");
    goto ERROR1;
  }

  flags = (copy ? O_CREAT|O_RDWR : O_RDONLY);
  store_fd = openat(storedir_fd, name, flags, S_IRUSR|S_IWUSR);
  if (store_fd < 0) {
    if (copy)
      warn("openat()");
    else
      result = 0;
    goto ERROR2;
  }

  if (read(fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
    warn("read()");
    goto ERROR3;
  }

  if (copy) {
    if (write(store_fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
      warn("write()");
      goto ERROR3;
    }
    result = 0;
    printf("synced %s\n", name);
  } else {
    if (read(store_fd, buf2, sizeof(buf2)) != sizeof(buf2)) {
      warn("read()");
      goto ERROR3;
    }
    result = (memcmp(buf1, buf2, sizeof(buf1)) == 0);
  }

ERROR3:
  if (close(store_fd) < 0) {
    warn("close()");
    result = -1;
  }

ERROR2:
  if (close(storedir_fd) < 0) {
    warn("close()");
    result = -1;
  }

ERROR1:
  if (close(fd) < 0) {
    warn("close(): %s", name);
    result = -1;
  }

ERROR:
  return result;
}

int open_cache_dir (char *dirname)
{
  int dir_fd;

  dir_fd = open(dirname, O_RDONLY|O_DIRECTORY);
  if (dir_fd < 0)
    err(1, "open()");

  return dir_fd;
}

int syncer_is_chunk_dirty (int dir_fd, char *name)
{
  return (_syncer_sync_chunk(dir_fd, name, 0) == 0);
}

void syncer_sync_chunk (int dir_fd, char *name)
{
  _syncer_sync_chunk(dir_fd, name, 1);
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

    if (!syncer_is_chunk_dirty(dir_fd, entry->d_name))
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

int compare_atimes (const void *a0, const void *b0)
{
  struct chunk_entry *a = (struct chunk_entry*) a0;
  struct chunk_entry *b = (struct chunk_entry*) b0;

  if (a->atime == b->atime) return 0;
  return (a->atime < b->atime ? -1 : 1);
}

int main ()
{
  int dir_fd;
  size_t num_chunks, i;
  struct chunk_entry *chunks = NULL;

  dir_fd = open_cache_dir("/var/tmp/s3nbd");
  num_chunks = read_cache_dir(dir_fd, &chunks);
  qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

  for (i = 0; i < num_chunks; i++)
    syncer_sync_chunk(dir_fd, chunks[i].name);

  free(chunks);
  close(dir_fd);

  return 0;
}
