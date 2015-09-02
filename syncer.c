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

#define logwarnx(fmt, params ...) warnx("%s [%s:%i]: " fmt, \
  __FUNCTION__, __FILE__, __LINE__, ## params)

#define logwarn(fmt, params ...) warn("%s [%s:%i]: " fmt, \
  __FUNCTION__, __FILE__, __LINE__, ## params)

struct chunk_entry {
  time_t atime;
  char name[17];
};

char buf1[CHUNKSIZE], buf2[CHUNKSIZE];

void sync_chunk (int dir_fd, char *name, int evict)
{
  int fd, storedir_fd, store_fd, equal;
  struct flock flk;
  struct stat st;

  fd = openat(dir_fd, name, (evict ? O_RDWR : O_RDONLY) | O_NOATIME);
  if (fd < 0) {
    logwarn("open(): %s", name);
    goto ERROR;
  }

  flk.l_type = (evict ? F_WRLCK : F_RDLCK);
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = CHUNKSIZE;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0) {
    logwarn("cannot lock %s", name);
    goto ERROR1;
  }

  if (fstatat(dir_fd, name, &st, 0) != 0) {
    logwarn("fstatat(): %s", name);
    goto ERROR1;
  }

  if (st.st_size != CHUNKSIZE) {
    logwarnx("%s: filesize != CHUNKSIZE", name);
    goto ERROR1;
  }

  if (read(fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
    logwarn("read(): %s", name);
    goto ERROR1;
  }

/* TODO */
  if ((storedir_fd = open("/var/tmp/s3nbd2", O_RDONLY|O_DIRECTORY)) < 0) {
    logwarn("open(): TODO");
    goto ERROR1;
  }

  store_fd = openat(storedir_fd, name, O_RDONLY);
  if (store_fd < 0) {
    equal = 0;
  } else if (read(store_fd, buf2, sizeof(buf2)) != sizeof(buf2)) {
    logwarn("read(): %s", name);
    goto ERROR3;
  } else {
    equal = (memcmp(buf1, buf2, sizeof(buf1)) == 0);
  }

  if (!equal && (evict != 1)) {
    if ((store_fd >= 0) && (close(store_fd) < 0)) {
      logwarn("close()");
      goto ERROR2;
    }

    store_fd = openat(storedir_fd, name, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
    if (store_fd < 0) {
      logwarn("openat(): %s", name);
      goto ERROR2;
    }

    if (write(store_fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
      logwarn("write(): %s", name);
      goto ERROR3;
    }

    printf("synced %s\n", name);
  }

  if ((equal && (evict == 1)) || (evict == 2)) {
    if (unlinkat(dir_fd, name, 0) != 0) {
      logwarn("unlinkat(): %s", name);
      goto ERROR3;
    }
    printf("evicted %s\n", name);
    *name = '\0';
  }

ERROR3:
  if ((store_fd >= 0) && (close(store_fd) < 0))
    logwarn("close()");

ERROR2:
  if (close(storedir_fd) < 0)
    logwarn("close()");

ERROR1:
  if (close(fd) < 0)
    logwarn("close(): %s", name);

ERROR:
  return;
}

int open_cache_dir (char *dirname)
{
  int dir_fd;

  dir_fd = open(dirname, O_RDONLY|O_DIRECTORY);
  if (dir_fd < 0)
    logwarn("open(): %s", dirname);

  return dir_fd;
}

size_t read_cache_dir (int dir_fd, struct chunk_entry **chunks)
{
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  size_t num_chunks = 0, size_chunks = 0;

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    logwarn("fdopendir()");
    return 0;
  }

  for (errno = 0; (entry = readdir(dir)) != NULL; errno = 0) {
    if ((entry->d_type != DT_REG) || (strlen(entry->d_name) != 16))
      continue;

    if (fstatat(dir_fd, entry->d_name, &st, 0) != 0) {
      if (errno != ENOENT) {
        logwarn("fstatat(): %s", entry->d_name);
        return 0;
      }
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

  if (errno != 0) {
    logwarn("readdir()");
    return 0;
  }

  return num_chunks;
}

int eviction_needed (int dir_fd, unsigned int max_used_pct)
{
  struct statfs fs;
  unsigned int min_free_pct = 100 - max_used_pct;

  if (fstatfs(dir_fd, &fs) != 0) {
    logwarn("fstatfs()");
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

void show_help ()
{
  puts(
"Usage:\n"
"\n"
"syncer [-c <config file>] [<max_used_pct> <min_used_pct>]\n"
"syncer -h\n"
"\n"
"  -c <config file>    read config options from specified file instead of\n"
"                      " DEFAULT_CONFIGFILE "\n"
"\n"
"  -h                  show this help ;-)\n"
);
}

int main (int argc, char **argv)
{
  int dir_fd, res;
  size_t num_chunks, i;
  long l;
  struct chunk_entry *chunks = NULL;
  enum { SYNCER, EVICTOR } mode;
  unsigned int min_used_pct, max_used_pct, errline, devnum;
  char *configfile = DEFAULT_CONFIGFILE, *errstr;
  struct config cfg;

  while ((res = getopt(argc, argv, "c:h")) != -1) {
    switch (res) {
      case 'c': configfile = optarg; break;
      case 'h': show_help(); return 0;
      default: errx(1, "Unknown option '%i'. Use -h for help.", res);
    }
  }

  switch (argc - optind) {
    case 0:
      mode = SYNCER;
      break;
    case 2:
      mode = EVICTOR;
      max_used_pct = atoi(argv[optind]);
      min_used_pct = atoi(argv[optind + 1]);
      if ((min_used_pct <= max_used_pct) && (max_used_pct <= 100))
        break;
      /* fall-thru */
    default:
      errx(1, "Wrong parameters. Use -h for help.");
  }

  cfg.ctime.tv_sec = 0;
  if (load_config(configfile, &cfg, &errline, &errstr) != 0)
    errx(1, "cannot load config file %s: %s (line %i)",
         configfile, errstr, errline);

  for (devnum = 0; devnum < cfg.num_devices; devnum++) {
    if ((dir_fd = open_cache_dir(cfg.devs[devnum].cachedir)) < 0)
      continue;

    num_chunks = read_cache_dir(dir_fd, &chunks);
    qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

    printf("cachedir %s contains %lu chunks\n",
           cfg.devs[devnum].cachedir,  num_chunks);

    if (mode == SYNCER) {
      for (l = num_chunks - 1; l >= 0; l--)
        sync_chunk(dir_fd, chunks[l].name, 0);
    } else if (eviction_needed(dir_fd, max_used_pct)) {
      for (i = 0; (i < num_chunks) && eviction_needed(dir_fd, min_used_pct); i++)
        sync_chunk(dir_fd, chunks[i].name, 1);

      for (i = 0; (i < num_chunks) && eviction_needed(dir_fd, min_used_pct); i++)
        if (chunks[i].name[0] != '\0')
          sync_chunk(dir_fd, chunks[i].name, 2);
    }

    free(chunks);
    close(dir_fd);
  }

  return 0;
}
