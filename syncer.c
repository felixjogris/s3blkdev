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

static void sync_chunk (struct device *dev, char *name, int evict)
{
  int dir_fd, fd, storedir_fd, store_fd, equal;
  struct flock flk;
  struct stat st;
  char sdpath[PATH_MAX];

  dir_fd = open(dev->cachedir, O_RDONLY|O_DIRECTORY);
  if (dir_fd < 0) {
    logwarn("open(): %s", dev->cachedir);
    goto ERROR;
  }

  fd = openat(dir_fd, name, (evict ? O_RDWR : O_RDONLY) | O_NOATIME);
  if (fd < 0) {
    logwarn("open(): %s/%s", dev->cachedir, name);
    goto ERROR1;
  }

  flk.l_type = (evict ? F_WRLCK : F_RDLCK);
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = CHUNKSIZE;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0) {
    logwarn("cannot lock %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

  if (fstatat(dir_fd, name, &st, 0) != 0) {
    logwarn("fstatat(): %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

  if (st.st_size != CHUNKSIZE) {
    logwarnx("%s/%s: filesize != CHUNKSIZE", dev->cachedir, name);
    goto ERROR2;
  }

  if (read(fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
    logwarn("read(): %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

/* TODO */
  snprintf(sdpath, sizeof(sdpath), "/var/tmp/%s.store", dev->name);
  if ((storedir_fd = open(sdpath, O_RDONLY|O_DIRECTORY)) < 0) {
    logwarn("open(): %s", sdpath);
    goto ERROR2;
  }

  store_fd = openat(storedir_fd, name, O_RDONLY);
  if (store_fd < 0) {
    equal = 0;
  } else if (read(store_fd, buf2, sizeof(buf2)) != sizeof(buf2)) {
    logwarn("read(): %s/%s", sdpath, name);
    goto ERROR4;
  } else {
    equal = (memcmp(buf1, buf2, sizeof(buf1)) == 0);
  }

  if (!equal && (evict != 1)) {
    if ((store_fd >= 0) && (close(store_fd) < 0)) {
      logwarn("close(): %s/%s", sdpath, name);
      goto ERROR3;
    }

    store_fd = openat(storedir_fd, name, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR);
    if (store_fd < 0) {
      logwarn("openat(): %s/%s", sdpath, name);
      goto ERROR3;
    }

    if (write(store_fd, buf1, sizeof(buf1)) != sizeof(buf1)) {
      logwarn("write(): %s/%s", sdpath, name);
      goto ERROR4;
    }

    printf("synced %s\n", name);
  }

  if ((equal && (evict == 1)) || (evict == 2)) {
    if (unlinkat(dir_fd, name, 0) != 0) {
      logwarn("unlinkat(): %s/%s", dev->cachedir, name);
      goto ERROR4;
    }
    printf("evicted %s\n", name);
    *name = '\0';
  }

ERROR4:
  if ((store_fd >= 0) && (close(store_fd) < 0))
    logwarn("close(): %s/%s", sdpath, name);

ERROR3:
  if (close(storedir_fd) < 0)
    logwarn("close(): %s", sdpath);

ERROR2:
  if (close(fd) < 0)
    logwarn("close(): %s/%s", dev->cachedir, name);

ERROR1:
  if (close(dir_fd) < 0)
    logwarn("close(): %s", dev->cachedir);

ERROR:
  return;
}

static int read_cache_dir (char *cachedir, struct chunk_entry **chunks,
                           size_t *num_chunks)
{
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  size_t size_chunks = 0;
  int result = -1;

  dir = opendir(cachedir);
  if (dir == NULL) {
    logwarn("opendir(): %s", cachedir);
    goto ERROR;
  }

  for (*num_chunks = 0, errno = 0; (entry = readdir(dir)) != NULL; errno = 0) {
    if ((entry->d_type != DT_REG) || (strlen(entry->d_name) != 16))
      continue;

    if (fstatat(dirfd(dir), entry->d_name, &st, 0) != 0) {
      if (errno != ENOENT) {
        logwarn("fstatat(): %s/%s", cachedir, entry->d_name);
        goto ERROR1;
      }
    }

    if (st.st_size != CHUNKSIZE)
      continue;

    if (*num_chunks >= size_chunks) {
      size_chunks += 4096;
      *chunks = realloc(*chunks, sizeof(chunks[0]) * size_chunks);
      if (*chunks == NULL)
        errx(1, "realloc() failed");
    }

    (*chunks)[*num_chunks].atime = st.st_atim.tv_sec;
    strncpy((*chunks)[*num_chunks].name, entry->d_name,
            sizeof((*chunks)[0].name));

    *num_chunks += 1;
  }

  result = 0;

  if (errno != 0) {
    logwarn("readdir(): %s", cachedir);
    result = -1;
  }

ERROR1:
  if (closedir(dir) != 0) {
    logwarn("closedir(): %s", cachedir);
    result = -1;
  }

ERROR:
  return result;
}

static int eviction_needed (char *cachedir, unsigned int max_used_pct)
{
  struct statfs fs;
  unsigned int min_free_pct = 100 - max_used_pct;

  if (statfs(cachedir, &fs) != 0) {
    logwarn("statfs(): %s", cachedir);
    return 0;
  }

  return ((fs.f_bavail * 100 / fs.f_blocks < min_free_pct) ||
          (fs.f_ffree * 100 / fs.f_files < min_free_pct));
}

static int compare_atimes (const void *a0, const void *b0)
{
  struct chunk_entry *a = (struct chunk_entry*) a0;
  struct chunk_entry *b = (struct chunk_entry*) b0;

  if (a->atime == b->atime) return 0;
  return (a->atime < b->atime ? -1 : 1);
}

static void show_help ()
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
  int res;
  size_t num_chunks, i;
  long l;
  struct chunk_entry *chunks = NULL;
  enum { SYNCER, EVICTOR } mode;
  unsigned int min_used_pct = 100, max_used_pct = 100, errline, devnum;
  char *configfile = DEFAULT_CONFIGFILE, *errstr;
  struct config cfg;
  struct device *dev;

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

  if (load_config(configfile, &cfg, &errline, &errstr) != 0)
    errx(1, "cannot load config file %s: %s (line %i)",
         configfile, errstr, errline);

  for (devnum = 0; devnum < cfg.num_devices; devnum++) {
    dev = &cfg.devs[devnum];

    if (read_cache_dir(dev->cachedir, &chunks, &num_chunks) != 0)
      continue;

    qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

    printf("cachedir %s contains %lu chunks\n",
           dev->cachedir,  num_chunks);

    if (mode == SYNCER) {
      for (l = num_chunks - 1; l >= 0; l--)
        sync_chunk(dev, chunks[l].name, 0);
    } else if (eviction_needed(dev->cachedir, max_used_pct)) {
      for (i = 0; i < num_chunks; i++) {
        if (eviction_needed(dev->cachedir, min_used_pct))
          sync_chunk(dev, chunks[i].name, 1);
        else
          break;
      }

      for (i = 0; i < num_chunks; i++) {
        if (chunks[i].name[0] == '\0')
          continue;
        if (eviction_needed(dev->cachedir, min_used_pct))
          sync_chunk(dev, chunks[i].name, 2);
        else
          break;
      }
    }

    free(chunks);
    chunks = NULL;
  }

  return 0;
}
