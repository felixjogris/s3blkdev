#define _GNU_SOURCE

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
#include <syslog.h>
#include <signal.h>
#include <fcntl.h>

#include "s3nbd.h"
#include "snappy-c.h"

#define errdiex(fmt, params ...) { \
  syslog(LOG_ERR, "%s (%s:%i): " fmt "\n", \
         __FUNCTION__, __FILE__, __LINE__, ## params); \
  errx(1, "%s (%s:%i): " fmt, \
       __FUNCTION__, __FILE__, __LINE__, ## params); \
} while (0)

#define errdie(fmt, params ...) \
  errdiex(fmt ": %s", ## params, strerror(errno))

#define logwarnx(fmt, params ...) { \
  syslog(LOG_WARNING, "%s (%s:%i): " fmt "\n", \
         __FUNCTION__, __FILE__, __LINE__, ## params); \
  warnx("%s (%s:%i): " fmt, \
        __FUNCTION__, __FILE__, __LINE__, ## params); \
} while (0)

#define logwarn(fmt, params ...) \
  logwarnx(fmt ": %s", ## params, strerror(errno))

struct chunk_entry {
  time_t atime;
  char name[17];
};

int running = 1;

char buf[COMPR_CHUNKSIZE], compbuf[COMPR_CHUNKSIZE];

static void sync_chunk (struct device *dev, char *name, int evict)
{
  int dir_fd, fd, storedir_fd, store_fd, equal, res;
  struct flock flk;
  struct stat st;
  char sdpath[PATH_MAX];
  size_t comprlen;

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

  if (fstat(fd, &st) != 0) {
    logwarn("fstat(): %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

  if (st.st_size != CHUNKSIZE) {
    logwarnx("%s/%s: filesize %lu != CHUNKSIZE",
             dev->cachedir, name, st.st_size);
    goto ERROR2;
  }

  if (read(fd, buf, CHUNKSIZE) != CHUNKSIZE) {
    logwarn("read(): %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

  comprlen = sizeof(compbuf);
  res = snappy_compress(buf, CHUNKSIZE, compbuf, &comprlen);
  if (res != SNAPPY_OK) {
    logwarnx("snappy_compress(): %s/%s: %i", dev->cachedir, name, res);
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
  } else if (fstat(store_fd, &st) != 0) {
    logwarn("fstat(): %s/%s", sdpath, name);
    goto ERROR4;
  } else if (read(store_fd, buf, st.st_size) != st.st_size) {
    logwarn("read(): %s/%s", sdpath, name);
    goto ERROR4;
  } else {
    equal = ((st.st_size == (ssize_t) comprlen) &&
             (memcmp(buf, compbuf, comprlen) == 0));
  }

  if (!equal && (evict != 1)) {
    if ((store_fd >= 0) && (close(store_fd) < 0)) {
      logwarn("close(): %s/%s", sdpath, name);
      goto ERROR3;
    }

    store_fd = openat(storedir_fd, name, O_WRONLY|O_CREAT|O_TRUNC,
                      S_IRUSR|S_IWUSR|S_IRGRP);
    if (store_fd < 0) {
      logwarn("openat(): %s/%s", sdpath, name);
      goto ERROR3;
    }

    if (write(store_fd, compbuf, comprlen) != (ssize_t) comprlen) {
      logwarn("write(): %s/%s", sdpath, name);
      goto ERROR4;
    }

    syslog(LOG_INFO, "synced %s/%s\n", dev->cachedir, name);
  }

  if ((equal && (evict == 1)) || (evict == 2)) {
    if (unlinkat(dir_fd, name, 0) != 0) {
      logwarn("unlinkat(): %s/%s", dev->cachedir, name);
      goto ERROR4;
    }

    syslog(LOG_INFO, "evicted %s/%s\n", dev->cachedir, name);
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
      *chunks = realloc(*chunks, sizeof(struct chunk_entry) * size_chunks);
      if (*chunks == NULL)
        errdiex("realloc() failed");
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

static void sigterm_handler (int sig __attribute__((unused)))
{
  syslog(LOG_INFO, "SIGTERM received, going down...\n");
  running = 0;
}

static void setup_signals ()
{
  sigset_t sigset;
  struct sigaction sa;

  if (sigfillset(&sigset) != 0)
    errdie("sigfillset()");

  if ((sigdelset(&sigset, SIGTERM) != 0) || (sigdelset(&sigset, SIGHUP) != 0))
    errdie("sigdelset()");

  if (sigprocmask(SIG_SETMASK, &sigset, NULL) != 0)
    errdie("sigprocmask()");

  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = sigterm_handler;
  if (sigaction(SIGTERM, &sa, NULL) != 0)
    errdie("signal()");
}

static void show_help ()
{
  puts(
"Usage:\n"
"\n"
"syncer [-c <config file>] -p <pid file> [<max_used_pct> <min_used_pct>]\n"
"syncer -h\n"
"\n"
"  -c <config file>    read config options from specified file instead of\n"
"                      " DEFAULT_CONFIGFILE "\n"
"  -p <pid file>       save pid to this file\n"
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
  char *configfile = DEFAULT_CONFIGFILE, *pidfile = NULL, *errstr;
  struct config cfg;
  struct device *dev;

  openlog("syncer", LOG_NDELAY|LOG_PID, LOG_DAEMON);

  while ((res = getopt(argc, argv, "c:f:hp:")) != -1) {
    switch (res) {
      case 'c': configfile = optarg; break;
      case 'f': pidfile = optarg; break;
      case 'h': show_help(); return 0;
      case 'p': pidfile = optarg; break;
      default: errdiex("Unknown option '%i'. See -h for help.", res);
    }
  }

  if (pidfile == NULL)
    errdiex("Need pidfile. See -h for help.");

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
      errdiex("Wrong parameters. See -h for help.");
  }

  if (load_config(configfile, &cfg, &errline, &errstr) != 0)
    errdiex("Cannot load config file %s: %s (line %i)",
            configfile, errstr, errline);

  if (save_pidfile(pidfile) != 0)
    errdie("Cannot save pidfile %s", pidfile);

  setup_signals();

  for (devnum = 0; devnum < cfg.num_devices; devnum++) {
    dev = &cfg.devs[devnum];

    if (read_cache_dir(dev->cachedir, &chunks, &num_chunks) != 0)
      continue;

    qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

    if (mode == SYNCER) {
      for (l = num_chunks - 1; (l >= 0) && running; l--)
        sync_chunk(dev, chunks[l].name, 0);
    } else if (eviction_needed(dev->cachedir, max_used_pct)) {
      for (i = 0; (i < num_chunks) && running; i++) {
        if (eviction_needed(dev->cachedir, min_used_pct))
          sync_chunk(dev, chunks[i].name, 1);
        else
          break;
      }

      for (i = 0; (i < num_chunks) && running; i++) {
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

  if (unlink(pidfile) != 0)
    errdie("unlink(): %s", pidfile);

  closelog();

  return 0;
}
