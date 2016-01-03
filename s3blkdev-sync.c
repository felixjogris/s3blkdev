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
#include <gnutls/gnutls.h>
#include <gnutls/crypto.h>
#include <snappy-c.h>

#include "s3blkdev.h"

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

enum eviction_mode {
  SYNC_ONLY,
  DELETE_IF_EQUAL,
  SYNC_AND_DELETE
};

struct chunk_entry {
  time_t atime;
  char name[17];
};

int running = 1;

char buf[COMPR_CHUNKSIZE], compbuf[COMPR_CHUNKSIZE];

#if 0
/* demo, stores chunk not in S3, but in /var/tmp/<cachedir>.store */
static void sync_chunk (struct config *cfg, struct device *dev, char *name,
                        enum eviction_mode evict)
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

  fd = openat(dir_fd, name,
              (evict == SYNC_ONLY ? O_RDONLY : O_RDWR) | O_NOATIME);
  if (fd < 0) {
    logwarn("open(): %s/%s", dev->cachedir, name);
    goto ERROR1;
  }

  flk.l_type = (evict == SYNC_ONLY ? F_RDLCK : F_WRLCK);
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

  if (!equal && (evict != DELETE_IF_EQUAL)) {
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

  if ((equal && (evict == DELETE_IF_EQUAL)) || (evict == SYNC_AND_DELETE)) {
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
#endif

static void sync_chunk (struct config *cfg, struct device *dev, char *name,
                        enum eviction_mode evict)
{
  int dir_fd, fd, equal, res;
  struct flock flk;
  struct stat st;
  size_t comprlen;
  unsigned char local_md5[16], remote_md5[16];
  struct s3connection *s3conn;
  unsigned short code;
  size_t contentlen;
  static unsigned int conn_num = 0;
  const char *err_str;

  dir_fd = open(dev->cachedir, O_RDONLY|O_DIRECTORY);
  if (dir_fd < 0) {
    logwarn("open(): %s", dev->cachedir);
    goto ERROR;
  }

  /* open and lock chunk */
  fd = openat(dir_fd, name,
              (evict == SYNC_ONLY ? O_RDONLY : O_RDWR) | O_NOATIME);
  if (fd < 0) {
    logwarn("open(): %s/%s", dev->cachedir, name);
    goto ERROR1;
  }

  flk.l_type = (evict == SYNC_ONLY ? F_RDLCK : F_WRLCK);
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
    /* chunk is being fetched by s3blkdev */
    logwarnx("%s/%s: filesize %lu != CHUNKSIZE",
             dev->cachedir, name, st.st_size);
    goto ERROR2;
  }

  /* read chunk */
  if (read(fd, buf, CHUNKSIZE) != CHUNKSIZE) {
    logwarn("read(): %s/%s", dev->cachedir, name);
    goto ERROR2;
  }

  /* uncompress chunk */
  comprlen = sizeof(compbuf);
  res = snappy_compress(buf, CHUNKSIZE, compbuf, &comprlen);
  if (res != SNAPPY_OK) {
    logwarnx("snappy_compress(): %s/%s: %i", dev->cachedir, name, res);
    goto ERROR2;
  }

  /* get md5 of chunk */
  res = gnutls_hash_fast(GNUTLS_DIG_MD5, compbuf, comprlen, local_md5);
  if (res != GNUTLS_E_SUCCESS) {
    logwarnx("gnutls_hash_fast(): %s", gnutls_strerror(res));
    goto ERROR2;
  }

  /* fetch md5 (etag) */
  s3conn = s3_get_conn(cfg, &conn_num, &err_str);
  if (s3conn == NULL) {
    logwarnx("s3_get_conn(): %s", err_str);
    goto ERROR2;
  }

  res = s3_request(cfg, s3conn, &err_str, HEAD, dev->name, name, NULL, 0,
                   local_md5, &code, &contentlen, remote_md5, buf,
                   sizeof(buf));
  if (res != 0) {
    logwarnx("s3_request(): %s/%s/%s/%s: %s", s3conn->host, s3conn->bucket,
            dev->name, name, err_str);
    goto ERROR3;
  }

  if (code == 200) {
    /* found chunk, compare md5 checksum to local one */
    equal = !memcmp(local_md5, remote_md5, 16);
  } else if (code == 404) {
    /* chunk not found */
    equal = 0;
  } else {
    logwarnx("s3_request(): %s/%s/%s/%s: HTTP status %hu", s3conn->host,
            s3conn->bucket, dev->name, name, code);
    goto ERROR3;
  }

  if (!equal && (evict != DELETE_IF_EQUAL)) {
    /* upload chunk */
    s3_release_conn(s3conn);

    s3conn = s3_get_conn(cfg, &conn_num, &err_str);
    if (s3conn == NULL) {
      logwarnx("s3_get_conn(): %s", err_str);
      goto ERROR2;
    }

    res = s3_request(cfg, s3conn, &err_str, PUT, dev->name, name, compbuf,
                     comprlen, local_md5, &code, &contentlen, remote_md5, buf,
                     sizeof(buf));
    if (res != 0) {
      logwarnx("s3_request(): %s/%s/%s/%s: %s", s3conn->host, s3conn->bucket,
              dev->name, name, err_str);
      goto ERROR3;
    }

    if (code != 200) {
      logwarnx("s3_request(): %s/%s/%s/%s: HTTP status %hu", s3conn->host,
              s3conn->bucket, dev->name, name, code);
      goto ERROR3;
    }

    syslog(LOG_INFO, "synced %s/%s\n", dev->cachedir, name);
  }

  if ((equal && (evict == DELETE_IF_EQUAL)) || (evict == SYNC_AND_DELETE)) {
    if (unlinkat(dir_fd, name, 0) != 0) {
      logwarn("unlinkat(): %s/%s", dev->cachedir, name);
      goto ERROR3;
    }

    syslog(LOG_INFO, "evicted %s/%s\n", dev->cachedir, name);
    *name = '\0';
  }

ERROR3:
  s3_release_conn(s3conn);

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
                           size_t *num_chunks, size_t *size_chunks)
{
  DIR *dir;
  struct dirent *entry;
  struct stat st;
  int result = -1;

  dir = opendir(cachedir);
  if (dir == NULL) {
    logwarn("opendir(): %s", cachedir);
    goto ERROR;
  }

  /* read cachedir, save name and access time of each chunk */
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

    if (*num_chunks >= *size_chunks) {
      *size_chunks = *size_chunks * 2 + 4096;
      *chunks = realloc(*chunks, sizeof(struct chunk_entry) * *size_chunks);
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

  /* eviction needed if used space or inodes are above max_used_pct */
  return ((fs.f_bavail * 100 / fs.f_blocks < min_free_pct) ||
          (fs.f_ffree * 100 / fs.f_files < min_free_pct));
}

/* qsort() callback, sort by ascending access times */
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

  if (sigdelset(&sigset, SIGTERM) != 0)
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
"s3blkdev-sync V" S3BLKDEV_VERSION "\n"
"\n"
"Usage:\n"
"\n"
"s3blkdev-sync [-c <config file>] -p <pid file> <runtime_seconds>\n"
"              [<start_pct> <stop_pct>]\n"
"s3blkdev-sync [-c <config file>] -p <pid file> <max_used_pct> <min_used_pct>\n"
"              [<start_pct> <stop_pct>]\n"
"s3blkdev-sync -h\n"
"\n"
"  -c <config file>    read config options from specified file instead of\n"
"                      " DEFAULT_CONFIGFILE "\n"
"  -p <pid file>       save pid to this file\n"
"  <runtime_seconds>   run in sync mode: upload any chunks which have been\n"
"                      modified locally, stop after <runtime_seconds>\n"
"  <max_used_pct>      run in eviction mode: if cache directory has more than\n"
"                      <max_used_pct> percent diskspace in use, first upload,\n"
"                      then delete chunks locally\n"
"  <min_used_pct>      stop eviction if cache directory has no more than\n"
"                      <min_used_pct> percent diskspace in use\n"
"  <start_pct>         work on a subset of all chunks when running multiple\n"
"                      instances of s3blkdev-sync; defaults to 0\n"
"  <stop_pct>          stop after (stop_pct - start_pct)% of all chunks has\n"
"                      been handled; defaults to 100\n"
"  -h                  show this help ;-)\n"
);
}

int main (int argc, char **argv)
{
  int res;
  size_t num_chunks, size_chunks = 0, i, start, stop;
  struct chunk_entry *chunks = NULL;
  enum { SYNCER, EVICTOR } mode;
  unsigned int min_used_pct = 100, max_used_pct = 100, errline, devnum,
               runtime_seconds = 0, deleted_chunks, start_pct = 0,
               stop_pct = 100;
  char *configfile = DEFAULT_CONFIGFILE, *pidfile = NULL;
  const char *errstr;
  struct config cfg;
  struct device *dev;
  time_t start_time;

  openlog("s3blkdev-sync", LOG_NDELAY|LOG_PID, LOG_LOCAL1);

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
    case 3:
      start_pct = atoi(argv[optind + 1]);
      stop_pct = atoi(argv[optind + 2]);
      /* fall thru */
    case 1:
      mode = SYNCER;
      runtime_seconds = atoi(argv[optind]);
      break;
    case 4:
      start_pct = atoi(argv[optind + 2]);
      stop_pct = atoi(argv[optind + 3]);
      /* fall thru */
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

  if ((start_pct > 100) || (stop_pct > 100) || (stop_pct < start_pct))
    errdiex("Parameters start_pct and stop_pct have wrong values. "
            "See -h for help.");

  if (load_config(configfile, &cfg, &errline, &errstr) != 0)
    errdiex("Cannot load config file %s: %s (line %i)",
            configfile, errstr, errline);

  if ((res = gnutls_global_init()) != GNUTLS_E_SUCCESS)
    errdiex("gnutls_global_init(): %s", gnutls_strerror(res));

  if (save_pidfile(pidfile) != 0)
    errdie("Cannot save pidfile %s", pidfile);

  setup_signals();

  for (devnum = 0; devnum < cfg.num_devices; devnum++) {
    dev = &cfg.devs[devnum];

    if (read_cache_dir(dev->cachedir, &chunks, &num_chunks, &size_chunks) != 0)
      continue;

    qsort(chunks, num_chunks, sizeof(chunks[0]), compare_atimes);

    start = (num_chunks * start_pct) / 100;
    stop = (num_chunks * stop_pct) / 100;

    if (mode == SYNCER) {
      /* just upload modified chunks, start with chunk that has been modified
         most recently */
      start_time = time(NULL);

      i = stop;

      while ((i > start) && running) {
        i--;
        sync_chunk(&cfg, dev, chunks[i].name, SYNC_ONLY);

        if (time(NULL) - start_time >= runtime_seconds)
          break;
      }
    } else if (eviction_needed(dev->cachedir, max_used_pct)) {
      /* first round of eviction, delete local chunks which have already been
         uploaded */
      deleted_chunks = 0;

      for (i = start; (i < stop) && running; i++) {
        if (!eviction_needed(dev->cachedir, min_used_pct))
          break;

        sync_chunk(&cfg, dev, chunks[i].name, DELETE_IF_EQUAL);

        if (++deleted_chunks >= 100)
          break;
      }

      /* second round of eviction, upload and delete local chunks until
         free space is below given percentage */
      for (i = start; (i < stop) && running; i++) {
        /* chunk was deleted during first round of eviction */
        if (chunks[i].name[0] == '\0')
          continue;

        if (eviction_needed(dev->cachedir, min_used_pct))
          sync_chunk(&cfg, dev, chunks[i].name, SYNC_AND_DELETE);
        else
          break;
      }
    }
  }

  gnutls_global_deinit();

  if (unlink(pidfile) != 0)
    errdie("unlink(): %s", pidfile);

  closelog();

  return 0;
}
