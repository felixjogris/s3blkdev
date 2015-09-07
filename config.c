#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#include "s3nbd.h"

#define XSTR(a) #a
#define STR(a) XSTR(a)

static int is_comment (char *line)
{
  return ((line[0] == '#') || (line[0] == ';'));
}

static int is_empty (char *line)
{
  char *p;

  for (p = line; *p != '\0'; p++)
    if (isspace(*p))
      return 0;

  return 1;
}

static int is_incomplete (char *line)
{
  return ((strlen(line) == 0) || (line[strlen(line) - 1] != '\n'));
}

int load_config (char *configfile, struct config *cfg,
                 unsigned int *err_line, char **errstr)
{
  struct config newcfg;
  FILE *fh;
  int result = -1, in_device = 0;
  char line[1024], tmp[256];

  *err_line = 0;

  if ((fh = fopen(configfile, "r")) == NULL) {
    *errstr = strerror(errno);
    goto ERROR;
  }

  memset(&newcfg, 0, sizeof(newcfg));

  while (fgets(line, sizeof(line), fh) != NULL) {
    *err_line += 1;

    if (is_incomplete(line)) {
      *errstr = "incomplete line (no terminating newline)";
      goto ERROR1;
    }

    if (is_comment(line) || is_empty(line))
      continue;

    if (in_device) {
      if (sscanf(line, "cachedir %4095s",
                 newcfg.devs[newcfg.num_devices].cachedir)) {
        in_device |= 2;
      } else if (sscanf(line, "size %lu",
                        &newcfg.devs[newcfg.num_devices].size)) {
        in_device |= 4;
      } else {
        *errstr = "unknown device parameter";
        goto ERROR1;
      }

      if (in_device == 7) {
        newcfg.num_devices++;
        in_device = 0;
      }

      continue;
    }

    if (sscanf(line, " listen %127s", newcfg.listen) ||
        sscanf(line, " port %7[0-9]", newcfg.port) ||
        sscanf(line, " workers %hu", &newcfg.num_io_threads) ||
        sscanf(line, " fetchers %hu", &newcfg.num_s3_fetchers) ||
        sscanf(line, " s3ssl %hhu", &newcfg.s3ssl) ||
        sscanf(line, " s3accesskey %127s", newcfg.s3accesskey) ||
        sscanf(line, " s3secretkey %127s", newcfg.s3secretkey))
      continue;

    if (sscanf(line, " s3host %255s", tmp)) {
      if (newcfg.num_s3hosts >= sizeof(newcfg.s3hosts)/sizeof(newcfg.s3hosts[0])) {
        *errstr = "too many S3 hosts";
        goto ERROR1;
      }

      strncpy(newcfg.s3hosts[newcfg.num_s3hosts], tmp,
              sizeof(newcfg.s3hosts[0]));
      newcfg.num_s3hosts++;

      continue;
    }

    if (sscanf(line, " s3port %7[0-9]", tmp)) {
      if (newcfg.num_s3ports >= sizeof(newcfg.s3ports)/sizeof(newcfg.s3ports[0])) {
        *errstr = "too many S3 ports";
        goto ERROR1;
      }

      strncpy(newcfg.s3ports[newcfg.num_s3ports], tmp,
              sizeof(newcfg.s3ports[0]));
      newcfg.num_s3ports++;

      continue;
    }

    if (sscanf(line, " [%63[^]]", tmp)) {
      if (newcfg.num_devices >= sizeof(newcfg.devs)/sizeof(newcfg.devs[0])) {
        *errstr = "too many devices";
        goto ERROR1;
      }

      strncpy(newcfg.devs[newcfg.num_devices].name, tmp,
              sizeof(newcfg.devs[0].name));
      in_device = 1;

      continue;
    }

    *errstr = "unknown configuration directive";
    goto ERROR1;
  }

  if (in_device) {
    *errstr = "incomplete device configuration";
    goto ERROR1;
  }

  if (newcfg.listen[0] == '\0') {
    *errstr = "no or empty listen statement";
    goto ERROR1;
  }

  if (newcfg.port[0] == '\0') {
    *errstr = "no or empty port statement";
    goto ERROR1;
  }

  if (newcfg.num_io_threads == 0) {
    *errstr = "number of workers must not be zero";
    goto ERROR1;
  }

  if (newcfg.num_io_threads >= MAX_IO_THREADS) {
    *errstr = "number of workers too large (max. " STR(MAX_IO_THREADS) ")";
    goto ERROR1;
  }

  if (newcfg.num_s3_fetchers == 0) {
    *errstr = "number of fetchers must not be zero";
    goto ERROR1;
  }

  if (newcfg.num_s3_fetchers > newcfg.num_io_threads) {
    *errstr = "number of fetchers must not exceed number of workers";
    goto ERROR1;
  }

  if (newcfg.num_s3hosts == 0) {
    *errstr = "no s3hosts";
    goto ERROR1;
  }

  if (newcfg.num_s3ports == 0) {
    if (!newcfg.s3ssl) {
      *errstr = "no s3ports and s3ssl not set";
      goto ERROR1;
    }
    newcfg.num_s3ports = 1;
    strcpy(newcfg.s3ports[0], "443");
  }

  if (newcfg.s3accesskey[0] == '\0') {
    *errstr = "no or empty s3accesskey statement";
    goto ERROR1;
  }

  if (newcfg.s3secretkey[0] == '\0') {
    *errstr = "no or empty s3secretkey statement";
    goto ERROR1;
  }

  memcpy(cfg, &newcfg, sizeof(*cfg));
  result = 0;

ERROR1:
  if (ferror(fh)) {
    *errstr = strerror(errno);
    result = -1;
  }

  if (fclose(fh) != 0) {
    *errstr = strerror(errno);
    result = -1;
  }

ERROR:
  return result;
}

int save_pidfile (char *pidfile)
{
  int fd, len;
  char pid[16];
  struct flock flk;

  fd = open(pidfile, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
  if (fd < 0)
    return -1;

  len = snprintf(pid, sizeof(pid), "%u\n", getpid());

  flk.l_type = F_WRLCK;
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = len;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0)
    return -1;
 
  if (write(fd, pid, len) != len)
    return -1;

  return 0;
}
