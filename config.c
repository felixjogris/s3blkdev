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
  char line[1024], devname[DEVNAME_SIZE];

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
        sscanf(line, " port %7s", newcfg.port))
      continue;

    if (sscanf(line, " workers %hu", &newcfg.num_io_threads)) {
      if (newcfg.num_io_threads == 0) {
        *errstr = "number of workers must not be zero";
        goto ERROR1;
      }
      if (newcfg.num_io_threads >= MAX_IO_THREADS) {
        *errstr = "number of workers too large (max. " STR(MAX_IO_THREADS) ")";
        goto ERROR1;
      }
      continue;
    }

    if (sscanf(line, " fetchers %hu", &newcfg.num_s3_fetchers)) {
      if (newcfg.num_s3_fetchers == 0) {
        *errstr = "number of fetchers must not be zero";
        goto ERROR1;
      }
      continue;
    }

    if (sscanf(line, " [%63[^]]", devname)) {
      if (newcfg.num_devices >= sizeof(newcfg.devs)/sizeof(newcfg.devs[0])) {
        *errstr = "too many devices";
        goto ERROR1;
      }

      strncpy(newcfg.devs[newcfg.num_devices].name, devname,
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
  FILE *fh;
  char pid[16];
  struct flock flk;

  if ((fh = fopen(pidfile, "r+")) == NULL)
    return -1;

  fprintf(fh, "%u\n", getpid());

  flk.l_type = F_WRLCK;
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = strlen(pid);
  flk.l_pid = 0;

  if (fcntl(fileno(fh), F_OFD_SETLK, &flk) != 0)
    return -1;
 
  return 0;
}
