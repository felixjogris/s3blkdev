#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include "s3nbd.h"

#define XSTR(a) #a
#define STR(a) XSTR(a)

int is_comment (char *line)
{
  return ((line[0] == '#') || (line[0] == ';'));
}

int is_empty (char *line)
{
  char *p;

  for (p = line; *p != '\0'; p++)
    if (isspace(*p))
      return 0;

  return 1;
}

int is_incomplete (char *line)
{
  return ((strlen(line) == 0) || (line[strlen(line) - 1] != '\n'));
}

int load_config (char *configfile, struct config *cfg,
                 unsigned int *err_line, char **errstr)
{
  struct config newcfg;
  struct stat st;
  FILE *fh;
  int result = -1, pos, in_device = 0;
  char line[1024];

  *err_line = 0;

  if (stat(configfile, &st) != 0) {
    *errstr = strerror(errno);
    return -1;
  }

  if ((st.st_ctim.tv_sec == cfg->ctime.tv_sec) &&
      (st.st_ctim.tv_nsec == cfg->ctime.tv_nsec))
    return 1;

  if ((fh = fopen(configfile, "r")) == NULL) {
    *errstr = strerror(errno);
    return -1;
  }

  memset(&newcfg, 0, sizeof(newcfg));

  while (fgets(line, sizeof(line), fh) != NULL) {
    *err_line += 1;

    if (is_incomplete(line)) {
      *errstr = "incomplete line (no terminating newline)";
      goto ERROR;
    }

    if (is_comment(line) || is_empty(line))
      continue;

    if (in_device == 7) {
      newcfg.num_devices++;
      in_device = 0;
    }

    if (in_device) {
      if (sscanf(line, "cachedir %4095s",
                 newcfg.devs[newcfg.num_devices].cachedir)) {
        in_device |= 2;
        continue;
      }

      if (sscanf(line, "size %lu", &newcfg.devs[newcfg.num_devices].size)) {
        in_device |= 4;
        continue;
      }

      *errstr = "unknown device parameter";
      goto ERROR;
    }

    if (sscanf(line, " listen %127s", newcfg.listen) ||
        sscanf(line, " port %7s", newcfg.port))
      continue;

    if (sscanf(line, " workers %hu", &newcfg.num_io_threads)) {
      if (newcfg.num_io_threads == 0) {
        *errstr = "number of workers must not be zero";
        goto ERROR;
      }
      if (newcfg.num_io_threads >= MAX_IO_THREADS) {
        *errstr = "number of workers too large (max. " STR(MAX_IO_THREADS) ")";
        goto ERROR;
      }
      continue;
    }

    if (sscanf(line, " fetchers %hu", &newcfg.num_s3_fetchers)) {
      if (newcfg.num_s3_fetchers == 0) {
        *errstr = "number of fetchers must not be zero";
        goto ERROR;
      }
      continue;
    }

    if (sscanf(line, " [%n", &pos)) {
      if (newcfg.num_devices >= sizeof(newcfg.devs)/sizeof(newcfg.devs[0])) {
        *errstr = "too many devices";
        goto ERROR;
      }

      if (!sscanf(line + pos, "%127[^[]",
                  newcfg.devs[newcfg.num_devices].name)) {
        *errstr = "invalid device name";
        goto ERROR;
      }

      in_device = 1;
      continue;
    }

    *errstr = "unknown configuration directive";
    goto ERROR;
  }

  newcfg.ctime.tv_sec = st.st_ctim.tv_sec;
  newcfg.ctime.tv_nsec = st.st_ctim.tv_nsec;
  memcpy(cfg, &newcfg, sizeof(*cfg));
  
  result = 0;

ERROR:
  if (!feof(fh)) {
    *errstr = strerror(errno);
    result = -1;
  }

  if (fclose(fh) != 0) {
    *errstr = strerror(errno);
    result = -1;
  }

  return result;
}
