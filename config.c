#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#include "s3nbd.h"

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
  struct config candidate;
  struct stat st;
  FILE *fh;
  int result = -1, pos;
  char line[1024];
  unsigned short dev;

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

  memset(&candidate, 0, sizeof(candidate));

  while (fgets(line, sizeof(line), fh) != NULL) {
    *err_line += 1;

    if (is_incomplete(line)) {
      *errstr = "incomplete line (no terminating newline)";
      goto ERROR;
    }

    if (is_comment(line) || is_empty(line))
      continue;

    if (sscanf(line, " listen %127s", cfg->listen) ||
        sscanf(line, " port %7s", cfg->port) ||
        sscanf(line, " workers %hu", &cfg->num_io_threads) ||
        sscanf(line, " fetchers %hu", &cfg->num_s3_fetchers))
      continue;

    if (sscanf(line, " dev.%hu%n", &dev, &pos)) {
      if (dev >= sizeof(cfg->devs)/sizeof(cfg->devs[0])) {
        *errstr = "too many devices";
        goto ERROR;
      }
      if (sscanf(line + pos, ".name %127s", cfg->devs[dev].name) ||
          sscanf(line + pos, ".cachedir %4095s", cfg->devs[dev].cachedir) ||
          sscanf(line + pos, ".size %lu", &cfg->devs[dev].size))
        continue;
    }

    *errstr = "unknown configuration directive";
    goto ERROR;
  }

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
