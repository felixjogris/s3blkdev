#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <err.h>

#define __USE_GNU
#include <fcntl.h>

#include "s3nbd.h"

int main (int argc, char **argv)
{
  int fd;
  struct flock flk;
  struct stat st;
  char buf[16];

  if (argc != 5) errx(1, "usage: lock_chunk <r|w> <start> <end> /path/to/chunk");

  fd = open(argv[4], O_RDWR);
  if (fd < 0) err(1, "open()");

  flk.l_type = (argv[1][0] == 'r' ? F_RDLCK : F_WRLCK);
  flk.l_whence = SEEK_SET;
  flk.l_start = atol(argv[2]);
  flk.l_len = atol(argv[3]);
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0)
    err(1, "fcntl()");

  if (stat(argv[4], &st) != 0)
    warn("stat()");
  else if (st.st_size != CHUNKSIZE)
    warnx("filesize != CHUNKSIZE");

  read(0, buf, sizeof(buf));

  return 0;
}
