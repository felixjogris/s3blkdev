#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <err.h>

#include "s3blkdev.h"

/* lock a chunk file to hinder s3blkdev and/or syncer */

static char *lock_type_to_string (int type)
{
  switch (type) {
    case F_RDLCK: return "F_RDLCK";
    case F_WRLCK: return "F_WRLCK";
    default:      return "<UNKNOWN>";
  }
}

static char *lock_whence_to_string (int whence)
{
  switch (whence) {
    case SEEK_SET: return "SEEK_SET";
    case SEEK_CUR: return "SEEK_CUR";
    case SEEK_END: return "SEEK_END";
    default :      return "<UNKNOWN>";
  }
}

static int do_lock (int fd, int cmd, int type, off_t start, off_t end)
{
  int result;
  struct flock flk;

  flk.l_type = type;
  flk.l_whence = SEEK_SET;
  flk.l_start = start;
  flk.l_len = end - start;
  flk.l_pid = 0;

  if ((result = fcntl(fd, cmd, &flk)) != 0)
    warn("fcntl()");

  if ((cmd == F_OFD_GETLK) && (flk.l_type != F_UNLCK)) {
    printf("At least one lock is being held:\n\n"
           "Type:   %s\n"
           "Whence: %s\n"
           "Start:  %li\n"
           "Length: %li\n"
           "PID:    %i\n\n",
            lock_type_to_string(flk.l_type),
            lock_whence_to_string(flk.l_whence),
            flk.l_start, flk.l_len, flk.l_pid);
  }

  return result;
}

int main (int argc, char **argv)
{
  int fd, i, type, cmd;
  char buf[2], *chunk;
  off_t start, end;

  if (argc != 6)
    errx(1, "locktool V" S3BLKDEV_VERSION "\n\n"
            "Usage: locktool <g|l|w> <r|w> <start> <end> /path/to/chunk");

  switch (argv[1][0]) {
    case 'l': cmd = F_OFD_SETLK;  break; /* try to lock chunk file */
    case 'w': cmd = F_OFD_SETLKW; break; /* lock and possibly wait for lock */
    default:  cmd = F_OFD_GETLK;  break; /* report lock status */
  }

  type = (argv[2][0] == 'w' ? F_WRLCK : F_RDLCK);
  start = atol(argv[3]);
  end = atol(argv[4]);
  chunk = argv[5];
  
  fd = open(chunk, O_RDWR);
  if (fd < 0)
    err(1, "open(): %s", chunk);

  switch (cmd) {
    case F_OFD_SETLKW:
      do_lock(fd, F_OFD_GETLK, type, start, end);
      puts("Waiting for lock...");
      /* fall-thru */

    case F_OFD_SETLK:
      if (do_lock(fd, cmd, type, start, end) != 0)
        do_lock(fd, F_OFD_GETLK, type, start, end);
      else {
        printf("Acquired %s lock! Press [Enter] to release...\n",
               type == F_WRLCK ? "write" : "read");
        i = read(0, buf, sizeof(buf));
        i = i;
      }
      break;

    default:
      do_lock(fd, F_OFD_GETLK, type, start, end);
      break;
  }

  return 0;
}
