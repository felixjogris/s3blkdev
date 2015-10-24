#include <unistd.h>
#include <err.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mount.h>

#define NBD_SET_FLAGS _IO( 0xab, 10 )

/* set an NBD device read+write */
int main (int argc, char **argv)
{
  char *device = argv[1];
  int nbd;
  int flags;

  if (argc != 2) errx(1, "usage: nbdrw </dev/nbdX>");

  if ((nbd = open(device, O_RDWR)) < 0) err(1, device);

  flags = 5; /* opts available + flush command supported */
  if (ioctl(nbd, NBD_SET_FLAGS, flags) != 0) err(1, "NBD_SET_FLAGS");

  flags = 0;
  if (ioctl(nbd, BLKROSET, &flags) != 0) err(1, "BLKROSET");

  return close(nbd);
}
