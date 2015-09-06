#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <snappy-c.h>

char inbuf[8 * 1024 * 1024];
char outbuf[2* 8 * 1024 * 1024];

int main ()
{
  size_t outlen;
  int ret;
  ssize_t len;

  if ((len=read(0, inbuf, 8*1024*1024)) <0)//!= sizeof(inbuf))
    return -1;

  outlen = sizeof(outbuf);
  ret = snappy_uncompress(inbuf, len, outbuf, &outlen);
  fprintf(stderr, "ret=%i inlen=%i outlen=%lu\n", ret, len, outlen);

  if (ret != SNAPPY_OK)
    return -1;

/*
  len = outlen;
  outlen = sizeof(inbuf);
  ret = snappy_uncompress(outbuf, len, inbuf, &outlen);
  fprintf(stderr, "ret=%i inlen=%i outlen=%lu\n", ret, len, outlen);
  if (write(1, outbuf, outlen) != (ssize_t) outlen)
    return -1;
*/

  return 0;
}
