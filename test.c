#include <stdio.h>
#include <string.h>
#include <err.h>
#include <gnutls/gnutls.h>
#include <gnutls/crypto.h>
#include <nettle/base64.h>

#include "s3nbd.h"

char buffer[4096*2048];

int main ()
{
  char *configfile = "s3nbd.conf.nogit";
  struct config cfg;
  const char *err_str;
  unsigned int err_line, conn_num;
  struct s3connection *s3conn;
  int res;
  unsigned short code;
  size_t contentlen;
  unsigned char md5[16];
  char *content = "hello, world!";

  if (load_config(configfile, &cfg, &err_line, &err_str) != 0)
    errx(1, "%s: %s (line %u)", configfile, err_str, err_line);

  if (gnutls_global_init() != GNUTLS_E_SUCCESS)
    errx(1, "gnutls_global_init()");

  conn_num = 5001;
  s3conn = s3_get_conn(&cfg, &conn_num, &err_str);
  if (s3conn == NULL)
    errx(1, "get_s3conn(): %s", err_str);

  res = gnutls_hash_fast(GNUTLS_DIG_MD5, content, strlen(content), md5);
  if (res != GNUTLS_E_SUCCESS)
    errx(1, "gnutls_hash_fast(): %s", gnutls_strerror(res));

  res = s3_request(&cfg, s3conn, &err_str, "GET", "folder1", "test.txt",
                   content, strlen(content), md5, &code, &contentlen, md5,
                   buffer, sizeof(buffer));
  fprintf(stderr, "res=%i code=%hu contentlen=%lu err_str=%s md5=0x",
          res, code, contentlen, err_str);

  for (res = 0; res < 16; res++)
    fprintf(stderr, "%hhx", md5[res]);
  fputs("\n", stderr);

  s3_release_conn(s3conn);
  gnutls_global_deinit();

  write(1, buffer, contentlen);
  write(1, "\n", 1);

  return 0;
}
