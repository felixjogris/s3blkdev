#include <err.h>
#include <gnutls/gnutls.h>
#include <nettle/base64.h>

#include "s3nbd.h"

int main ()
{
  char *configfile = "s3nbd.conf";
  struct config cfg;
  char *err_str;
  unsigned int err_line, conn_num;
  struct s3connection *s3conn;
  char buffer[4096];
  int res;
  unsigned char etag[16];

  if (load_config(configfile, &cfg, &err_line, &err_str) != 0)
    errx(1, "%s: %s (line %u)", configfile, err_str, err_line);

  if (gnutls_global_init() != GNUTLS_E_SUCCESS)
    errx(1, "gnutls_global_init()");

  conn_num = 5001;
  s3conn = get_s3_conn(&cfg, &conn_num);
  if (s3conn == NULL)
    errx(1, "no s3conn");

  res = send_s3_request(&cfg, s3conn, "HEAD", "", "42.gif", "", "", 0);
  printf("res=%i\n", res);

  res = gnutls_record_recv(s3conn->tls_sess, buffer, sizeof(buffer) - 1);
  printf("res=%i\n", res);

  release_s3_conn(s3conn, 0);
  gnutls_global_deinit();

  buffer[res] = 0;
  puts(buffer);

  return 0;
}
