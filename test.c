#include <err.h>
#include <gnutls/gnutls.h>
#include <gnutls/crypto.h>

int main ()
{
  int err;
  gnutls_cipher_hd_t handle;
  gnutls_datum_t key = { .data = "wurstwurstwurst.", .size = 16 };

  if ((err = gnutls_global_init()) != 0)
    errx(1, "global_init: %s", gnutls_strerror(err));

  if ((err = gnutls_cipher_init(&handle, GNUTLS_CIPHER_AES_256_CBC, &key, &key)) != 0)
    errx(1, "cipher_init: %s", gnutls_strerror(err));

  return 0;
}
