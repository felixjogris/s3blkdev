#ifndef _S3NBD_H
#define _S3NBD_H

#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <gnutls/gnutls.h>

#ifndef F_OFD_GETLK
#  define F_OFD_GETLK 36
#endif
#ifndef F_OFD_SETLK
#  define F_OFD_SETLK 37
#endif
#ifndef F_OFD_SETLKW
#  define F_OFD_SETLKW 38
#endif

#define CHUNKSIZE (8 * 1024 * 1024)
#define COMPR_CHUNKSIZE (CHUNKSIZE + CHUNKSIZE/4)

#define DEFAULT_CONFIGFILE "/etc/s3nbd.conf"
#define MAX_IO_THREADS 128
#define DEVNAME_SIZE 64

struct device {
  char name[DEVNAME_SIZE];
  char cachedir[PATH_MAX];
  size_t size;
};

struct s3connection {
  char *host;
  char *port;
  int sock;
  int is_ssl;
  gnutls_session_t sslctx;
  pthread_mutex_t mtx;
};

struct config {
  char s3hosts[4][256];
  unsigned short num_s3hosts;
  char s3ports[4][8];
  unsigned short num_s3ports;
  unsigned char s3ssl;
  char s3accesskey[128];
  char s3secretkey[128];

  struct s3connection s3conns[MAX_IO_THREADS]; // max(s3hosts*s3ports, num_io_threads<=MAX_IO_THREADS)

  unsigned short num_io_threads;
  unsigned short num_s3fetchers;

  struct device devs[128];
  unsigned short num_devices;

  char listen[128];
  char port[8];
};

int load_config (char *configfile, struct config *cfg,
                 unsigned int *err_line, char **errstr);
int save_pidfile (char *pidfile);

#endif
