#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <gnutls/gnutls.h>

#include "s3nbd.h"

#define XSTR(a) #a
#define STR(a) XSTR(a)

static int is_comment (char *line)
{
  return ((line[0] == '#') || (line[0] == ';'));
}

static int is_empty (char *line)
{
  char *p;

  for (p = line; *p != '\0'; p++)
    if (isspace(*p))
      return 0;

  return 1;
}

static int is_incomplete (char *line)
{
  return ((strlen(line) == 0) || (line[strlen(line) - 1] != '\n'));
}

int load_config (char *configfile, struct config *cfg,
                 unsigned int *err_line, char **errstr)
{
  FILE *fh;
  int result = -1, in_device = 0;
  unsigned int i;
  char line[1024], tmp[256];

  *err_line = 0;

  memset(cfg, 0, sizeof(*cfg));
  for (i = 0; i < sizeof(cfg->s3conns)/sizeof(cfg->s3conns[0]); i++)
    cfg->s3conns[i].sock = -1;

  if ((fh = fopen(configfile, "r")) == NULL) {
    *errstr = strerror(errno);
    goto ERROR;
  }

  while (fgets(line, sizeof(line), fh) != NULL) {
    *err_line += 1;

    if (is_incomplete(line)) {
      *errstr = "incomplete line (no terminating newline)";
      goto ERROR1;
    }

    if (is_comment(line) || is_empty(line))
      continue;

    if (in_device) {
      if (sscanf(line, "cachedir %4095s",
                 cfg->devs[cfg->num_devices].cachedir)) {
        in_device |= 2;
      } else if (sscanf(line, "size %lu", &cfg->devs[cfg->num_devices].size)) {
        in_device |= 4;
      } else {
        *errstr = "unknown device parameter";
        goto ERROR1;
      }

      if (in_device == 7) {
        cfg->num_devices++;
        in_device = 0;
      }

      continue;
    }

    if (sscanf(line, " listen %127s", cfg->listen) ||
        sscanf(line, " port %7[0-9]", cfg->port) ||
        sscanf(line, " workers %hu", &cfg->num_io_threads) ||
        sscanf(line, " fetchers %hu", &cfg->num_s3fetchers) ||
        sscanf(line, " s3ssl %hhu", &cfg->s3ssl) ||
        sscanf(line, " s3accesskey %127s", cfg->s3accesskey) ||
        sscanf(line, " s3secretkey %127s", cfg->s3secretkey))
      continue;

    if (sscanf(line, " s3host %255s", tmp)) {
      if (cfg->num_s3hosts >= sizeof(cfg->s3hosts)/sizeof(cfg->s3hosts[0])) {
        *errstr = "too many S3 hosts";
        goto ERROR1;
      }

      strncpy(cfg->s3hosts[cfg->num_s3hosts], tmp, sizeof(cfg->s3hosts[0]));
      cfg->num_s3hosts++;

      continue;
    }

    if (sscanf(line, " s3port %7[0-9]", tmp)) {
      if (cfg->num_s3ports >= sizeof(cfg->s3ports)/sizeof(cfg->s3ports[0])) {
        *errstr = "too many S3 ports";
        goto ERROR1;
      }

      strncpy(cfg->s3ports[cfg->num_s3ports], tmp, sizeof(cfg->s3ports[0]));
      cfg->num_s3ports++;

      continue;
    }

    if (sscanf(line, " [%63[^]]", tmp)) {
      if (cfg->num_devices >= sizeof(cfg->devs)/sizeof(cfg->devs[0])) {
        *errstr = "too many devices";
        goto ERROR1;
      }

      strncpy(cfg->devs[cfg->num_devices].name, tmp, sizeof(cfg->devs[0].name));
      in_device = 1;

      continue;
    }

    *errstr = "unknown configuration directive";
    goto ERROR1;
  }

  if (in_device) {
    *errstr = "incomplete device configuration";
    goto ERROR1;
  }

  if (cfg->listen[0] == '\0') {
    *errstr = "no or empty listen statement";
    goto ERROR1;
  }

  if (cfg->port[0] == '\0') {
    *errstr = "no or empty port statement";
    goto ERROR1;
  }

  if (cfg->num_io_threads == 0) {
    *errstr = "number of workers must not be zero";
    goto ERROR1;
  }

  if (cfg->num_io_threads >= MAX_IO_THREADS) {
    *errstr = "number of workers too large (max. " STR(MAX_IO_THREADS) ")";
    goto ERROR1;
  }

  if (cfg->num_s3fetchers == 0) {
    *errstr = "number of fetchers must not be zero";
    goto ERROR1;
  }

  if (cfg->num_s3fetchers > cfg->num_io_threads) {
    *errstr = "number of fetchers must not exceed number of workers";
    goto ERROR1;
  }

  if (cfg->num_s3hosts == 0) {
    *errstr = "no s3hosts";
    goto ERROR1;
  }

  if (cfg->num_s3ports == 0) {
    if (!cfg->s3ssl) {
      *errstr = "no s3ports and s3ssl not set";
      goto ERROR1;
    }
    cfg->num_s3ports = 1;
    strcpy(cfg->s3ports[0], "443");
  }

  if (cfg->s3accesskey[0] == '\0') {
    *errstr = "no or empty s3accesskey statement";
    goto ERROR1;
  }

  if (cfg->s3secretkey[0] == '\0') {
    *errstr = "no or empty s3secretkey statement";
    goto ERROR1;
  }

  result = 0;

ERROR1:
  if (ferror(fh)) {
    *errstr = strerror(errno);
    result = -1;
  }

  if (fclose(fh) != 0) {
    *errstr = strerror(errno);
    result = -1;
  }

ERROR:
  return result;
}

int save_pidfile (char *pidfile)
{
  int fd, len;
  char pid[16];
  struct flock flk;

  fd = open(pidfile, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
  if (fd < 0)
    return -1;

  len = snprintf(pid, sizeof(pid), "%u\n", getpid());

  flk.l_type = F_WRLCK;
  flk.l_whence = SEEK_SET;
  flk.l_start = 0;
  flk.l_len = len;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0)
    return -1;
 
  if (write(fd, pid, len) != len)
    return -1;

  return 0;
}

static int connect_s3 (struct s3connection *conn)
{
  struct addrinfo hints, *result, *walk;
  int res, yes;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  res = getaddrinfo(conn->host, conn->port, &hints, &result);
  if (res != 0)
    return -1;

  for (walk = result; walk != NULL; walk = walk->ai_next) {
    conn->sock = socket(walk->ai_family, walk->ai_socktype, 0);
    if (conn->sock < 0)
      continue;

    yes = 1;
    res = setsockopt(conn->sock, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));

    if ((res == 0) &&
        (connect(conn->sock, walk->ai_addr, walk->ai_addrlen) == 0))
      break;

    close(conn->sock);
  }

  freeaddrinfo(result);

  return (walk == NULL ? -1 : 0);
}

static int setup_s3_ssl (struct s3connection *conn)
{
  int res;

  if (gnutls_init(&conn->sslctx, GNUTLS_CLIENT) != GNUTLS_E_SUCCESS)
    return -1;

#if 0
  gnutls_transport_set_pull_timeout_function(conn->sslctx,
                                             gnutls_system_recv_timeout);
#endif
  gnutls_handshake_set_timeout(conn->sslctx, GNUTLS_DEFAULT_HANDSHAKE_TIMEOUT);
  gnutls_record_set_timeout(conn->sslctx, 10000);
  gnutls_transport_set_int(conn->sslctx, conn->sock);

  for (;;) {
    res = gnutls_handshake(conn->sslctx);
    if (res == GNUTLS_E_SUCCESS)
      break;

    if (gnutls_error_is_fatal(res) != 0) {
      gnutls_deinit(conn->sslctx);
      return -1;
    }
  }

  conn->is_ssl = 1;

  return 0;
}

struct s3connection *get_s3_conn (struct config *cfg, unsigned int *num)
{
  struct s3connection *ret;
  unsigned int conn, host, port;
  int res;

  for (;;) {
    if (*num >= cfg->num_s3fetchers * cfg->num_s3hosts * cfg->num_s3ports)
      *num = 0;

    conn = *num % cfg->num_s3fetchers;
    host = *num % cfg->num_s3hosts;
    port = (*num / cfg->num_s3hosts) % cfg->num_s3ports;

    *num += 1;

    ret = &cfg->s3conns[conn];
    res = pthread_mutex_trylock(&ret->mtx);

    if (res == 0) { /* no-op */ }
    else if (res == EBUSY) continue;
    else return NULL;

    ret->host = cfg->s3hosts[host];
    ret->port = cfg->s3ports[port];

    if (ret->sock < 0) {
      if (connect_s3(ret) != 0)
        goto NEXT1;

      if ((cfg->s3ssl != 0) && (setup_s3_ssl(ret) != 0))
        goto NEXT2;
    }

    return ret;

    NEXT2:
      close(ret->sock);

    NEXT1:
      pthread_mutex_unlock(&ret->mtx);
  }
}

void release_s3_conn (struct s3connection *conn, int error)
{
  if (error != 0) {
    if (conn->is_ssl != 0)
      gnutls_deinit(conn->sslctx);

    close(conn->sock);
    conn->sock = -1;
  }

  pthread_mutex_unlock(&conn->mtx);
}
