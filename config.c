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
#include <time.h>
#include <netdb.h>
#include <pthread.h>
#include <gnutls/gnutls.h>
#include <gnutls/crypto.h>
#include <nettle/base64.h>

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
                 unsigned int *err_line, char const **errstr)
{
  FILE *fh;
  int result = -1, in_device = 0;
  unsigned int i;
  char line[1024], tmp[256];

  *err_line = 0;

  memset(cfg, 0, sizeof(*cfg));
  for (i = 0; i < sizeof(cfg->s3conns)/sizeof(cfg->s3conns[0]); i++) {
    cfg->s3conns[i].sock = -1;
    if (pthread_mutex_init(&cfg->s3conns[i].mtx, NULL) != 0)
      goto ERROR;
  }

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
        sscanf(line, " s3bucket %127s", cfg->s3bucket) ||
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

  *err_line = 0;

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

  if (cfg->s3bucket[0] == '\0') {
    *errstr = "no or empty s3bucket statement";
    goto ERROR1;
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

static int connect_s3 (struct s3connection *conn, char const **errstr)
{
  struct addrinfo hints, *result, *walk;
  int res, yes;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  res = getaddrinfo(conn->host, conn->port, &hints, &result);
  if (res != 0) {
    *errstr = gai_strerror(res);
    return -1;
  }

  for (walk = result; walk != NULL; walk = walk->ai_next) {
    conn->sock = socket(walk->ai_family, walk->ai_socktype, 0);
    if (conn->sock >= 0) {
      yes = 1;
      res = setsockopt(conn->sock, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));

      if ((res == 0) &&
          (connect(conn->sock, walk->ai_addr, walk->ai_addrlen) == 0))
        break;
    }

    *errstr = strerror(errno);
    close(conn->sock);
  }

  freeaddrinfo(result);

  return (walk == NULL ? -1 : 0);
}

static int tls_handshake (struct s3connection *conn, char const **errstr)
{
  int res;

  for (;;) {
    res = gnutls_handshake(conn->tls_sess);
    if (res == GNUTLS_E_SUCCESS)
      return 0;

    if (gnutls_error_is_fatal(res) != 0) {
      *errstr = gnutls_strerror(res);
      return -1;
    }
  }
}

static int setup_s3_ssl (struct s3connection *conn, char const **errstr)
{
  int res;

  *errstr = NULL;

  if ((res = gnutls_init(&conn->tls_sess, GNUTLS_CLIENT)) != GNUTLS_E_SUCCESS)
    goto ERROR;

  res = gnutls_set_default_priority(conn->tls_sess);
  if (res != GNUTLS_E_SUCCESS)
    goto ERROR1;

  res = gnutls_certificate_allocate_credentials(&conn->tls_cred);
  if (res != GNUTLS_E_SUCCESS)
    goto ERROR1;

  res = gnutls_credentials_set(conn->tls_sess, GNUTLS_CRD_CERTIFICATE,
                               conn->tls_cred);
  if (res != GNUTLS_E_SUCCESS)
    goto ERROR2;

  gnutls_transport_set_int(conn->tls_sess, conn->sock);
#if 0
  gnutls_transport_set_pull_timeout_function(conn->tls_sess,
                                             gnutls_system_recv_timeout);
#endif
  gnutls_handshake_set_timeout(conn->tls_sess, GNUTLS_DEFAULT_HANDSHAKE_TIMEOUT);
  gnutls_record_set_timeout(conn->tls_sess, 10000);

  if (tls_handshake(conn, errstr) != 0)
    goto ERROR2;

  conn->is_ssl = 1;

  return 0;

ERROR2:
  gnutls_certificate_free_credentials(conn->tls_cred);

ERROR1:
  gnutls_deinit(conn->tls_sess);

ERROR:
  if (*errstr == NULL)
    *errstr = gnutls_strerror(res);

  return -1;
}

struct s3connection *get_s3_conn (struct config *cfg, unsigned int *num,
                                  char const **errstr)
{
  struct s3connection *ret;
  unsigned int conn, host, port;
  int res;

  for (;;) {
   *num %= cfg->num_s3fetchers * cfg->num_s3hosts * cfg->num_s3ports;

    conn = *num % cfg->num_s3fetchers;
    host = *num % cfg->num_s3hosts;
    port = (*num / cfg->num_s3hosts) % cfg->num_s3ports;

    *num += 1;

    ret = &cfg->s3conns[conn];
    res = pthread_mutex_trylock(&ret->mtx);

    if (res == 0) { /* no-op */ }
    else if (res == EBUSY) continue;
    else {
      *errstr = strerror(res);
      return NULL;
    }

    ret->host = cfg->s3hosts[host];
    ret->port = cfg->s3ports[port];
    ret->bucket = cfg->s3bucket;

    if (ret->sock < 0) {
      if (connect_s3(ret, errstr) != 0)
        goto NEXT1;

      if ((cfg->s3ssl != 0) && (setup_s3_ssl(ret, errstr) != 0))
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
    if (conn->is_ssl != 0) {
      gnutls_certificate_free_credentials(conn->tls_cred);
      gnutls_deinit(conn->tls_sess);
    }

    close(conn->sock);
    conn->sock = -1;
  }

  pthread_mutex_unlock(&conn->mtx);
}

static int sha1_b64 (char *key, char *msg, char *b64, char const **errstr)
{
  int res;

  res = gnutls_hmac_fast(GNUTLS_MAC_SHA1, key, strlen(key), msg, strlen(msg),
                         b64);
  if (res != GNUTLS_E_SUCCESS) {
    *errstr = gnutls_strerror(res);
    return -1;
  }

  base64_encode_raw((uint8_t *) b64, 20, (uint8_t *) b64);
  b64[BASE64_ENCODE_RAW_LENGTH(20)] = '\0';

  return 0;
}

static int send_all (struct s3connection *conn, void *buffer, size_t to_write,
                     char const **errstr)
{
  size_t written;
  ssize_t res;

  for (written = 0; to_write > 0; written += res, to_write -= res) {
    if (!conn->is_ssl) {
      res = write(conn->sock, buffer + written, to_write);
      if ((res < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK) &&
          (errno != EINTR)) {
        *errstr = strerror(errno);
        return -1;
      }
    } else for (;;) {
      res = gnutls_record_send(conn->tls_sess, buffer + written, to_write);
      if (res >= 0)
        break;
      if ((res != GNUTLS_E_INTERRUPTED) && (res != GNUTLS_E_AGAIN)) {
        *errstr = gnutls_strerror(res);
        return -1;
      }
    }
  }

  return 0;
}

static ssize_t read_s3 (struct s3connection *conn, void *buffer, size_t buflen,
                        char const **errstr)
{
  ssize_t ret;

  if (!conn->is_ssl) {
    ret = read(conn->sock, buffer, buflen);
    if ((ret < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK) &&
        (errno != EINTR)) {
      *errstr = gnutls_strerror(ret);
      return -1;
    }
  } else for (;;) {
    ret = gnutls_record_recv(conn->tls_sess, buffer, buflen);
    if (ret >= 0)
      break;
    if ((ret == GNUTLS_E_REHANDSHAKE) && (tls_handshake(conn, errstr) != 0))
      return -1;
    if ((ret != GNUTLS_E_INTERRUPTED) && (ret != GNUTLS_E_AGAIN)) {
      *errstr = gnutls_strerror(ret);
      return -1;
    }
  }

  return ret;
}

int send_s3_request (struct config *cfg, struct s3connection *conn,
                     char *httpverb, char *folder, char *filename, void *data,
                     void *data_md5, size_t data_len, char const **errstr)
{
  time_t now;
  struct tm tm;
  int url_start, is_put, res;
  char date[32], string_to_sign[512], header[1024];
  unsigned char md5b64[BASE64_ENCODE_RAW_LENGTH(16) + 1];

  is_put = !strcmp(httpverb, "PUT");

  time(&now);
  gmtime_r(&now, &tm);
  strftime(date, sizeof(date) - 1, "%a, %d %b %Y %T GMT", &tm);

  if (is_put) {
    base64_encode_raw(md5b64, 16, data_md5);
    md5b64[BASE64_ENCODE_RAW_LENGTH(16)] = '\0';
  } else
    md5b64[0] = '\0';

  snprintf(string_to_sign, sizeof(string_to_sign) - 1,
           "%s\n" // http verb
           "%s\n" // content md5
           "\n"   // content type
           "%s\n" // date
           "%n/%s/%s/%s",
           httpverb, md5b64, date, &url_start, conn->bucket, folder, filename);

  snprintf(header, sizeof(header) - 1,
           "%s %s HTTP/1.1\r\n"
           "Host: %s\r\n"
           "Date: %s\r\n"
           "User-Agent: s3nbd\r\n"
           "Authorization: AWS %s:",
           httpverb, string_to_sign + url_start, conn->host, date,
           cfg->s3accesskey);

  res = sha1_b64(cfg->s3secretkey, string_to_sign, header + strlen(header),
                 errstr);
  if (res != 0)
    return -1;

  if (is_put)
    snprintf(header + strlen(header), sizeof(header) - strlen(header) - 1,
             "\r\n"
             "Content-Length: %lu\r\n"
             "Content-MD5: %s",
             data_len, md5b64);

  strcat(header, "\r\n\r\n");

  if (send_all(conn, header, strlen(header), errstr) != 0)
    return -1;

  if (is_put && (send_all(conn, data, data_len, errstr) != 0))
    return -1;

  return 0;
}

int read_s3_request (struct s3connection *conn, int is_head,
                     unsigned short *code, size_t *contentlen,
                     unsigned char *md5, char *buffer, size_t buflen,
                     char const **errstr)
{
  ssize_t res;
  size_t readbytes;
  char *option, *body;
  unsigned char etag[32];
  unsigned int i;

  readbytes = 0;
  for (;;) {
    res = read_s3(conn, buffer + readbytes, buflen - readbytes - 1, errstr);
    if (res < 0)
      return -1;

    readbytes += res;
    if (readbytes >= buflen - 1) {
      *errstr = "HTTP header too large";
      return -1;
    }

    buffer[readbytes] = '\0';
    if ((body = strstr(buffer, "\r\n\r\n")) != NULL)
      break;
  }

  body += 4;

  if (sscanf(buffer, "HTTP/1.1 %hu", code) != 1) {
    *errstr = "no HTTP/1.1 response code";
    return -1;
  }

  if ((option = strstr(buffer, "Content-Length")) == NULL) {
    *errstr = "no Content-Length";
    return -1;
  }
  if (sscanf(option, "Content-Length: %lu", contentlen) != 1) {
    *errstr = "invalid Content-Length";
    return -1;
  }
  if (*contentlen > buflen) {
    *errstr = "Content-Length too large";
    return -1;
  }

  if ((option = strstr(buffer, "ETag")) == NULL) {
    *errstr = "no ETag";
    return -1;
  }
  if (sscanf(option, "ETag: \"%32s", etag) != 1) {
    *errstr = "invalid ETag";
    return -1;
  }

  /* etag is lowercase hex */
  for (i = 0; i < sizeof(etag); i++) {
    if (i % 2 == 0)
      md5[i / 2] = 0;

    if ((etag[i] >= '0') && (etag[i] <= '9'))
      md5[i / 2] |= etag[i] - '0';
    else if ((etag[i] >= 'a') && (etag[i] <= 'f'))
      md5[i / 2] |= etag[i] - 'a' + 10;
    else {
      *errstr = "ETag contains invalid character";
      return -1;
    }

    if (i % 2 == 0)
      md5[i / 2] <<= 4;
  }

  if (!is_head) {
    /* get rid of header */
    readbytes -= body - buffer;
    memmove(buffer, body, readbytes);

    /* receive payload */
    while (readbytes < *contentlen) {
      res = read_s3(conn, buffer + readbytes, *contentlen - readbytes, errstr);
      if (res <= 0)
        return -1;

      readbytes += res;
    }
  }

  return 0;
}
