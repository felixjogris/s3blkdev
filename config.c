#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <netdb.h>
#include <pthread.h>
#include <gnutls/gnutls.h>
#include <gnutls/crypto.h>
#include <nettle/base64.h>

#include "s3blkdev.h"

/* integer to string by preprocessor */
#define XSTR(a) #a
#define STR(a) XSTR(a)

enum readwrite {
  READ,
  WRITE
};

static int is_comment (char *line)
{
  return ((line[0] == '#') || (line[0] == ';'));
}

static int is_empty (char *line)
{
  char *p;

  for (p = line; *p != '\0'; p++)
    if (!isspace(*p))
      return 0;

  return 1;
}

static int is_incomplete (char *line)
{
  return ((strlen(line) == 0) || (line[strlen(line) - 1] != '\n'));
}

static int validate_config (struct config *cfg, char const **errstr)
{
  if ((cfg->listen[0] == '\0') && (cfg->geom_listen[0] == '\0')) {
    *errstr = "no or empty listen statements";
    return -1;
  }

  if ((cfg->listen[0] != '/') && (cfg->port[0] == '\0')) {
    *errstr = "no or empty port statement and listen is not a unix socket";
    return -1;
  }

  if ((cfg->geom_listen[0] != '\0') && (cfg->geom_port[0] == '\0')) {
    *errstr = "no or empty geom port statement";
    return -1;
  }

  if (cfg->num_io_threads == 0) {
    *errstr = "number of workers must not be zero";
    return -1;
  }

  if (cfg->num_io_threads >= MAX_IO_THREADS) {
    *errstr = "number of workers too large (max. " STR(MAX_IO_THREADS) ")";
    return -1;
  }

  if (cfg->num_s3fetchers == 0) {
    *errstr = "number of fetchers must not be zero";
    return -1;
  }

  if (cfg->num_s3fetchers > cfg->num_io_threads) {
    *errstr = "number of fetchers must not exceed number of workers";
    return -1;
  }

  if (cfg->num_s3hosts == 0) {
    *errstr = "no s3hosts";
    return -1;
  }

  if (cfg->num_s3ports == 0) {
    if (!cfg->s3ssl) {
      *errstr = "no s3ports and s3ssl not set";
    return -1;
    }
    cfg->num_s3ports = 1;
    strcpy(cfg->s3ports[0], "443");
  }

  if (cfg->s3bucket[0] == '\0') {
    *errstr = "no or empty s3bucket statement";
    return -1;
  }

  if (cfg->s3accesskey[0] == '\0') {
    *errstr = "no or empty s3accesskey statement";
    return -1;
  }

  if (cfg->s3secretkey[0] == '\0') {
    *errstr = "no or empty s3secretkey statement";
    return -1;
  }

  return 0;
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
        sscanf(line, " geom_listen %127s", cfg->geom_listen) ||
        sscanf(line, " geom_port %7[0-9]", cfg->geom_port) ||
        sscanf(line, " workers %hu", &cfg->num_io_threads) ||
        sscanf(line, " fetchers %hu", &cfg->num_s3fetchers) ||
        sscanf(line, " s3maxreqsperconn %hu", &cfg->s3_max_reqs_per_conn) ||
        sscanf(line, " s3timeout %u", &cfg->s3timeout) ||
        sscanf(line, " s3ssl %hhu", &cfg->s3ssl) ||
        sscanf(line, " s3name %127s", cfg->s3name) ||
        sscanf(line, " s3bucket %127s", cfg->s3bucket) ||
        sscanf(line, " s3accesskey %127s", cfg->s3accesskey) ||
        sscanf(line, " s3secretkey %127s", cfg->s3secretkey))
      continue;

    /* s3host */
    if (sscanf(line, " s3host %255s", tmp)) {
      if (cfg->num_s3hosts >= sizeof(cfg->s3hosts)/sizeof(cfg->s3hosts[0])) {
        *errstr = "too many S3 hosts";
        goto ERROR1;
      }

      strncpy(cfg->s3hosts[cfg->num_s3hosts], tmp, sizeof(cfg->s3hosts[0]));
      cfg->num_s3hosts++;

      continue;
    }

    /* s3port */
    if (sscanf(line, " s3port %7[0-9]", tmp)) {
      if (cfg->num_s3ports >= sizeof(cfg->s3ports)/sizeof(cfg->s3ports[0])) {
        *errstr = "too many S3 ports";
        goto ERROR1;
      }

      strncpy(cfg->s3ports[cfg->num_s3ports], tmp, sizeof(cfg->s3ports[0]));
      cfg->num_s3ports++;

      continue;
    }

    /* device */
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
  result = validate_config(cfg, errstr);;

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

  /* leave fd open intentionally */
  return 0;
}

int set_socket_options (int sock)
{
  int opt;

  opt = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) == -1)
    return -1;

  opt = TCP_RMEM;
  if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &opt, sizeof(opt)) == -1)
    return -1;

  opt = TCP_WMEM;
  if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &opt, sizeof(opt)) == -1)
    return -1;

  return 0;
}

static int s3_connect (struct s3connection *conn, char const **errstr)
{
  struct addrinfo hints, *result, *walk;
  int res;

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
    if ((conn->sock >= 0) &&
        (set_socket_options(conn->sock) == 0) &&
        (connect(conn->sock, walk->ai_addr, walk->ai_addrlen) == 0))
      break;

    *errstr = strerror(errno);
    close(conn->sock);
  }

  freeaddrinfo(result);

  return (walk == NULL ? -1 : 0);
}

static int s3_tls_handshake (struct s3connection *conn, char const **errstr)
{
  int res;

  for (;;) {
    res = gnutls_handshake(conn->tls_sess);
    if (res == GNUTLS_E_SUCCESS)
      break;

    if (gnutls_error_is_fatal(res) != 0) {
      *errstr = gnutls_strerror(res);
      return -1;
    }
  }

  return 0;
}

static int s3_tls_setup (struct s3connection *conn, char const **errstr)
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
  gnutls_handshake_set_timeout(conn->tls_sess,
                               GNUTLS_DEFAULT_HANDSHAKE_TIMEOUT);
  gnutls_record_set_timeout(conn->tls_sess, 10000);

  if (s3_tls_handshake(conn, errstr) != 0)
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

struct s3connection *s3_get_conn (struct config *cfg, unsigned int *conn_num,
                                  char const **errstr)
{
  struct s3connection *ret;
  unsigned int host, port;
  int res;

  for (;;) {
    *conn_num %= MAX(cfg->num_s3fetchers, cfg->num_s3hosts * cfg->num_s3ports);

    /* prefer different host over different port */
    host = *conn_num % cfg->num_s3hosts;
    port = (*conn_num / cfg->num_s3hosts) % cfg->num_s3ports;

    *conn_num += 1;

    ret = &cfg->s3conns[*conn_num];
    res = pthread_mutex_trylock(&ret->mtx);

    if (res == EBUSY) continue;
    if (res != 0) {
      *errstr = strerror(res);
      return NULL;
    }

    ret->host = cfg->s3hosts[host];
    ret->name = (cfg->s3name[0] == '\0' ? cfg->s3hosts[host] : cfg->s3name);
    ret->port = cfg->s3ports[port];
    ret->bucket = cfg->s3bucket;
    ret->timeout = cfg->s3timeout;

    if (ret->sock < 0) {
      if (s3_connect(ret, errstr) != 0)
        goto NEXT1;

      if ((cfg->s3ssl != 0) && (s3_tls_setup(ret, errstr) != 0))
        goto NEXT2;

      ret->remaining_reqs = cfg->s3_max_reqs_per_conn;
    }

    ret->remaining_reqs--;

    return ret;

    NEXT2:
      close(ret->sock);

    NEXT1:
      pthread_mutex_unlock(&ret->mtx);
  }
}

void s3_release_conn (struct s3connection *conn)
{
  if ((conn->is_error != 0) || (conn->remaining_reqs == 0)) {
    if (conn->is_ssl != 0) {
      gnutls_bye(conn->tls_sess, GNUTLS_SHUT_RDWR);
      gnutls_deinit(conn->tls_sess);
      gnutls_certificate_free_credentials(conn->tls_cred);
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

static int s3_wait_for_socket (struct s3connection *conn, enum readwrite mode,
                               char const **errstr)
{
  fd_set fds;
  struct timeval timeout;
  int res;

  FD_ZERO(&fds);
  FD_SET(conn->sock, &fds);

  timeout.tv_sec = conn->timeout / 1000;
  timeout.tv_usec = (conn->timeout % 1000) * 1000;

  if (mode == READ)
    res = select(conn->sock + 1, &fds, NULL, NULL, &timeout);
  else
    res = select(conn->sock + 1, NULL, &fds, NULL, &timeout);

  if (res > 0)
    return 0;

  if (res < 0)
    *errstr = strerror(errno);
  else if (mode == READ)
    *errstr = "timeout while reading from net";
  else
    *errstr = "timeout while writing to net";

  return -1;
}

static int s3_send_all (struct s3connection *conn, void *buffer,
                        size_t to_write, char const **errstr)
{
  size_t written;
  ssize_t res;

  for (written = 0; to_write > 0; written += res, to_write -= res) {
    if (!conn->is_ssl) {
      if (s3_wait_for_socket(conn, WRITE, errstr) != 0)
        return -1;

      res = write(conn->sock, buffer + written, MIN(to_write, 131072));
      if ((res < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK) &&
          (errno != EINTR)) {
        *errstr = strerror(errno);
        return -1;
      }
    } else for (;;) {
      res = gnutls_record_send(conn->tls_sess, buffer + written,
                               MIN(to_write, 131072));
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

static ssize_t s3_recv (struct s3connection *conn, void *buffer, size_t buflen,
                        char const **errstr)
{
  ssize_t ret;

  if (!conn->is_ssl) {
    if (s3_wait_for_socket(conn, READ, errstr) != 0)
      return -1;

    ret = read(conn->sock, buffer, buflen);
    if ((ret < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK) &&
        (errno != EINTR)) {
      *errstr = strerror(errno);
      return -1;
    }
  } else for (;;) {
    ret = gnutls_record_recv(conn->tls_sess, buffer, buflen);
    if (ret >= 0)
      break;
    if ((ret == GNUTLS_E_REHANDSHAKE) && (s3_tls_handshake(conn, errstr) != 0))
      return -1;
    if ((ret != GNUTLS_E_INTERRUPTED) && (ret != GNUTLS_E_AGAIN)) {
      *errstr = gnutls_strerror(ret);
      return -1;
    }
  }

  if (ret == 0) {
    *errstr = "connection closed";
    return -1;
  }

  return ret;
}

static const char *httpverb_to_string (enum httpverb verb)
{
  switch (verb) {
    case GET:  return "GET";
    case HEAD: return "HEAD";
    case PUT:  return "PUT";
    default:   return "FUCK";
  }
}

static int s3_start_req (struct config *cfg, struct s3connection *conn,
                         enum httpverb verb, char *folder, char *filename,
                         void *data, size_t data_len, void *data_md5,
                         char const **errstr)
{
  time_t now;
  struct tm tm;
  int url_start, res;
  char date[32], string_to_sign[512], header[1024];
  unsigned char md5b64[BASE64_ENCODE_RAW_LENGTH(16) + 1];

  time(&now);
  gmtime_r(&now, &tm);
  strftime(date, sizeof(date) - 1, "%a, %d %b %Y %T GMT", &tm);

  if (verb == PUT) {
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
           httpverb_to_string(verb), md5b64, date, &url_start, cfg->s3bucket,
           folder, filename);

  snprintf(header, sizeof(header) - 1,
           "%s %s HTTP/1.1\r\n"
           "Host: %s\r\n"
           "Date: %s\r\n"
           "User-Agent: s3blkdev\r\n"
           "Authorization: AWS %s:",
           httpverb_to_string(verb), string_to_sign + url_start, conn->name,
           date, cfg->s3accesskey);

  res = sha1_b64(cfg->s3secretkey, string_to_sign, header + strlen(header),
                 errstr);
  if (res != 0)
    return -1;

  if (verb == PUT)
    snprintf(header + strlen(header), sizeof(header) - strlen(header) - 1,
             "\r\n"
             "Content-Length: %lu\r\n"
             "Content-MD5: %s",
             data_len, md5b64);

  strcat(header, "\r\n\r\n");

  if (s3_send_all(conn, header, strlen(header), errstr) != 0)
    return -1;

  if ((verb == PUT) && (s3_send_all(conn, data, data_len, errstr) != 0))
    return -1;

  return 0;
}

static int s3_scan_etag (char *option, unsigned char *md5, const char **errstr)
{
  unsigned char etag[32];
  unsigned int i;

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

  return 0;
}

static int s3_finish_req (struct s3connection *conn, enum httpverb verb,
                          unsigned short *code, size_t *contentlen,
                          unsigned char *md5, char *buffer,
                          size_t buflen, char const **errstr)
{
  char header[1024];
  ssize_t res;
  size_t readbytes;
  char *option, *body;

  readbytes = 0;

  for (;;) {
    res = s3_recv(conn, header + readbytes, sizeof(header) - readbytes - 1,
                  errstr);
    if (res <= 0)
      return -1;

    readbytes += res;
    header[readbytes] = '\0';

    if ((body = strstr(header, "\r\n\r\n")) != NULL)
      break;

    if (readbytes >= sizeof(header) - 1) {
      *errstr = "HTTP header too large";
      return -1;
    }
  }

  body += 4;

  if (sscanf(header, "HTTP/1.1 %hu", code) != 1) {
    *errstr = "no HTTP/1.1 response code";
    return -1;
  }

  if ((option = strstr(header, "Content-Length")) == NULL) {
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

  /* etag is content md5 */
  if (((option = strstr(header, "ETag")) != NULL) &&
      (s3_scan_etag(option, md5, errstr) != 0))
    return -1;

  /* get rid of header */
  readbytes -= body - header;
  memmove(buffer, body, readbytes);

  if (verb != HEAD) {
    /* receive payload */
    while (readbytes < *contentlen) {
      res = s3_recv(conn, buffer + readbytes,
                    MIN(*contentlen - readbytes, 131072), errstr);
      if (res <= 0)
        return -1;

      readbytes += res;
    }
  }

  return 0;
}

int s3_request (struct config *cfg, struct s3connection *conn,
                char const **errstr,
                enum httpverb verb, char *folder, char *filename, void *data,
                size_t data_len, void *data_md5,
                unsigned short *code, size_t *contentlen, unsigned char *md5,
                char *buffer, size_t buflen)
{
  int res;

  conn->is_error = 1;

  res = s3_start_req(cfg, conn, verb, folder, filename, data, data_len,
                     data_md5, errstr);
  if (res != 0)
    return -1;

  res = s3_finish_req(conn, verb, code, contentlen, md5, buffer, buflen,
                      errstr);
  if (res != 0)
    return -1;

  conn->is_error = (*code != 200);

  return 0;
}
