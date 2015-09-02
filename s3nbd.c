#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <err.h>
#include <netdb.h>
#include <errno.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/un.h>
#include <sys/select.h>
#include <limits.h>
#include <syslog.h>

#define __USE_GNU
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "s3nbd.h"

#define logerr(fmt, params ...) syslog(LOG_ERR, "%s [%s:%i]: " fmt "\n", \
  __FUNCTION__, __FILE__, __LINE__, ## params)

#define NBD_BUFSIZE 1024
#define NBD_FLAG_FIXED_NEWSTYLE 1
#define NBD_FLAG_NO_ZEROES 2
#define NBD_OPT_EXPORT_NAME 1
#define NBD_OPT_ABORT 2
#define NBD_OPT_LIST 3
#define NBD_REP_ACK 1
#define NBD_REP_SERVER 2
#define NBD_REP_ERR_UNSUP 0x80000001
#define NBD_REP_ERR_INVALID 0x80000003
#define NBD_FLAG_HAS_FLAGS 1
#define NBD_FLAG_SEND_FLUSH 4
#define NBD_REQUEST_MAGIC 0x25609513
#define NBD_REPLY_MAGIC 0x67446698
#define NBD_CMD_MASK_COMMAND 0x0000ffff
#define NBD_CMD_READ 0
#define NBD_CMD_WRITE 1
#define NBD_CMD_DISC 2
#define NBD_CMD_FLUSH 3

const char const NBD_INIT_PASSWD[] = { 'N','B','D','M','A','G','I','C' };
const char const NBD_OPTS_MAGIC[] =  { 'I','H','A','V','E','O','P','T' };
const char const NBD_OPTS_REPLY_MAGIC[] = { 0x00, 0x03, 0xe8, 0x89,
                                            0x04, 0x55, 0x65, 0xa9 };

struct client_thread_arg {
  int socket;
  struct sockaddr addr;
  socklen_t addr_len;
  pthread_mutex_t socket_mtx;
  char clientname[INET6_ADDRSTRLEN + 8];
  struct device dev;
  int cachedir_fd;
};

struct io_thread_arg {
  pthread_t thread;
  pthread_cond_t wakeup_cond;
  pthread_mutex_t wakeup_mtx;
  int busy;
  int socket;
  pthread_mutex_t *socket_mtx;
  char devicename[DEVNAME_SIZE];
  int cachedir_fd;
  struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t type;
    char handle[8];
    uint64_t offs;
    uint32_t len;
  } req;
  size_t buflen;
  void *buffer;
};

int running = 1;
int reload_config = 0;
struct io_thread_arg io_threads[128];
unsigned short num_io_threads;
struct config cfg;

static ssize_t read_all (int fd, void *buffer, size_t len)
{
  ssize_t res;

  for (; len > 0; buffer += res, len -= res) {
    if ((res = read(fd, buffer, len)) <= 0) {
      if (res < 0)
        logerr("read(): %s", strerror(errno));
      return -1;
    }
  }

  return 0;
}

static ssize_t write_all (int fd, const void *buffer, size_t len)
{
  ssize_t res;

  for (; len > 0; buffer += res, len -= res) {
    if ((res = write(fd, buffer, len)) < 0) {
      logerr("write(): %s", strerror(errno));
      return -1;
    }
  }

  return 0;
}

/* TODO */
static int fetch_chunk (char *devicename, int fd, char *name)
{
  int storedir_fd, store_fd, result = -1;
  unsigned int i;
  char buffer[4096];

  snprintf(buffer, sizeof(buffer), "/var/tmp/%s.store", devicename);

  if ((storedir_fd = open(buffer, O_RDONLY|O_DIRECTORY)) < 0) {
    logerr("open(): %s: %s", buffer, strerror(errno));
    goto ERROR;
  }

  if ((store_fd = openat(storedir_fd, name, O_RDONLY)) < 0) {
    if (errno != ENOENT) {
      logerr("openat(): %s", strerror(errno));
      goto ERROR1;
    }

    memset(buffer, 0, sizeof(buffer));

    for (i = 0; i < CHUNKSIZE/sizeof(buffer); i++) {
      if (write_all(fd, buffer, sizeof(buffer)) != 0)
        goto ERROR1;
    }

    result = 0;
  } else {
    for (i = 0; i < CHUNKSIZE/sizeof(buffer); i++) {
      if (read_all(store_fd, buffer, sizeof(buffer)) != 0)
        goto ERROR2;

      if (write_all(fd, buffer, sizeof(buffer)) != 0)
        goto ERROR2;
    }

    result = 0;

ERROR2:
    if (close(store_fd) != 0) {
      logerr("close(): %s", strerror(errno));
      result = -1;
    }
  }

ERROR1:
  if (close(storedir_fd) != 0) {
    logerr("close(): %s", strerror(errno));
    result = -1;
  }

ERROR:
  return result;
}

static uint64_t htonll (uint64_t u64h)
{
  uint32_t lo = u64h & 0xffffffffffffffff;
  uint32_t hi = u64h >> 32;
  uint64_t u64n = htonl(lo);

  u64n <<= 32;
  u64n |= htonl(hi);

  return u64n;
}

static uint64_t ntohll (uint64_t u64n)
{
  return htonll(u64n);
}

static int block_signals ()
{
  sigset_t sigset;
  int res;

  if (sigfillset(&sigset) != 0) {
    logerr("sigfillset(): %s", strerror(errno));
    return -1;
  }

  if ((res = pthread_sigmask(SIG_SETMASK, &sigset, NULL)) != 0) {
    logerr("pthread_sigmask(): %s", strerror(res));
    return -1;
  }

  return 0;
}

static int io_send_reply (struct io_thread_arg *arg, uint32_t error,
                          uint32_t len)
{
  const int hdrlen = sizeof(arg->req.magic) + sizeof(arg->req.type) +
                     sizeof(arg->req.handle);
  int res;

  arg->req.magic = htonl(NBD_REPLY_MAGIC);
  arg->req.type = htonl(error);

  if ((res = pthread_mutex_lock(arg->socket_mtx)) != 0) {
    logerr("pthread_mutex_lock(): %s", strerror(res));
    return -1;
  }

  if ((write_all(arg->socket, &arg->req, hdrlen) == 0) && (len > 0))
    write_all(arg->socket, arg->buffer, len);

  if ((res = pthread_mutex_unlock(arg->socket_mtx)) != 0) {
    logerr("pthread_mutex_unlock(): %s", strerror(res));
    return -1;
  }

  return 0;
}

static int io_lock_chunk (int fd, short int type, uint64_t start_offs,
                          uint64_t end_offs)
{
  struct flock flk;

  flk.l_type = type;
  flk.l_whence = SEEK_SET;
  flk.l_start = start_offs;
  flk.l_len = end_offs - start_offs;
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLKW, &flk) != 0) {
    logerr("fcntl(): %s", strerror(errno));
    return -1;
  }

  return 0;
}

static int io_open_chunk (struct io_thread_arg *arg, uint64_t chunk_no,
                          uint64_t start_offs, uint64_t end_offs)
{
  char name[17];
  int fd;
  struct stat st;

  snprintf(name, sizeof(name), "%016llx", (unsigned long long) chunk_no);

  for (;;) {
    fd = openat(arg->cachedir_fd, name, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);
    if (fd < 0) {
      logerr("openat(): %s", strerror(errno));
      goto ERROR;
    }

    if (io_lock_chunk(fd, F_RDLCK, start_offs, end_offs) != 0)
      goto ERROR1;

    if (fstatat(arg->cachedir_fd, name, &st, 0) != 0) {
      if (errno != ENOENT) {
        logerr("fstatat(): %s", strerror(errno));
        goto ERROR1;
      }

      if (close(fd) != 0) {
        logerr("close(): %s", strerror(errno));
        goto ERROR;
      }

      continue;
    }

    if (st.st_size == CHUNKSIZE)
      break;

    if (io_lock_chunk(fd, F_UNLCK, start_offs, end_offs) != 0)
      goto ERROR1;

    if (io_lock_chunk(fd, F_WRLCK, 0, CHUNKSIZE) != 0)
      goto ERROR1;

    if (fstatat(arg->cachedir_fd, name, &st, 0) != 0) {
      if (errno != ENOENT) {
        logerr("fstatat(): %s", strerror(errno));
        goto ERROR1;
      }

      if (close(fd) != 0) {
        logerr("close(): %s", strerror(errno));
        goto ERROR;
      }

      continue;
    }

    if (st.st_size == CHUNKSIZE)
      break;

    if (fetch_chunk(arg->devicename, fd, name) != 0)
      goto ERROR1;

    break;
  }

  if (lseek(fd, start_offs, SEEK_SET) == (off_t) -1) {
    logerr("lseek(): %s", strerror(errno));
    goto ERROR1;
  }

  return fd;

ERROR1:
  if (close(fd) != 0)
    logerr("close(): %s", strerror(errno));

ERROR:
  return -1;
}

static int io_read_chunk (struct io_thread_arg *arg, uint64_t chunk_no,
                          uint64_t start_offs, uint64_t end_offs,
                          uint32_t *pos)
{
  int fd, result = -1;
  int64_t len = end_offs - start_offs;

  fd = io_open_chunk(arg, chunk_no, start_offs, end_offs);
  if (fd < 0)
    goto ERROR;

  if (read_all(fd, arg->buffer + *pos, len) != 0)
    goto ERROR1;

  *pos += len;

  result = 0;

ERROR1:
  if (close(fd) != 0)
    logerr("close(): %s", strerror(errno));

ERROR:
  return result;
}

static int io_read_chunks (struct io_thread_arg *arg)
{
  uint64_t start_chunk, end_chunk, start_offs, end_offs;
  uint32_t pos = 0;

  start_chunk = arg->req.offs / CHUNKSIZE;
  end_chunk = (arg->req.offs + arg->req.len) / CHUNKSIZE;
  start_offs = arg->req.offs % CHUNKSIZE;
  end_offs = (arg->req.offs + arg->req.len) % CHUNKSIZE;

  while (start_chunk < end_chunk) {
    if (io_read_chunk(arg, start_chunk, start_offs, CHUNKSIZE, &pos) != 0)
      goto ERROR;
    start_chunk++;
    start_offs = 0;
  }

  if (io_read_chunk(arg, start_chunk, start_offs, end_offs, &pos) != 0)
    goto ERROR;

  return io_send_reply(arg, 0, pos);

ERROR:
  io_send_reply(arg, EIO, 0);
  return -1;
}

static int io_write_chunk (struct io_thread_arg *arg, uint64_t chunk_no,
                    uint64_t start_offs, uint64_t end_offs, uint32_t *pos)
{
  int fd, result = -1;
  int64_t len = end_offs - start_offs;

  fd = io_open_chunk(arg, chunk_no, start_offs, end_offs);
  if (fd < 0)
    goto ERROR;

  if (write_all(fd, arg->buffer + *pos, len) != 0)
    goto ERROR1;

  *pos += len;

  result = 0;

ERROR1:
  if (close(fd) != 0)
    logerr("close(): %s", strerror(errno));

ERROR:
  return result;
}

static int io_write_chunks (struct io_thread_arg *arg)
{
  uint64_t start_chunk, end_chunk, start_offs, end_offs;
  uint32_t pos = 0;

  start_chunk = arg->req.offs / CHUNKSIZE;
  end_chunk = (arg->req.offs + arg->req.len) / CHUNKSIZE;
  start_offs = arg->req.offs % CHUNKSIZE;
  end_offs = (arg->req.offs + arg->req.len) % CHUNKSIZE;

  while (start_chunk < end_chunk) {
    if (io_write_chunk(arg, start_chunk, start_offs, CHUNKSIZE, &pos) != 0)
      goto ERROR;
    start_chunk++;
    start_offs = 0;
  }

  if (io_write_chunk(arg, start_chunk, start_offs, end_offs, &pos) != 0)
    goto ERROR;

  return io_send_reply(arg, 0, 0);

ERROR:
  io_send_reply(arg, EIO, 0);
  return -1;
}

static void *io_worker (void *arg0)
{
  struct io_thread_arg *arg = (struct io_thread_arg*) arg0;
  int res;

  if (block_signals() != 0)
    goto ERROR;

  if ((res = pthread_setname_np(pthread_self(), "I/O worker")) != 0) {
    logerr("pthread_setname_np(): %s", strerror(res));
    goto ERROR;
  }

  if ((res = pthread_mutex_lock(&arg->wakeup_mtx)) != 0) {
    logerr("pthread_mutex_lock(): %s", strerror(res));
    goto ERROR;
  }

  for (;;) {
    arg->busy = 0;

    if ((res = pthread_cond_wait(&arg->wakeup_cond, &arg->wakeup_mtx)) != 0) {
      logerr("pthread_cond_wait(): %s", strerror(res));
      break;
    }

    if (!running)
      break;

    if (!arg->busy)
      continue;

    if (ntohl(arg->req.magic) != NBD_REQUEST_MAGIC) {
      logerr("%s", "request without NDB_REQUEST_MAGIC");
      io_send_reply(arg, EINVAL, 0);
      continue;
    }

    arg->req.offs = ntohll(arg->req.offs);

    switch (arg->req.type & NBD_CMD_MASK_COMMAND) {
      case NBD_CMD_READ:
        io_read_chunks(arg);
        break;
      case NBD_CMD_WRITE:
        io_write_chunks(arg);
        break;
      case NBD_CMD_FLUSH:
        if (syncfs(arg->cachedir_fd) == 0)
          io_send_reply(arg, 0, 0);
        else {
          logerr("syncfs(): %s", strerror(errno));
          io_send_reply(arg, EIO, 0);
        }
        break;
      default:
        logerr("unknown request type: %u", arg->req.type);
        io_send_reply(arg, EIO, 0);
        break;
    }
  }

  if ((res = pthread_mutex_unlock(&arg->wakeup_mtx)) != 0)
    logerr("pthread_mutex_unlock(): %s", strerror(res));

ERROR:
  return NULL;
}

static int get_device_by_name (struct client_thread_arg *arg)
{
  unsigned int i;
  int result;

/* TODO: mutex lock global cfg here */
  for (i = 0; i < cfg.num_devices; i++)
    if (!strcmp(cfg.devs[i].name, arg->dev.name))
      break;

  if (i < cfg.num_devices) {
    memcpy(&arg->dev, &cfg.devs[i], sizeof(arg->dev));
    result = 0;
  } else
    result = -1;

  return result;
}

static int nbd_send_reply (int socket, uint32_t opt_type, uint32_t reply_type,
                           char *reply_data)
{
  uint32_t reply_len;
  int len, res;

  res = write_all(socket, NBD_OPTS_REPLY_MAGIC, sizeof(NBD_OPTS_REPLY_MAGIC));
  if (res != 0)
    return -1;

  if (write_all(socket, &opt_type, sizeof(opt_type)) != 0)
    return -1;

  reply_type = htonl(reply_type);
  if (write_all(socket, &reply_type, sizeof(reply_type)) != 0)
    return -1;

  if ((reply_data == NULL) || (*reply_data == '\0')) {
    reply_len = 0;
    if (write_all(socket, &reply_len, sizeof(reply_len)) != 0)
      return -1;
  } else {
    len = strlen(reply_data);
    reply_len = htonl(len + sizeof(reply_len));

    if (write_all(socket, &reply_len, sizeof(reply_len)) != 0)
      return -1;

    reply_len = htonl(len);
    if (write_all(socket, &reply_len, sizeof(reply_len)) != 0)
      return -1;

    if (write_all(socket, reply_data, len) != 0)
      return -1;
  }

  return 0;
}

static int nbd_send_devicelist (int socket, uint32_t opt_type)
{
  unsigned int i;

/* XXX: mutex lock global cfg here? */
  for (i = 0; i < cfg.num_devices; i++) {
    if (nbd_send_reply(socket, opt_type, NBD_REP_SERVER, cfg.devs[i].name))
      return -1;
  }

  return nbd_send_reply(socket, opt_type, NBD_REP_ACK, NULL);
}

static int nbd_send_device_info (struct client_thread_arg *arg, uint32_t flags)
{
  uint64_t devsize;
  uint16_t devflags;
  char padding[124];

  devsize = htonll(arg->dev.size);

  if (write_all(arg->socket, &devsize, sizeof(devsize)) != 0)
    return -1;

  devflags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;
  devflags = htons(devflags);

  if (write_all(arg->socket, &devflags, sizeof(devflags)) != 0)
    return -1;

  if ((flags & NBD_FLAG_NO_ZEROES) != NBD_FLAG_NO_ZEROES) {
    memset(padding, 0, sizeof(padding));
    if (write_all(arg->socket, padding, sizeof(padding)) != 0)
      return -1;
  }

  return 0;
}

static int nbd_handshake (struct client_thread_arg *arg)
{
  uint32_t flags, opt_type, opt_len;
  char ihaveopt[8];
  uint16_t srv_flags;

  if (write_all(arg->socket, NBD_INIT_PASSWD, sizeof(NBD_INIT_PASSWD)) != 0)
    return -1;

  if (write_all(arg->socket, NBD_OPTS_MAGIC, sizeof(NBD_OPTS_MAGIC)) != 0)
    return -1;

  srv_flags = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
  srv_flags = htons(srv_flags);
  if (write_all(arg->socket, &srv_flags, sizeof(srv_flags)) != 0)
    return -1;

  if (read_all(arg->socket, &flags, sizeof(flags)) != 0)
    return -1;

  flags = ntohl(flags);
  if ((flags & NBD_FLAG_FIXED_NEWSTYLE) != NBD_FLAG_FIXED_NEWSTYLE) {
    logerr("client %s without NBD_FLAG_FIXED_NEWSTYLE (qemu?) may fail",
           arg->clientname);
#if 0
    return -1;
#endif
  }

  for (;;) {
    if (read_all(arg->socket, ihaveopt, sizeof(ihaveopt)) != 0)
      return -1;

    if (memcmp(ihaveopt, NBD_OPTS_MAGIC, sizeof(NBD_OPTS_MAGIC)) != 0) {
      logerr("client %s sent no NBD_OPTS_MAGIC", arg->clientname);
      return -1;
    }

    if (read_all(arg->socket, &opt_type, sizeof(opt_type)) != 0)
      return -1;

    if (read_all(arg->socket, &opt_len, sizeof(opt_len)) != 0)
      return -1;

    opt_len = ntohl(opt_len);
    if (opt_len >= NBD_BUFSIZE) {
      logerr("client %s opt_len %u too large", arg->clientname, opt_len);
      nbd_send_reply(arg->socket, opt_type, NBD_REP_ERR_INVALID, NULL);
      return -1;
    }

    if (opt_len > 0) {
      if (read_all(arg->socket, arg->dev.name, opt_len) != 0)
        return -1;

      arg->dev.name[opt_len] = '\0';
    }

    switch (ntohl(opt_type)) {
      case NBD_OPT_EXPORT_NAME:
        if (get_device_by_name(arg) != 0) {
          logerr("client %s unknown device %s",
                 arg->clientname, arg->dev.name);
          return -1;
        }
        return nbd_send_device_info(arg, flags);

      case NBD_OPT_ABORT:
        if (opt_len == 0)
          nbd_send_reply(arg->socket, opt_type, NBD_REP_ACK, NULL);
        else
          nbd_send_reply(arg->socket, opt_type, NBD_REP_ERR_INVALID, NULL);
        return -1;

      case NBD_OPT_LIST:
        if (opt_len == 0) {
          if (nbd_send_devicelist(arg->socket, opt_type) != 0)
            return -1;
        } else {
          if (nbd_send_reply(arg->socket, opt_type, NBD_REP_ERR_INVALID, NULL) != 0)
            return -1;
        }
        break;

      default:
        logerr("client %s unknown opt_type %u", arg->clientname, opt_type);
        if (nbd_send_reply(arg->socket, opt_type, NBD_REP_ERR_UNSUP, NULL) != 0)
          return -1;
        break;
    }
  }
}

static struct io_thread_arg *find_free_io_worker ()
{
  int res;
  unsigned int i, round;
  struct timespec holdon = { .tv_sec = 0, .tv_nsec = 500 };

  for (round = 0;; round++) {
    if (round > 10000)
      nanosleep(&holdon, NULL);

    i = round % num_io_threads;

    res = pthread_mutex_trylock(&io_threads[i].wakeup_mtx);
    if (res == EBUSY)
      continue;

    if (res != 0) {
      logerr("pthread_mutex_trylock(): %s", strerror(res));
      return NULL;
    }

    if (!io_threads[i].busy)
      return &io_threads[i];

    if ((res = pthread_mutex_unlock(&io_threads[i].wakeup_mtx)) != 0) {
      logerr("pthread_mutex_unlock(): %s", strerror(res));
      return NULL;
    }
  }
}

static void client_address (struct client_thread_arg *arg)
{
  char addr[INET6_ADDRSTRLEN];
  struct sockaddr_in *sin;
  struct sockaddr_in6 *sin6;

  switch (arg->addr.sa_family) {
    case AF_INET:
      sin = (struct sockaddr_in*) &arg->addr;
      snprintf(arg->clientname, sizeof(arg->clientname), "%s:%u",
               inet_ntop(sin->sin_family, &sin->sin_addr, addr, sizeof(addr)),
               htons(sin->sin_port));
      break;
    case AF_INET6:
      sin6 = (struct sockaddr_in6*) &arg->addr;
      snprintf(arg->clientname, sizeof(arg->clientname), "[%s]:%u",
               inet_ntop(sin6->sin6_family, &sin6->sin6_addr, addr,
                         sizeof(addr)),
               htons(sin6->sin6_port));
      break;
    default:
      snprintf(arg->clientname, sizeof(arg->clientname), "%s",
               "<unknown address family>");
      break;
  }
}

static int client_worker_loop (struct client_thread_arg *arg)
{
  fd_set rfds;
  struct timeval timeout;
  struct io_thread_arg *slot;
  int res, result = -1;

  FD_ZERO(&rfds);
  FD_SET(arg->socket, &rfds);

  timeout.tv_sec = 1;
  timeout.tv_usec = 0;

  res = select(arg->socket + 1, &rfds, NULL, NULL, &timeout);
  if (res == 0)
    return 0;

  if (res < 0) {
    logerr("select(): %s", strerror(errno));
    goto ERROR;
  }

  if (!running)
    goto ERROR;

  slot = find_free_io_worker();
  if (slot == NULL)
    goto ERROR;

  if (read_all(arg->socket, &slot->req, sizeof(slot->req)) != 0)
    goto ERROR1;

  slot->req.len = ntohl(slot->req.len);
  if (slot->req.len > slot->buflen) {
    slot->buflen = slot->req.len;
    slot->buffer = realloc(slot->buffer, slot->buflen);

    if (slot->buffer == NULL) {
      logerr("%s", "realloc() failed");
      goto ERROR1;
    }
  }

  slot->req.type = ntohl(slot->req.type);
  switch (slot->req.type & NBD_CMD_MASK_COMMAND) {
    case NBD_CMD_WRITE:
      if ((slot->req.len > 0) &&
          (read_all(arg->socket, slot->buffer, slot->req.len) != 0))
        goto ERROR1;
      break;

    case NBD_CMD_DISC:
      goto ERROR1;

    default:
      break;
  }

  slot->socket = arg->socket;
  slot->socket_mtx = &arg->socket_mtx;
  strcpy(slot->devicename, arg->dev.name);
  slot->cachedir_fd = arg->cachedir_fd;

  if ((res = pthread_cond_signal(&slot->wakeup_cond)) != 0) {
    logerr("pthread_cond_signal(): %s", strerror(res));
    goto ERROR1;
  }

  slot->busy = 1;
  result = 0;

ERROR1:
  if ((res = pthread_mutex_unlock(&slot->wakeup_mtx)) != 0) {
    logerr("pthread_mutex_unlock(): %s", strerror(res));
    result = -1;
  }

ERROR:
  return result;
}

static void *client_worker (void *arg0)
{
  struct client_thread_arg *arg = (struct client_thread_arg*) arg0;
  int res;

  if (block_signals() != 0)
    goto ERROR;

  if ((res = pthread_setname_np(pthread_self(), "Client worker")) != 0) {
    logerr("pthread_setname_np(): %s", strerror(res));
    goto ERROR;
  }

  client_address(arg);

  if (nbd_handshake(arg) != 0)
    goto ERROR;

  if ((res = pthread_mutex_init(&arg->socket_mtx, NULL)) != 0) {
    logerr("pthread_mutex_init(): %s", strerror(res));
    goto ERROR;
  }

  if ((arg->cachedir_fd = open(arg->dev.cachedir, O_RDONLY|O_DIRECTORY)) < 0) {
    logerr("open(): %s", strerror(errno));
    goto ERROR1;
  }

  syslog(LOG_INFO, "client %s connecting to device %s\n", arg->clientname,
         arg->dev.name);

  while (client_worker_loop(arg) == 0)
    ;;

  syslog(LOG_INFO, "client %s disconnecting from device %s\n", arg->clientname,
         arg->dev.name);

  if (close(arg->cachedir_fd) != 0)
    logerr("close(): %s", strerror(errno));

ERROR1:
  if ((res = pthread_mutex_destroy(&arg->socket_mtx)) != 0)
    logerr("pthread_mutex_destroy(): %s", strerror(res));

ERROR:
  if (close(arg->socket) != 0)
    logerr("close(): %s", strerror(errno));

  free(arg0);
  return NULL;
}

static void sigterm_handler (int sig __attribute__((unused)))
{
  syslog(LOG_INFO, "SIGTERM received, going down...\n");
  running = 0;
}

static void sighup_handler (int sig __attribute__((unused)))
{
  syslog(LOG_INFO, "SIGHUP received, reloading configuration...\n");
  reload_config = 1;
}

static void setup_signal (int sig, void (*handler)(int))
{
  struct sigaction sa;

  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = handler;
  if (sigaction(sig, &sa, NULL) != 0)
    err(1, "signal()");
}

static void setup_signals ()
{
  sigset_t sigset;

  if (sigfillset(&sigset) != 0)
    err(1, "sigfillset()");

  if ((sigdelset(&sigset, SIGTERM) != 0) || (sigdelset(&sigset, SIGHUP) != 0))
    err(1, "sigdelset()");

  if (pthread_sigmask(SIG_SETMASK, &sigset, NULL) != 0)
    err(1, "sigprocmask()");

  setup_signal(SIGTERM, sigterm_handler);
  setup_signal(SIGHUP, sighup_handler);
}

static int create_listen_socket_inet (char *ip, char *port)
{
  int listen_socket, yes, res;
  struct addrinfo hints, *result, *walk;

  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = AI_NUMERICHOST | AI_PASSIVE | AI_NUMERICSERV;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((res = getaddrinfo(ip, port, &hints, &result)) != 0)
    errx(1, "getaddrinfo(): %s", gai_strerror(res));

  for (walk = result;;) {
    listen_socket = socket(walk->ai_family, walk->ai_socktype, 0);
    if ((listen_socket >= 0) &&
        (bind(listen_socket, walk->ai_addr, walk->ai_addrlen) == 0))
      break;

    if (walk->ai_next == NULL)
      err(1, "bind()");

    close(listen_socket);
  }

  freeaddrinfo(result);

  yes = 1;
  res = setsockopt(listen_socket, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
  if (res != 0)
    err(1, "setsockopt()");

  yes = 1;
  res = setsockopt(listen_socket, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes));
  if (res != 0)
    err(1, "setsockopt()");

  return listen_socket;
}

static int create_listen_socket_unix (char *ip)
{
  int listen_socket;
  struct sockaddr_un sun;

  if (strlen(ip) >= sizeof(sun.sun_path))
    errx(1, "path of unix socket too long");

  listen_socket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_socket < 0)
    err(1, "socket()");

  sun.sun_family = AF_UNIX;
  strncpy(sun.sun_path, ip, sizeof(sun.sun_path));

  if (bind(listen_socket, (struct sockaddr*) &sun, sizeof(sun)) != 0)
    err(1, "bind()");

  return listen_socket;
}

static int create_listen_socket (char *ip, char *port)
{
  int listen_socket;

  if (*ip == '/')
    listen_socket = create_listen_socket_unix(ip);
  else
    listen_socket = create_listen_socket_inet(ip, port);

  if (listen(listen_socket, 0) != 0)
    err(1, "listen()");

  return listen_socket;
}

static void show_help ()
{
  puts(
"Usage:\n"
"\n"
"s3nbd [-c <config file>] [-p <pid file>] [-f]\n"
"s3nbd -h\n"
"\n"
"  -c <config file>    read config options from specified file instead of\n"
"                      " DEFAULT_CONFIGFILE "\n"
"\n"
"  -p <pid file>       save pid to this file\n"
"\n"
"  -f                  run in foreground, don't daemonize\n"
"\n"
"  -h                  show this help ;-)\n"
);
}

static void launch_io_workers ()
{
  int i, res;

  for (i = 0; i < num_io_threads; i++) {
    io_threads[i].busy = 1;
    io_threads[i].buflen = 1024 * 1024;
    io_threads[i].buffer = malloc(io_threads[i].buflen);

    if (io_threads[i].buffer == NULL)
      errx(1, "malloc() failed");

    if ((res = pthread_cond_init(&io_threads[i].wakeup_cond, NULL)) != 0)
      errx(1, "pthread_cond_init(): %s", strerror(res));

    if ((res = pthread_mutex_init(&io_threads[i].wakeup_mtx, NULL)) != 0)
      errx(1, "pthread_mutex_init(): %s", strerror(res));

    res = pthread_create(&io_threads[i].thread, NULL, &io_worker,
                         &io_threads[i]);
    if (res != 0)
      errx(1, "pthread_create(): %s", strerror(res));
  }
}

static void join_io_workers ()
{
  int i, res;

  for (i = 0; i < num_io_threads; i++) {
    if ((res = pthread_mutex_lock(&io_threads[i].wakeup_mtx)) != 0)
      syslog(LOG_ERR, "pthread_mutex_lock(): %s", strerror(res));
    if ((res = pthread_cond_signal(&io_threads[i].wakeup_cond)) != 0)
      syslog(LOG_ERR, "pthread_cond_signal(): %s", strerror(res));
    if ((res = pthread_mutex_unlock(&io_threads[i].wakeup_mtx)) != 0)
      syslog(LOG_ERR, "pthread_mutex_unlock(): %s", strerror(res));
    if ((res = pthread_join(io_threads[i].thread, NULL)) != 0)
      syslog(LOG_ERR, "pthread_join(): %s", strerror(res));
    if ((res = pthread_mutex_destroy(&io_threads[i].wakeup_mtx)) != 0)
      syslog(LOG_ERR, "pthread_mutex_destroy(): %s", strerror(res));
    if ((res = pthread_cond_destroy(&io_threads[i].wakeup_cond)) != 0)
      syslog(LOG_ERR, "pthread_cond_destroy(): %s", strerror(res));
    free(io_threads[i].buffer);
  }
}

static void daemonize ()
{
  pid_t pid;

  if ((pid = fork()) < 0)
    err(1, "fork()");

  if (pid > 0)
    exit(0);

  if (setsid() == -1)
    err(1, "setsid()");

  if (setpgrp() != 0)
    err(1, "setpgrp()");

  if (chdir("/"))
    err(1, "chdir(): /");
}

static void save_pidfile (char *pidfile)
{
  FILE *fh;

  if ((fh = fopen(pidfile, "wx")) == NULL)
    err(1, "fopen(): %s", pidfile);

  fprintf(fh, "%u\n", getpid());

  if (fclose(fh) != 0)
    err(1, "fclose(): %s", pidfile);
}

int main (int argc, char **argv)
{
#define log_error(fmt, params ...) do { \
  if (foreground) warnx(fmt "\n", ## params); \
  else syslog(LOG_WARNING, fmt "\n", ## params); \
} while (0)

  char *configfile = DEFAULT_CONFIGFILE, *pidfile = NULL, *errstr;
  int foreground = 0, listen_socket, res;
  unsigned int errline;
  pthread_attr_t thread_attr;
  pthread_t thread;
  struct client_thread_arg *thread_arg;

  while ((res = getopt(argc, argv, "c:fhp:")) != -1) {
    switch (res) {
      case 'c': configfile = optarg; break;
      case 'f': foreground = 1; break;
      case 'h': show_help(); return 0;
      case 'p': pidfile = optarg; break;
      default: errx(1, "Unknown option '%i'. Use -h for help.", res);
    }
  }

  if (load_config(configfile, &cfg, &errline, &errstr) != 0)
    errx(1, "cannot load config file %s: %s (line %i)",
         configfile, errstr, errline);

  num_io_threads = cfg.num_io_threads;

  if (!foreground)
    daemonize();

  if (pidfile != NULL)
    save_pidfile(pidfile);

  setup_signals();
  listen_socket = create_listen_socket(cfg.listen, cfg.port);
  launch_io_workers();

  if ((res = pthread_attr_init(&thread_attr)) != 0)
    errx(1, "pthread_attr_init(): %s", strerror(res));
  res = pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED);
  if (res != 0)
    errx(1, "pthread_attr_setdetachstate(): %s", strerror(res));

  if (!foreground) {
    close(0);
    close(1);
    close(2);
  }

  openlog("s3nbd", LOG_NDELAY|LOG_PID, LOG_DAEMON);
  syslog(LOG_INFO, "starting...\n");

  while (running) {
    if (reload_config &&
        (load_config(configfile, &cfg, &errline, &errstr) != 0))
      log_error("cannot reload config file %s: %s (line %i)",
                configfile, errstr, errline);

    thread_arg = malloc(sizeof(*thread_arg));
    if (thread_arg == NULL) {
      log_error("%s", "malloc() failed");
      break;
    }

    thread_arg->addr_len = sizeof(thread_arg->addr);
    thread_arg->socket = accept(listen_socket, &thread_arg->addr,
                                &thread_arg->addr_len);

    if (thread_arg->socket < 0) {
      if (errno != EINTR) {
        log_error("accept(): %s", strerror(errno));
        break;
      }

      free(thread_arg);
      continue;
    }

    res = pthread_create(&thread, &thread_attr, &client_worker, thread_arg);
    if (res != 0) {
      log_error("pthread_create(): %s", strerror(res));
      break;
    }
  }

  running = 0;

  syslog(LOG_INFO, "waiting for I/O workers...\n");
  join_io_workers();

  if (unlink(pidfile) != 0)
    log_error("unlink(): %s: %s", pidfile, strerror(errno));

  syslog(LOG_INFO, "exiting...\n");
  closelog();

  return 0;
}
