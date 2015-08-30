#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <err.h>
#include <netdb.h>
#include <errno.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/un.h>

#define __USE_GNU
#include <pthread.h>

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
#define NBD_FLAG_SEND_FUA 8
const char const NBD_MAGIC[] = { 'N','B','D','M','A','G','I','C' };
const char const NBD_IHAVEOPT[] = { 'I','H','A','V','E','O','P','T' };
const char const NBD_REPLY_MAGIC[] = { 0x00, 0x03, 0xe8, 0x89,
                                       0x04, 0x55, 0x65, 0xa9 };

struct client_thread_arg {
  int socket;
  struct sockaddr addr;
  socklen_t addr_len;
};

struct io_thread_arg {
  pthread_t thread;
  pthread_cond_t cond;
  pthread_mutex_t mtx;
  int socket;
  pthread_mutex_t *sock_mtx;
};

int running = 1;
int num_io_threads = 16;
struct io_thread_arg *io_threads;

void set_client_thread_name (struct client_thread_arg *client)
{
  char name[16], addr[INET6_ADDRSTRLEN];
  struct sockaddr_in *sin;
  struct sockaddr_in6 *sin6;

  switch (client->addr.sa_family) {
    case AF_INET:
      sin = (struct sockaddr_in*) &client->addr;
      snprintf(name, sizeof(name), "client %s:%u",
               inet_ntop(sin->sin_family, &sin->sin_addr, addr, sizeof(addr)),
               htons(sin->sin_port));
      break;
    case AF_INET6:
      sin6 = (struct sockaddr_in6*) &client->addr;
      snprintf(name, sizeof(name), "client [%s]:%u",
               inet_ntop(sin6->sin6_family, &sin6->sin6_addr, addr,
                         sizeof(addr)),
               htons(sin6->sin6_port));
      break;
    default:
      snprintf(name, sizeof(name), "client %s", "<unknown address family>");
      break;
  }
puts(name);
  pthread_setname_np(pthread_self(), name);
}

int block_signals ()
{
  sigset_t sigset;

  if (sigfillset(&sigset) != 0)
    return -1;

  if (pthread_sigmask(SIG_SETMASK, &sigset, NULL) != 0)
    return -1;

  return 0;
}

void *io_worker (void *arg0)
{
  struct io_thread_arg *arg = (struct io_thread_arg*) arg0;
  int res;

  if (block_signals() != 0)
    goto ERROR;

  if (pthread_setname_np(pthread_self(), "I/O worker") != 0)
    goto ERROR;

  if (pthread_mutex_lock(&arg->mtx) != 0)
    goto ERROR;

  for (;;) {
    res = pthread_cond_wait(&arg->cond, &arg->mtx);
    if (res != 0)
      break;
    if (!running)
      break;
  }

  pthread_mutex_unlock(&arg->mtx);

ERROR:
  return NULL;
}

int nbd_send_reply (int socket, uint32_t opt_type, uint32_t reply_type,
                    char *reply_data)
{
  uint32_t reply_len;
  int len;
  ssize_t written;

  written =  write(socket, NBD_REPLY_MAGIC, sizeof(NBD_REPLY_MAGIC));
  if (written != sizeof(NBD_REPLY_MAGIC))
    return -1;

  if (write(socket, &opt_type, sizeof(opt_type)) != sizeof(opt_type))
    return -1;

  reply_type = htonl(reply_type);
  if (write(socket, &reply_type, sizeof(reply_type)) != sizeof(reply_type))
    return -1;

  if ((reply_data == NULL) || (*reply_data == '\0')) {
    reply_len = 0;
    if (write(socket, &reply_len, sizeof(reply_len)) != sizeof(reply_len))
      return -1;
  } else {
    len = strlen(reply_data);
    reply_len = htonl(len + sizeof(reply_len));

    if (write(socket, &reply_len, sizeof(reply_len)) != sizeof(reply_len))
      return -1;

    reply_len = htonl(len);
    if (write(socket, &reply_len, sizeof(reply_len)) != sizeof(reply_len))
      return -1;

    if (write(socket, reply_data, len) != len)
      return -1;
  }

  return 0;
}

int nbd_send_devicelist (int socket, uint32_t opt_type)
{
  char *devlist[]={"brwatzwurt", "klowasser"};
  unsigned int i;

  for (i = 0; i < sizeof(devlist)/sizeof(devlist[0]); i++) {
    if (nbd_send_reply(socket, opt_type, NBD_REP_SERVER, devlist[i]))
      return -1;
  }

  return nbd_send_reply(socket, opt_type, NBD_REP_ACK, NULL);
}

int nbd_send_device_info (int socket, char *devicename, uint32_t flags)
{
  uint64_t devsize;
  uint32_t devflags;
  char padding[124];

  if (strcmp(devicename, "tehdisk"))
    return -1;

  devsize = 100 * 1024 * 1024;
  devsize = htonl(devsize);

  if (write(socket, &devsize, sizeof(devsize)) != sizeof(devsize))
    return -1;

  devflags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_FUA;
  devflags = htons(devflags);

  if (write(socket, &devflags, sizeof(devflags)) != sizeof(devflags))
    return -1;

  if ((flags & NBD_FLAG_NO_ZEROES) != NBD_FLAG_NO_ZEROES) {
    memset(padding, 0, sizeof(padding));
    if (write(socket, padding, sizeof(padding)) != sizeof(padding))
      return -1;
  }

  return 0;
}

int nbd_handshake (int socket, char *devicename)
{
  uint32_t flags, opt_type, opt_len;
  char ihaveopt[8];
  uint16_t srv_flags;

  if (write(socket, NBD_MAGIC, sizeof(NBD_MAGIC)) != sizeof(NBD_MAGIC))
    return -1;

  if (write(socket, NBD_IHAVEOPT, sizeof(NBD_IHAVEOPT)) != sizeof(NBD_IHAVEOPT))
    return -1;

  srv_flags = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
  srv_flags = htons(srv_flags);
  if (write(socket, &srv_flags, sizeof(srv_flags)) != sizeof(srv_flags))
    return -1;

  if (read(socket, &flags, sizeof(flags)) != sizeof(flags))
    return -1;

  flags = ntohl(flags);
  if ((flags & NBD_FLAG_FIXED_NEWSTYLE) != NBD_FLAG_FIXED_NEWSTYLE)
    return -1;

  for (;;) {
    if (read(socket, ihaveopt, sizeof(ihaveopt)) != sizeof(ihaveopt))
      return -1;

    if (memcmp(ihaveopt, NBD_IHAVEOPT, sizeof(NBD_IHAVEOPT)) != 0)
      return -1;

    if (read(socket, &opt_type, sizeof(opt_type)) != sizeof(opt_type))
      return -1;

    if (read(socket, &opt_len, sizeof(opt_len)) != sizeof(opt_len))
      return -1;

    opt_len = ntohl(opt_len);
    if (opt_len >= NBD_BUFSIZE) {
      nbd_send_reply(socket, opt_type, NBD_REP_ERR_INVALID, NULL);
      return -1;
    }

    if (opt_len > 0) {
      if (read(socket, devicename, opt_len) != opt_len)
        return -1;
    }

    switch (ntohl(opt_type)) {
      case NBD_OPT_EXPORT_NAME:
        devicename[opt_len] = '\0';
        return nbd_send_device_info(socket, devicename, flags);

      case NBD_OPT_ABORT:
        if (opt_len == 0)
          nbd_send_reply(socket, opt_type, NBD_REP_ACK, NULL);
        else
          nbd_send_reply(socket, opt_type, NBD_REP_ERR_INVALID, NULL);
        return -1;

      case NBD_OPT_LIST:
        if (opt_len == 0) {
          if (nbd_send_devicelist(socket, opt_type) != 0)
            return -1;
        } else {
          if (nbd_send_reply(socket, opt_type, NBD_REP_ERR_INVALID, NULL) != 0)
            return -1;
        }
        break;

      default:
        if (nbd_send_reply(socket, opt_type, NBD_REP_ERR_UNSUP, NULL) != 0)
          return -1;
        break;
    }
  }
}

void *client_worker (void *arg0) {
  struct client_thread_arg *arg = (struct client_thread_arg*) arg0;
  char devicename[NBD_BUFSIZE];

  if (block_signals() != 0)
    goto ERROR;

  if (pthread_setname_np(pthread_self(), "Client worker") != 0)
    goto ERROR;

  if (nbd_handshake(arg->socket, devicename) != 0)
    goto ERROR;

ERROR:
  close(arg->socket);
  free(arg0);
  return NULL;
}

void sigterm_handler (int sig)
{
  running = 0;
}

void setup_signals ()
{
  sigset_t sigset;
  struct sigaction sa;

  if (sigfillset(&sigset) != 0) err(1, "sigfillset()");
  if (sigdelset(&sigset, SIGTERM) != 0) err(1, "sigdelset()");
  if (pthread_sigmask(SIG_SETMASK, &sigset, NULL) != 0)
    err(1, "sigprocmask()");

  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = sigterm_handler;
  if (sigaction(SIGTERM, &sa, NULL) != 0) err(1, "signal()");
}

int create_listen_socket_inet (char *ip, char *port)
{
  int listen_socket, reuse, res;
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

    if (walk->ai_next == NULL) err(1, "bind()");

    close(listen_socket);
  }

  freeaddrinfo(result);

  reuse = 1;
  res = setsockopt(listen_socket, SOL_SOCKET, SO_REUSEPORT, &reuse,
                   sizeof(reuse));
  if (res != 0) err(1, "setsockopt()");

  return listen_socket;
}

int create_listen_socket_unix (char *ip)
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

int create_listen_socket (char *ip, char *port)
{
  int listen_socket;

  if (*ip == '/')
    listen_socket = create_listen_socket_unix(ip);
  else
    listen_socket = create_listen_socket_inet(ip, port);

  if (listen(listen_socket, 0) != 0) err(1, "listen()");

  return listen_socket;
}

void show_help ()
{
  puts(
"Usage:\n"
"\n"
"s3nbd [-c <config directory>] [-f] [-l <ip>] [-p <port>]\n"
"s3nbd -h\n"
"\n"
"  -c <config directory>    read config options from specified directory\n"
"                           instead of /etc/s3nbd\n"
"\n"
"  -f                       run in foreground, don't daemonize\n"
"\n"
"  -l <ip>                  listen on specified IPv4/IPv6 address, or \n"
"                           Unix socket; default: 0.0.0.0\n"
"\n"
"  -p <port>                listen on specified port instead of 10809\n"
"\n"
"  -h                       show this help ;-)\n"
);
}

void launch_io_workers ()
{
  int i, res;

  io_threads = malloc(num_io_threads * sizeof(struct io_thread_arg));
  if (io_threads == NULL)
    errx(1, "malloc() failed");

  for (i = 0; i < num_io_threads; i++) {
    if ((res = pthread_cond_init(&io_threads[i].cond, NULL)) != 0)
      errx(1, "pthread_cond_init(): %s", strerror(res));

    if ((res = pthread_mutex_init(&io_threads[i].mtx, NULL)) != 0)
      errx(1, "pthread_mutex_init(): %s", strerror(res));

    res = pthread_create(&io_threads[i].thread, NULL, &io_worker,
                         &io_threads[i]);
    if (res != 0)
      errx(1, "pthread_create(): %s", strerror(res));
  }
}

void join_io_workers ()
{
  int i, res;

  for (i = 0; i < num_io_threads; i++) {
    if ((res = pthread_mutex_lock(&io_threads[i].mtx)) != 0)
      warnx("pthread_mutex_lock(): %s", strerror(res));
    else if ((res = pthread_cond_signal(&io_threads[i].cond)) != 0)
      warnx("pthread_cond_signal(): %s", strerror(res));
    else if ((res = pthread_mutex_unlock(&io_threads[i].mtx)) != 0)
      warnx("pthread_mutex_unlock(): %s", strerror(res));
    else if ((res = pthread_join(io_threads[i].thread, NULL)) != 0)
      warnx("pthread_join(): %s", strerror(res));
    else if ((res = pthread_mutex_destroy(&io_threads[i].mtx)) != 0)
      warnx("pthread_mutex_destroy(): %s", strerror(res));
    else if ((res = pthread_cond_destroy(&io_threads[i].cond)) != 0)
      warnx("pthread_cond_destroy(): %s", strerror(res));
  }

  free(io_threads);
}

int main (int argc, char **argv)
{
  char *ip = "0.0.0.0", *port = "10809", *configdir = "/etc/s3nbd";
  int foreground = 0, listen_socket, res;
  pthread_attr_t thread_attr;
  pthread_t thread;
  struct client_thread_arg *thread_arg;

  while ((res = getopt(argc, argv, "hc:fl:p:")) != -1) {
    switch (res) {
      case 'c': configdir = optarg; break;
      case 'f': foreground = 1; break;
      case 'l': ip = optarg; break;
      case 'p': port = optarg; break;
      case 'h': show_help(); return 0;
      default: errx(1, "Unknown option '%i'. Use -h for help.", res);
    }
  }

  setup_signals();
  listen_socket = create_listen_socket(ip, port);
  launch_io_workers();

  if ((res = pthread_attr_init(&thread_attr)) != 0)
    errx(1, "pthread_attr_init(): %s", strerror(res));
  res = pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED);
  if (res != 0)
    errx(1, "pthread_attr_setdetachstate(): %s", strerror(res));

  while (running) {
    thread_arg = malloc(sizeof(*thread_arg));
    if (thread_arg == NULL) {
      warnx("malloc() failed");
      break;
    }

    thread_arg->addr_len = sizeof(thread_arg->addr);
    thread_arg->socket = accept(listen_socket, &thread_arg->addr,
                                &thread_arg->addr_len);

    if (thread_arg->socket < 0) {
      if (errno != EINTR) {
        warn("accept()");
        break;
      }
      free(thread_arg);
      continue;
    }

    res = pthread_create(&thread, &thread_attr, &client_worker, thread_arg);
    if (res != 0) {
      warnx("pthread_create(): %s", strerror(res));
      break;
    }
  }

  running = 0;
  join_io_workers();

  return 0;
}
