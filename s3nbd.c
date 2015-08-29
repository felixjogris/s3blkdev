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

#define __USE_GNU
#include <pthread.h>

struct client_thread_arg {
 int socket;
 struct sockaddr addr;
 socklen_t addr_len;
};

int running;

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

void *client_worker (void *arg) {
  struct client_thread_arg *client = (struct client_thread_arg*) arg;
  char name[128];

  pthread_setname_np(pthread_self(), "client worker");

  write(client->socket, "BLA\n", 4);
  read(client->socket, name, 120);

ERROR:
  close(client->socket);
  free(arg);
  return NULL;
}

void sigterm_handler (int sig)
{
  running = 0;
}

void block_signals ()
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

int create_listen_socket (char *ip, char *port)
{
  int listen_socket, reuse, res;
  struct addrinfo hints, *result, *walk;

  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = AI_NUMERICHOST | AI_PASSIVE | AI_NUMERICSERV;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((res = getaddrinfo(ip, port, &hints, &result)) != 0)
    errx(1, "getaddrinfo(): %s", gai_strerror(res));

  for (walk = result; walk != NULL; walk = walk->ai_next) {
    listen_socket = socket(walk->ai_family, walk->ai_socktype, 0);
    if (listen_socket < 0) {
      warn("socket()");
      continue;
    }

    if (bind(listen_socket, walk->ai_addr, walk->ai_addrlen) != 0) {
      warn("bind()");
      close(listen_socket);
      continue;
    }

    break;
  }

  if (walk == NULL) errx(1, "cannot bind socket");
  freeaddrinfo(result);

  reuse = 1;
  res = setsockopt(listen_socket, SOL_SOCKET, SO_REUSEPORT, &reuse,
                   sizeof(reuse));
  if (res != 0) err(1, "setsockopt()");

  if (listen(listen_socket, 0) != 0) err(1, "listen()");

  return listen_socket;
}

int main (int argc, char **argv)
{
  int listen_socket, res;
  pthread_attr_t thread_attr;
  pthread_t thread;
  struct client_thread_arg *thread_arg;

  block_signals();
  listen_socket = create_listen_socket("127.0.0.1", "10809");

  if (pthread_attr_init(&thread_attr) != 0) err(1, "pthread_attr_init()");
  if (pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED) != 0)
    err(1, "pthread_attr_setdetachstate()");

  running = 1;
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
      warn("pthread_create()");
      break;
    }
  }

  return 0;
}
