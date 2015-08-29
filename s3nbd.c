#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <err.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>

struct client_thread_arg {
 int socket;
 struct sockaddr addr;
 socklen_t addr_len;
};

void *client_worker (void *arg) {
  struct client_thread_arg *client = (struct client_thread_arg*) arg;

  free(arg);
  return NULL;
}

int main (int argc, char **argv)
{
  int running, error, listen_socket;
  struct addrinfo hints, *result, *walk;
  pthread_attr_t thread_attr;
  pthread_t thread;
  struct client_thread_arg *thread_arg;

  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = AI_NUMERICHOST | AI_PASSIVE | AI_NUMERICSERV;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  error = getaddrinfo("127.0.0.1", "10809", &hints, &result);
  if (error != 0) errx(1, "getaddrinfo(): %s", gai_strerror(error));

  for (walk = result; walk != NULL; walk = walk->ai_next) {
    listen_socket = socket(walk->ai_family, walk->ai_socktype, 0);
    if ((listen_socket >= 0) &&
        (bind(listen_socket, walk->ai_addr, walk->ai_addrlen) == 0)) break;
    close(listen_socket);
  }

  if (walk == NULL) errx(1, "cannot bind");
  freeaddrinfo(result);

  if (listen(listen_socket, 0) != 0) err(1, "listen()");

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

    error = pthread_create(&thread, &thread_attr, &client_worker, thread_arg);
    if (error != 0) {
      warn("pthread_create()");
      break;
    }
  }

  return 0;
}
