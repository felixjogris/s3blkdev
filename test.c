#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <err.h>

#define __USE_GNU
#include <fcntl.h>

int main (int argc, char **argv)
{
  int fd;
  struct flock flk;
  struct stat st;
  char buf[16];

  if (argc != 5) errx(1, "usage: lock_chunk <r|w> <start> <end> /path/to/chunk");

  fd = open(argv[4], O_RDWR);
  if (fd < 0) err(1, "open()");

  flk.l_type = (argv[1][0] == 'r' ? F_RDLCK : F_WRLCK);
  flk.l_whence = SEEK_SET;
  flk.l_start = atol(argv[2]);
  flk.l_len = atol(argv[3]);
  flk.l_pid = 0;

  if (fcntl(fd, F_OFD_SETLK, &flk) != 0)
    err(1, "fcntl()");

  if (stat(argv[4], &st) != 0)
    warn("stat()");
  else if (st.st_size != 8*1024*1024)
    warnx("filesize != CHUNKSIZE");

  read(0, buf, sizeof(buf));

  return 0;
}

#if 0
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

int running=1;
int recv=0;
int sent=0;
int new_data=1;

void *worker (void *arg)
{
  pthread_mutex_lock(&mtx);

  new_data=0;

  while (running) {
    while (!new_data)
      pthread_cond_wait(&cond, &mtx);
    recv++;
    new_data = 0;
  }

  pthread_mutex_unlock(&mtx);

  return arg;
}

int main ()
{
  pthread_t thread;
  int i;

  pthread_create(&thread, NULL, &worker, NULL);

  for (i=0; i < 1000; i++) {
    pthread_mutex_lock(&mtx);
    if (!new_data) {
      new_data = 1;
      pthread_cond_signal(&cond);
      sent++;
    }
    pthread_mutex_unlock(&mtx);
  }

  running=0;

  for (i=0; !i; )
  {
  pthread_mutex_lock(&mtx);
  if (!new_data) {
    new_data = 1;
   i=1;
    pthread_cond_signal(&cond);
    sent++;
  }
  pthread_mutex_unlock(&mtx);
  }
  printf("sent=%d recv=%d\n", sent,recv);
  pthread_join(thread, NULL);


  return 0;
}
#endif

#if 0
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <err.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/signal.h>
#include <pthread.h>

void sigterm_handler (int sig)
{
while (waitpid(-1, &sig, WNOHANG) > 0)
;;
}

void setup_signals ()
{
  sigset_t sigset;
  struct sigaction sa;

  if (sigfillset(&sigset) != 0)
    err(1, "sigfillset()");

  if (sigdelset(&sigset, SIGCHLD) != 0)
    err(1, "sigdelset()");

  if (pthread_sigmask(SIG_SETMASK, &sigset, NULL) != 0)
    err(1, "sigprocmask()");

  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = sigterm_handler;
  if (sigaction(SIGCHLD, &sa, NULL) != 0)
    err(1, "signal()");
}

int num=0;

void *worker(void *arg)
{
num--;
  return arg;
}

int main ()
{
  int i,pid;
pthread_t thread;
setup_signals();
for (i=0;i<10000;i++) {
/*
pid=fork();
if (pid<0) err(1,"fork");
if (!pid) return 0;
*/
if ((pid=pthread_create(&thread,NULL,&worker,NULL))!=0)
errx(1,"pthread_create: %s",strerror(pid));
num++;
}
printf("%d %d\n",i,num);
return 0;
}
#endif
