#include <pthread.h>

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

void *worker (void *arg)
{
  pthread_cond_wait(&cond, &mtx);

  return arg;
}

int main ()
{
  pthread_t thread;
int i;

for (i=0; i < 10; i++) {
  pthread_create(&thread, NULL, &worker, NULL);
sleep(1);
  pthread_mutex_lock(&mtx);
  pthread_cond_signal(&cond);
sleep(1);
  pthread_mutex_unlock(&mtx);
  pthread_join(thread, NULL);
}

  return 0;
}
