#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>

/* 前向声明，解决结构体互相引用 */
struct NMANAGER;

struct NJOB {
    void (*func)(struct NJOB *job);
    void *user_data;
    struct NJOB *prev;
    struct NJOB *next;
};

struct NWORKER {
    pthread_t thread;
    struct NMANAGER *pool;
    int terminate;
    struct NWORKER *prev;
    struct NWORKER *next;
};

struct NMANAGER {
    struct NWORKER *workers;
    struct NJOB *jobs;
    pthread_cond_t jobs_cond;
    pthread_mutex_t jobs_mutex;
};

typedef struct NMANAGER nThreadPool;

/* API */
int  nThreadPoolCreate(nThreadPool *pool, int numWorkers);
int  nThreadPoolDestroy(nThreadPool *pool);
void nThreadPoolPush(nThreadPool *pool, struct NJOB *job);

#endif /* THREAD_POOL_H */