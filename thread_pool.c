#include "thread_pool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* 鍐呴儴閾捐〃瀹忥紝涓嶆毚闇茬粰浣跨敤鑰?*/
#define LL_ADD(item, list) do {             \
    (item)->prev = NULL;                    \
    (item)->next = (list);                  \
    if ((list) != NULL) (list)->prev = (item); \
    (list) = (item);                        \
} while (0)

#define LL_REMOVE(item, list) do {                          \
    if ((item)->prev != NULL) (item)->prev->next = (item)->next; \
    if ((item)->next != NULL) (item)->next->prev = (item)->prev; \
    if ((list) == (item)) (list) = (item)->next;            \
    (item)->prev = (item)->next = NULL;                     \
} while (0)

static void *nThreadCallback(void *arg) {
    struct NWORKER *worker = (struct NWORKER*)arg;

	while (1) {

		pthread_mutex_lock(&worker->pool->jobs_mutex);
		while (worker->pool->jobs == NULL) {
			if (worker->terminate) break;
			pthread_cond_wait(&worker->pool->jobs_cond, &worker->pool->jobs_mutex);
		}

		if (worker->terminate) {
			pthread_mutex_unlock(&worker->pool->jobs_mutex);
			break;
		}

		struct NJOB *job = worker->pool->jobs;
		LL_REMOVE(job, worker->pool->jobs);
		
		pthread_mutex_unlock(&worker->pool->jobs_mutex);

		job->func(job);
	}

	pthread_exit(NULL);
}

int nThreadPoolCreate(nThreadPool *pool, int numWorkers) {
    if (numWorkers < 1)  numWorkers = 1;
	if (pool == NULL)  return -1;
	memset(pool, 0, sizeof(nThreadPool));


	pthread_cond_t blank_cond = PTHREAD_COND_INITIALIZER;
	memcpy(&pool->jobs_cond, &blank_cond, sizeof(pthread_cond_t));

	pthread_mutex_t blank_mutex = PTHREAD_MUTEX_INITIALIZER;
	memcpy(&pool->jobs_mutex, &blank_mutex, sizeof(pthread_mutex_t));
	
	
	int i = 0;
	for (i = 0;i < numWorkers;i ++) {

		struct NWORKER *worker = (struct NWORKER*)malloc(sizeof(struct NWORKER));
		if (worker == NULL) {
			perror("malloc");
			return -2;
		}

		memset(worker, 0, sizeof(struct NWORKER));
		worker->pool = pool;

		int ret = pthread_create(&worker->thread, NULL, nThreadCallback, worker);
		if (ret) {
			perror("pthread_create");
			free(worker);
			return -3;
		}

		LL_ADD(worker, pool->workers);

	}

	return 0;
}

int nThreadPoolDestroy(nThreadPool *pool) {
    if (pool == NULL) return -1;

    /* ① 设 terminate */
    for (struct NWORKER *w = pool->workers; w != NULL; w = w->next)
        w->terminate = 1;

    /* ② 唤醒所有等待者 */
    pthread_mutex_lock(&pool->jobs_mutex);
    pthread_cond_broadcast(&pool->jobs_cond);
    pthread_mutex_unlock(&pool->jobs_mutex);

    /* ③ join + free */
    struct NWORKER *w = pool->workers;
    while (w != NULL) {
        struct NWORKER *next = w->next;
        int ret = pthread_join(w->thread, NULL);
        if (ret != 0)
            fprintf(stderr, "pthread_join failed: %d\n", ret);
        free(w);
        w = next;
    }
    pool->workers = NULL;
	return 0;
}

void nThreadPoolPush(nThreadPool *pool, struct NJOB *job) {
    pthread_mutex_lock(&pool->jobs_mutex);
	
	LL_ADD(job, pool->jobs);
	pthread_cond_signal(&pool->jobs_cond);
	
	pthread_mutex_unlock(&pool->jobs_mutex);
}