#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <pthread.h>

#include "thread_pool.h"

#define NUM_TASKS 1000

/* 任务完成计数，用原子变量，避免数据竞争 */
static atomic_int completed = 0;

/* 任务函数：签名必须和 NJOB.func 一致 */
void my_task(struct NJOB *job) {
    int *p = (int *)job->user_data;
    printf("task %2d in thread %lu\n",
           *p, (unsigned long)pthread_self());
    atomic_fetch_add(&completed, 1);
}

int main(void) {
    nThreadPool pool;

    /* 1. 创建线程池：4 个 worker */
    int ret = nThreadPoolCreate(&pool, 4);
    if (ret != 0) {
        fprintf(stderr, "create pool failed: %d\n", ret);
        return 1;
    }
    printf("pool created with 4 workers\n");

    /* 2. 构造任务并提交 */
    struct NJOB jobs[NUM_TASKS];
    int args[NUM_TASKS];

    for (int i = 0; i < NUM_TASKS; i++) {
        args[i] = i;
        memset(&jobs[i], 0, sizeof(jobs[i]));  /* 清空 prev/next */
        jobs[i].func = my_task;
        jobs[i].user_data = &args[i];
        nThreadPoolPush(&pool, &jobs[i]);
    }
    printf("%d tasks pushed\n", NUM_TASKS);

    /* 3. 等所有任务完成：轮询 + 超时，不靠固定 sleep */
    int timeout_ms = 5000;
    int waited = 0;
    while (atomic_load(&completed) < NUM_TASKS && waited < timeout_ms) {
        usleep(10 * 1000);   /* 10ms */
        waited += 10;
    }

    /* 4. 断言 */
    int done = atomic_load(&completed);
    if (done != NUM_TASKS) {
        fprintf(stderr, "TEST FAILED: expected %d, got %d (waited %d ms)\n",
                NUM_TASKS, done, waited);
        nThreadPoolDestroy(&pool);
        return 1;
    }
    printf("TEST PASSED: all %d tasks completed in ~%d ms\n",
           NUM_TASKS, waited);

    /* 5. 销毁：所有任务已完成，jobs 数组可以安全退出作用域 */
    nThreadPoolDestroy(&pool);
    printf("pool destroyed\n");
    return 0;
}
/*
简单测试线程池代码是否可运行
*/