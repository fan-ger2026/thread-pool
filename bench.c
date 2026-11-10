#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>

#include "thread_pool.h"

/* ==================== 可调参数 ==================== */
#define DEFAULT_N          1000
#define DEFAULT_WORK       1000000
#define BATCH_SIZE         16      /* thread-per-task 基线的批量大小 */
#define REPEAT             3       /* 每组配置重复次数，取中位数 */

/* ==================== 全局状态 ==================== */
static atomic_int completed = 0;
static long g_work_iters = 0;

/* 纯 CPU 计算：用 sink 防止优化，循环本身在寄存器里跑 */
static void do_work(void) {
    long sum = 0;
    for (long i = 0; i < g_work_iters; i++)
        sum += i;
    volatile long sink = sum;    /* 防止整个循环被优化掉 */
    (void)sink;
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* 等待 completed 达到 n：100us 轮询，误差 < 0.4% */
static void wait_all(int n) {
    while (atomic_load(&completed) < n)
        usleep(100);
}

/* 中位数：排序后取中间，3 次取第 2 个 */
static double median(double *a, int n) {
    for (int i = 0; i < n - 1; i++)
        for (int j = i + 1; j < n; j++)
            if (a[i] > a[j]) {
                double t = a[i]; a[i] = a[j]; a[j] = t;
            }
    return a[n / 2];
}

/* ==================== 线程池任务 ==================== */
static void pool_task(struct NJOB *job) {
    (void)job;
    do_work();
    atomic_fetch_add(&completed, 1);
}

/* ==================== 每任务建线程的线程入口 ==================== */
static void *thread_baseline(void *arg) {
    (void)arg;
    do_work();
    return NULL;
}

/* ==================== 基线 1：串行 ==================== */
static double bench_serial(int n) {
    double times[REPEAT];
    for (int r = 0; r < REPEAT; r++) {
        double t0 = now_sec();
        for (int i = 0; i < n; i++) do_work();
        times[r] = now_sec() - t0;
    }
    return median(times, REPEAT);
}

/* ==================== 基线 2：每任务建线程（批量创建 + 批量 join） ==================== */
static double bench_thread_per_task(int n) {
    double times[REPEAT];
    pthread_t tids[BATCH_SIZE];

    for (int r = 0; r < REPEAT; r++) {
        double t0 = now_sec();
        for (int i = 0; i < n; i += BATCH_SIZE) {
            int cnt = (n - i < BATCH_SIZE) ? (n - i) : BATCH_SIZE;
            int created = 0;

            for (int j = 0; j < cnt; j++) {
                int ret = pthread_create(&tids[j], NULL, thread_baseline, NULL);
                if (ret != 0) {
                    fprintf(stderr, "pthread_create failed: %d\n", ret);
                    break;
                }
                created++;
            }
            for (int j = 0; j < created; j++)
                pthread_join(tids[j], NULL);
        }
        times[r] = now_sec() - t0;
    }
    return median(times, REPEAT);
}

/* ==================== 实验组：线程池 ==================== */
static double bench_pool(int n, int workers, struct NJOB *jobs,
                          nThreadPool *pool_out) {
    double times[REPEAT];

    for (int r = 0; r < REPEAT; r++) {
        /* 每次重新创建池，避免状态残留 */
        nThreadPool pool;
        if (nThreadPoolCreate(&pool, workers) != 0) {
            fprintf(stderr, "create failed for %d workers\n", workers);
            return -1;
        }

        /* 重置任务链表指针和计数 */
        for (int i = 0; i < n; i++)
            jobs[i].prev = jobs[i].next = NULL;
        atomic_store(&completed, 0);

        double t0 = now_sec();
        for (int i = 0; i < n; i++)
            nThreadPoolPush(&pool, &jobs[i]);
        wait_all(n);
        times[r] = now_sec() - t0;

        nThreadPoolDestroy(&pool);
    }

    *pool_out = (nThreadPool){0};   /* 占位，实际池已销毁 */
    return median(times, REPEAT);
}

/* ==================== main ==================== */
int main(int argc, char **argv) {
    int n      = (argc > 1) ? atoi(argv[1]) : DEFAULT_N;
    long work  = (argc > 2) ? atol(argv[2]) : DEFAULT_WORK;

    g_work_iters = work;

    printf("=== Benchmark: N=%d, work=%ld iters/task, repeat=%d, batch=%d ===\n",
           n, work, REPEAT, BATCH_SIZE);
    fflush(stdout);

    /* ---------- 串行基线 ---------- */
    double serial = bench_serial(n);
    printf("[serial]           time=%.4f s  (baseline)\n", serial);
    fflush(stdout);

    /* ---------- 每任务建线程（批量） ---------- */
    double tpt = bench_thread_per_task(n);
    printf("[thread-per-task]  time=%.4f s  speedup=%.2fx\n", tpt, serial / tpt);
    fflush(stdout);

    /* ---------- 线程池 ---------- */
    struct NJOB *jobs = calloc(n, sizeof(struct NJOB));
    if (!jobs) { perror("calloc"); return 1; }
    for (int i = 0; i < n; i++)
        jobs[i].func = pool_task;

    int wc[] = {1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64};
    int n_wc = sizeof(wc) / sizeof(wc[0]);

    for (int k = 0; k < n_wc; k++) {
        int nw = wc[k];
        nThreadPool dummy;
        double pt = bench_pool(n, nw, jobs, &dummy);
        if (pt < 0) continue;

        printf("[pool workers=%2d]  time=%.4f s  speedup=%.2fx\n",
               nw, pt, serial / pt);
        fflush(stdout);
    }

    free(jobs);
    return 0;
}

/*
=== Benchmark: N=1000, work=1000000 iters/task, repeat=3, batch=16 ===
[serial]           time=0.2252 s  (baseline)
[thread-per-task]  time=0.0580 s  speedup=3.88x
[pool workers= 1]  time=0.2316 s  speedup=0.97x
[pool workers= 2]  time=0.1161 s  speedup=1.94x
[pool workers= 4]  time=0.0584 s  speedup=3.85x
[pool workers= 6]  time=0.0391 s  speedup=5.77x
[pool workers= 8]  time=0.0307 s  speedup=7.34x
[pool workers=12]  time=0.0222 s  speedup=10.13x
[pool workers=16]  time=0.0168 s  speedup=13.43x
[pool workers=24]  time=0.0165 s  speedup=13.64x
[pool workers=32]  time=0.0157 s  speedup=14.34x
[pool workers=48]  time=0.0158 s  speedup=14.28x
[pool workers=64]  time=0.0177 s  speedup=12.70x
*/