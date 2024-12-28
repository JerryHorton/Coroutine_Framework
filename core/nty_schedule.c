#include "nty_coroutine.h"


#define FD_KEY(f, e) (((int64_t)(f) << (sizeof(int32_t) * 8)) | e)  // 将一个文件描述符 f 和一个事件 e 合并成一个 64 位整数，作为唯一标识符
#define FD_EVENT(f) ((int32_t)(f))  // 从一个 64 位的键中提取低 32 位，表示事件 e
#define FD_ONLY(f) ((f) >> ((sizeof(int32_t) * 8)))  // 从一个 64 位的键中提取高 32 位，表示文件描述符 f

/* 基于睡眠微秒数比较两个协程的优先级或顺序 */
static inline int nty_coroutine_sleep_cmp(nty_coroutine *co1, nty_coroutine *co2) {
    if (co1->sleep_usecs < co2->sleep_usecs) {
        return -1;
    }
    if (co1->sleep_usecs == co2->sleep_usecs) {
        return 0;
    }
    return 1;
}

/* 基于等待属性比较两个协程的优先级或顺序 */
static inline int nty_coroutine_wait_cmp(nty_coroutine *co1, nty_coroutine *co2) {
#if CANCEL_FD_WAIT_UINT64
    if (co1->fd < co2->fd) {
        return -1;
    } else if (co1->fd == co2->fd) {
        return 0;
    } else {
        return 1;
    }
#else
    if (co1->fd_wait < co2->fd_wait) {
        return -1;
    }
    if (co1->fd_wait == co2->fd_wait) {
        return 0;
    }
#endif
    return 1;
}

// 按 sleep_usecs 排序，用于存储进入睡眠状态的协程，方便根据超时值唤醒协程
RB_GENERATE(_nty_coroutine_rbtree_sleep, _nty_coroutine, sleep_node, nty_coroutine_sleep_cmp);
// 按 fd 或 fd_wait 排序，用于管理等待 I/O 事件的协程，方便在事件到达时快速定位并唤醒
RB_GENERATE(_nty_coroutine_rbtree_wait, _nty_coroutine, wait_node, nty_coroutine_wait_cmp);

/* 将协程加入调度器的睡眠队列中，按给定的睡眠时间（毫秒）延迟唤醒 */
void nty_schedule_sched_sleepdown(nty_coroutine *co, uint64_t msecs) {
    uint64_t usecs = msecs * 1000u;
    nty_coroutine *co_tmp = RB_FIND(_nty_coroutine_rbtree_sleep, &co->sched->sleeping, co);  // 在睡眠队列中查找当前协程
    if (co_tmp != NULL) {  // 存在则将其从红黑树中移除，这避免重复插入或防止冲突
        RB_REMOVE(_nty_coroutine_rbtree_sleep, &co->sched->sleeping, co_tmp);
    }
    co->sleep_usecs = nty_coroutine_diff_usecs(co->sched->birth, nty_coroutine_usec_now()) + usecs;  // 计算唤醒时间
    while (msecs) {
        co_tmp = RB_INSERT(_nty_coroutine_rbtree_sleep, &co->sched->sleeping, co);  // 将当前协程插入睡眠队列（红黑树）
        if (co_tmp) {  // 插入时发现冲突（RB_INSERT 返回非空，说明已经有相同唤醒时间的节点）
            printf("sleep_usecs conflict sleep_usecs %"
                   PRIu64
                   "\n", co->sleep_usecs);
            co->sleep_usecs++;  // （自增 1 微秒）避免冲突，重试插入
            continue;
        }
        co->status |= BIT(NTY_COROUTINE_STATUS_SLEEPING);  // 将协程状态标记为睡眠状态
        break;
    }
    nty_coroutine_yield(co);  // 挂起当前协程
}

/* 从调度器的睡眠队列中移除一个协程并更新其状态 */
void nty_schedule_desched_sleepdown(nty_coroutine *co) {
    if (co->status & BIT(NTY_COROUTINE_STATUS_SLEEPING)) {  // 协程是否处于休眠状态
        RB_REMOVE(_nty_coroutine_rbtree_sleep, &co->sched->sleeping, co);  // 从睡眠队列移除协程
        co->status &= CLEARBIT(NTY_COROUTINE_STATUS_SLEEPING);  // 清除睡眠状态标志
        co->status &= CLEARBIT(NTY_COROUTINE_STATUS_EXPIRED);  // 清除过期标志
        co->status |= BIT(NTY_COROUTINE_STATUS_READY);  // 标记为就绪状态
    }
}

/* 从调度器的等待队列中查找与指定文件描述符 (fd) 相关联的协程 */
nty_coroutine *nty_schedule_search_wait(int fd) {
    nty_coroutine find_it = {0};  // 构造一个虚拟协程用于搜索目标协程
    find_it.fd = fd;
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程的调度器
    nty_coroutine *co = RB_FIND(_nty_coroutine_rbtree_wait, &sched->waiting, &find_it);  // 查找相关协程
    if (co == NULL) {  // 未找到匹配协程
        return NULL;
    }
    co->status = 0;
    return co;
}

/* 将一个协程注册到调度器的等待队列中 */
void nty_schedule_sched_wait(nty_coroutine *co, int fd, unsigned short events, uint64_t timeout) {
    if (co->status & BIT(NTY_COROUTINE_STATUS_WAIT_READ) ||
        co->status & BIT(NTY_COROUTINE_STATUS_WAIT_WRITE)) {  // 已经处于等待读取或写入的状态
        printf("Unexpected event. lt id %"
               PRIu64
               " fd %"
               PRId32
               " already in %"
               PRId32
               " state\n",
               co->id, co->fd, co->status);
        assert(0);  // 强制触发程序崩溃
    }
    if (events & POLLIN) {  // 等待可读事件
        co->status |= BIT(NTY_COROUTINE_STATUS_WAIT_READ);  // 设置协程状态为等待读
    } else if (events & POLLOUT) {  // 等待可写事件
        co->status |= BIT(NTY_COROUTINE_STATUS_WAIT_WRITE);  // 设置协程状态为等待写
    } else {  // 非法事件
        printf("events : %d\n", events);
        assert(0);  // 强制触发程序崩溃
    }
    co->fd = fd;  // 设置等待事件所在的描述符
    co->events = events;  // 设置等待事件
    nty_coroutine *co_tmp = RB_INSERT(_nty_coroutine_rbtree_wait, &co->sched->waiting, co);  // 插入等待队列
    assert(co_tmp == NULL);  // 不为NULL则插入失败（即协程已经在队列中）
    printf("timeout --> %"
           PRIu64
           "\n", timeout);
    if (timeout == NO_TIMEOUT) {
        return;
    }
    nty_schedule_sched_sleepdown(co, timeout);  // 使协程进入睡眠状态，直到超时或事件发生
}

/* 从调度器的等待队列中移除指定文件描述符 fd 对应的协程 */
nty_coroutine *nty_schedule_desched_wait(int fd) {
    nty_coroutine *co = nty_schedule_search_wait(fd);  // 等待队列中查找目标协程
    if (co != NULL) {  // 查找到目标协程
        RB_REMOVE(_nty_coroutine_rbtree_wait, &co->sched->waiting, co);  // 从等待队列中移除协程
        co->status = 0;  // 重置状态
        nty_schedule_desched_sleepdown(co);  // 解除睡眠状态
    }
    return co;
}

/* 从调度器的等待队列中移除指定的协程 */
void nty_schedule_cancel_wait(nty_coroutine *co) {
    RB_REMOVE(_nty_coroutine_rbtree_wait, &co->sched->waiting, co);
}

/* 清理和释放调度器资源 */
void nty_schedule_free(nty_schedule *sched) {
    if (sched->poller_fd > 0) {  // 关闭 poller 文件描述符
        close(sched->poller_fd);
    }
    if (sched->eventfd > 0) {  // 关闭 eventfd 文件描述符
        close(sched->eventfd);
    }
    if (sched->stack != NULL) {  // 释放调度器栈空间
        free(sched->stack);
    }
    free(sched);  // 释放调度器本身的内存
    assert(pthread_setspecific(global_sched_key, NULL) == 0);  // 清除线程本地变量中保存的调度器
}

/* 初始化协程调度器 */
int nty_schedule_create(int stack_size) {
    int sched_stack_size = stack_size ? stack_size : NTY_CO_MAX_STACKSIZE;  // 设置协程调度器的堆栈大小
    nty_schedule *sched = (nty_schedule *) calloc(1, sizeof(nty_schedule));  // 为调度器分配内存
    if (sched == NULL) {  // 分配失败
        printf("Failed to initialize scheduler\n");
        return -1;
    }
    assert(pthread_setspecific(global_sched_key, sched) == 0);  // 线程局部存储（TLS）初始化，将调度器设置为当前线程的全局变量
    sched->poller_fd = nty_epoller_create();  // epoll 初始化
    if (sched->poller_fd == -1) {  // 失败则调用 nty_schedule_free 清理已经分配的资源
        printf("Failed to initialize epoller\n");
        nty_schedule_free(sched);
        return -2;
    }
    sched->eventfd = -1;  // 初始化eventfd
    nty_epoller_ev_register_trigger();  // 注册与事件触发相关的资源
    sched->stack_size = sched_stack_size;  // 设置调度器堆栈大小
    sched->page_size = getpagesize();  // 设置页面大小

#ifdef _USE_UCONTEXT  // 基于 ucontext 的协程切换实现需要栈空间来保存协程的上下文状态
    int ret = posix_memalign(&sched->stack, sched->page_size, sched->stack_size);  // 为调度器分配一块对齐到页面大小的内存作为协程的栈空间
    assert(ret == 0);  // 分配成功，返回值为 0
#else  // 基于汇编手动实现的协程切换不需要专门的栈分配
    sched->stack = NULL;
    bzero(&sched->ctx, sizeof(nty_cpu_ctx));
#endif

    sched->spawned_coroutines = 0;  // 初始化调度器中已生成的协程计数
    sched->default_timeout = 3000000u;  // 设置调度器的默认超时时间
    RB_INIT(&sched->sleeping);  // 初始化睡眠队列
    RB_INIT(&sched->waiting);  // 初始化等待队列
    sched->birth = nty_coroutine_usec_now();  // 记录调度器的创建时间
    TAILQ_INIT(&sched->ready);  // 初始化就绪队列
    TAILQ_INIT(&sched->defer);  // 初始化延迟队列
    LIST_INIT(&sched->busy);  // 初始化执行队列
    return 0;
}

/* 从调度器的睡眠队列中查找并移除一个超时的协程 */
static nty_coroutine *nty_schedule_expired(nty_schedule *sched) {
    uint64_t t_diff_usecs = nty_coroutine_diff_usecs(sched->birth, nty_coroutine_usec_now());  // 计算调度器的当前时间与其创建时间之间的时间差
    nty_coroutine *co = RB_MIN(_nty_coroutine_rbtree_sleep, &sched->sleeping);  // 从调度器的睡眠队列中获取睡眠时间最短的协程
    if (co == NULL) {  // 睡眠队列为空
        return NULL;
    }
    if (co->sleep_usecs <= t_diff_usecs) {  // 协程已经超时
        RB_REMOVE(_nty_coroutine_rbtree_sleep, &co->sched->sleeping, co);  // 将超时的协程从睡眠队列中移除
        return co;  // 返回已超时的协程
    }
    return NULL;  // 如果最小值的协程尚未超时，则返回 NULL，表明没有协程需要被唤醒
}

/* 检查调度器是否完成了所有任务 */
static inline int nty_schedule_isdone(nty_schedule *sched) {
    return (RB_EMPTY(&sched->waiting) &&  // 等待队列为空
            LIST_EMPTY(&sched->busy) &&  // 执行队列为空
            RB_EMPTY(&sched->sleeping) &&  // 睡眠队列为空
            TAILQ_EMPTY(&sched->ready));  // 就绪队列为空
}

/* 计算调度器中最小的超时时间 */
static uint64_t nty_schedule_min_timeout(nty_schedule *sched) {
    uint64_t t_diff_usecs = nty_coroutine_diff_usecs(sched->birth, nty_coroutine_usec_now());  // 计算当前时间与调度器的创建时间之间的时间差
    uint64_t min = sched->default_timeout;  // 初始化为调度器的默认超时时间
    nty_coroutine *co = RB_MIN(_nty_coroutine_rbtree_sleep, &sched->sleeping);  // // 从调度器的睡眠队列中获取睡眠时间最短的协程
    if (co == NULL) {  // 睡眠队列为空
        return min;  // 返回默认的超时时间
    }
    min = co->sleep_usecs;  // 更新为最短休眠协程的唤醒时间
    if (min > t_diff_usecs) {  // 最短的休眠时间大于当前时间差
        return min - t_diff_usecs;  // 距离该协程应该唤醒的剩余时间
    }
    return 0;
}

/* 通过 epoll 等待事件 */
static int nty_schedule_epoll(nty_schedule *sched) {
    sched->num_new_events = 0;  // 将调度器中记录的新事件数量清零
    struct timespec t = {0, 0};
    uint64_t usecs = nty_schedule_min_timeout(sched);  // 获取调度器中最短的超时时间，即当前需要等待的最小时间
    if (usecs && TAILQ_EMPTY(&sched->ready)) {  // 有有效的超时时间，并且调度器的就绪队列为空（没有协程需要立即运行），则计算具体的等待时间
        t.tv_sec = usecs / 1000000u;  // 微秒转化为秒
        if (t.tv_sec != 0) {  // 超时时间大于 1 秒
            t.tv_nsec = (usecs % 1000000u) * 1000u;  // 将微秒中的剩余部分（不足一秒的部分）转化为纳秒
        } else {  // 超时时间小于 1 秒
            t.tv_nsec = usecs * 1000u;  // 直接将微秒转为纳秒
        }
    } else {  // 没有有效的超时时间或者调度器中有可运行的协程，不进行任何等待
        return 0;
    }
    int nready = 0;
    do {
        nready = nty_epoller_wait(t);  // 等待事件
        if (nready == -1) {
            if (errno == EINTR) {  // 调用被信号中断
                continue;
            } else {  // 其他错误
                assert(0);
            }
        }
        break;
    } while (1);
    sched->nevents = 0;  // 调度器的事件计数重置为 0
    sched->num_new_events = nready;  // 记录新事件的数量
    return 0;
}

/* 运行协程调度器，开始执行调度任务 */
void nty_schedule_run(void) {
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程绑定的调度器
    if (sched == NULL) {  // 调度器为空
        return;
    }
    while (!nty_schedule_isdone(sched)) {  // 调度器是未完成所有任务，说明有协程需要调度
        // 1. 处理超时协程
        nty_coroutine *expired = NULL;
        while ((expired = nty_schedule_expired(sched)) != NULL) {  // 从调度器的睡眠队列中获取已超时的协程
            nty_coroutine_resume(expired);  // 恢复运行这些协程
        }
        // 2. 运行就绪队列中的协程
        nty_coroutine *last_co_ready = TAILQ_LAST(&sched->ready,
                                                  _nty_coroutine_queue);  // 获取就绪队列中的最后一个协程，标记队列的末尾，防止循环中新增协程导致无限循环
        while (!TAILQ_EMPTY(&sched->ready)) {  // 就绪队列不为空
            nty_coroutine *co = TAILQ_FIRST(&sched->ready);  // 获取就绪队列的第一个协程
            TAILQ_REMOVE(&co->sched->ready, co, ready_next);  // 并从队列中移除
            if (co->status & BIT(NTY_COROUTINE_STATUS_FDEOF)) {  // 如果协程处于 EOF（没有更多数据可以读取）状态
                nty_coroutine_free(co);  // 释放并跳过
                continue;
            }
            nty_coroutine_resume(co);  // 恢复协程的运行
            if (co == last_co_ready) {  // 当前运行的协程是之前标记的最后一个协程，则退出循环，防止无限循环
                break;
            }
        }
        // 3. 处理等待队列中的协程
        nty_schedule_epoll(sched);  // 从 epoll 等待队列中获取就绪的事件
        while (sched->num_new_events) {
            int idx = --sched->num_new_events;
            struct epoll_event *ev = sched->eventlist + idx;  // 取出一个事件
            int fd = ev->data.fd;  // 获取关联的文件描述符
            int is_eof = ev->events & EPOLLHUP;
            if (is_eof) {  // 事件包含 EPOLLHUP（挂起或连接断开）
                errno = ECONNRESET;  // 连接被重置
            }
            nty_coroutine *co = nty_schedule_search_wait(fd);  // 找到等待该文件描述符的协程
            if (co != NULL) {
                if (is_eof) {
                    co->status |= BIT(NTY_COROUTINE_STATUS_FDEOF);
                }
                nty_coroutine_resume(co);  // 唤醒并恢复执行
            }
            is_eof = 0;  // 重置为 0
        }
    }
    nty_schedule_free(sched);  // 调度器是完成所有任务，释放调度器资源
}

