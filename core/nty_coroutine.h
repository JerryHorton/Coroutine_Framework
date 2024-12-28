#ifndef COROUTINE_FRAMWORK_NTY_COROUTINE_H
#define COROUTINE_FRAMWORK_NTY_COROUTINE_H

#define _GNU_SOURCE

#include <dlfcn.h>

#define _USE_UCONTEXT

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <inttypes.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <netinet/tcp.h>

#ifdef _USE_UCONTEXT

#include <ucontext.h>

#endif

#include <sys/epoll.h>
#include <sys/poll.h>

#include <errno.h>

#include "nty_queue.h"
#include "nty_tree.h"

#define NTY_CO_MAX_EVENTS        (1024*1024)  // 最大事件数, 用于表示协程事件调度的容量
#define NTY_CO_MAX_STACKSIZE    (128*1024)  // 最大栈大小,协程的栈大小 {http: 16*1024, tcp: 4*1024}

#define BIT(x)                    (1 << (x))  // 位操作宏，用于设置位(相或)
#define CLEARBIT(x)            ~(1 << (x))  // 位操作宏，用于清除位(相与)

#define CANCEL_FD_WAIT_UINT64    1  // 用于取消文件描述符的等待标志
#define NO_TIMEOUT          1  // 无效超时时间，表示非阻塞行为

typedef void (*proc_coroutine)(void *);  // 协程回调函数类型

// 枚举类型定义了协程的状态、事件和调度的方式
typedef enum {
    // 协程正在等待读事件
    NTY_COROUTINE_STATUS_WAIT_READ,
    // 协程正在等待写事件
    NTY_COROUTINE_STATUS_WAIT_WRITE,
    // 协程刚刚被创建，但还未被调度执行
    NTY_COROUTINE_STATUS_NEW,
    // 协程已经准备好执行，等待调度器调度
    NTY_COROUTINE_STATUS_READY,
    // 协程已经退出执行
    NTY_COROUTINE_STATUS_EXITED,
    // 协程当前正在执行，忙于处理任务
    NTY_COROUTINE_STATUS_BUSY,
    // 协程正在休眠中
    NTY_COROUTINE_STATUS_SLEEPING,
    // 协程的超时时间已到，通常是协程在等待某些事件时设置了超时，超时后进入该状态
    NTY_COROUTINE_STATUS_EXPIRED,
    // 协程所等待的文件描述符已到达文件结束符（EOF）当从文件或流中读取数据时，如果没有更多数据可以读取，协程会进入此状态
    NTY_COROUTINE_STATUS_FDEOF,
    // 协程被设置为“分离”状态，这意味着协程不会再与其他协程进行同步
    NTY_COROUTINE_STATUS_DETACH,
    // 协程已经被取消，通常是由调度器或其他协程调用取消操作
    NTY_COROUTINE_STATUS_CANCELLED,
    // 协程处于计算任务待执行的状态
    NTY_COROUTINE_STATUS_PENDING_RUNCOMPUTE,
    // 协程正在进行计算任务
    NTY_COROUTINE_STATUS_RUNCOMPUTE,
    // 协程正在等待IO读事件
    NTY_COROUTINE_STATUS_WAIT_IO_READ,
    // 协程正在等待IO写事件
    NTY_COROUTINE_STATUS_WAIT_IO_WRITE,
    // 协程正在等待多个事件，通常是多个IO事件或者多个条件的组合
    NTY_COROUTINE_STATUS_WAIT_MULTI
} nty_coroutine_status;

// 计算状态枚举：用于标识协程是否处于计算状态
typedef enum {
    NTY_COROUTINE_COMPUTE_BUSY,  // 正在忙碌
    NTY_COROUTINE_COMPUTE_FREE  // 空闲
} nty_coroutine_compute_status;

// 事件枚举：表示协程监听的事件类型
typedef enum {
    NTY_COROUTINE_EV_READ,  // 读取事件
    NTY_COROUTINE_EV_WRITE  // 写入事件
} nty_coroutine_event;


LIST_HEAD(_nty_coroutine_link, _nty_coroutine);  // 链表（LIST_HEAD）：协程可以通过这些链表来管理其状态变化
TAILQ_HEAD(_nty_coroutine_queue, _nty_coroutine);  // 队列（TAILQ_HEAD）：用于存储协程的准备队列、延迟队列等

RB_HEAD(_nty_coroutine_rbtree_sleep, _nty_coroutine);  // 红黑树（RB_HEAD）：用于存储按时间排序的休眠协程
RB_HEAD(_nty_coroutine_rbtree_wait, _nty_coroutine);  // 红黑树（RB_HEAD）：用于存储按时间排序的等待协程



typedef struct _nty_coroutine_link nty_coroutine_link;
typedef struct _nty_coroutine_queue nty_coroutine_queue;

typedef struct _nty_coroutine_rbtree_sleep nty_coroutine_rbtree_sleep;
typedef struct _nty_coroutine_rbtree_wait nty_coroutine_rbtree_wait;


#ifndef _USE_UCONTEXT
// 保存协程的CPU上下文（寄存器状态）
typedef struct _nty_cpu_ctx {
    void *esp;  // 栈指针
    void *ebp;  // 帧指针
    void *eip;  // 指令指针
    void *ebx;  // 通用寄存器
    void *r12;  // 通用寄存器 r12
    void *r13;  // 通用寄存器 r13
    void *r14;  // 通用寄存器 r14
    void *r15;  // 通用寄存器 r15
} nty_cpu_ctx;
#endif

// 协程调度器
typedef struct _nty_schedule {
    uint64_t birth;  // 协程的创建时间
#ifdef _USE_UCONTEXT
    ucontext_t ctx;  // 使用 ucontext_t 来保存调度器的上下文
#else
    nty_cpu_ctx ctx;  // 使用 nty_cpu_ctx 来手动管理调度器的寄存器状态
#endif
    void *stack;  // 堆栈指针即协程的栈基址，指向协程使用的内存堆栈，
    size_t stack_size;  // 协程堆栈的总大小
    int spawned_coroutines;  // 已经创建并由调度器管理的协程数量
    uint64_t default_timeout;  // 默认的超时时间，用于协程调度中的超时控制
    struct _nty_coroutine *curr_thread;  // 当前正在执行的协程
    int page_size;  // 操作系统页面的大小

    int poller_fd;  // 用于事件轮询的文件描述符
    int eventfd;  // 事件文件描述符
    struct epoll_event eventlist[NTY_CO_MAX_EVENTS];  // 保存从 epoll 获取的事件列表
    int nevents;  // 当前事件的数量

    int num_new_events;  // 新事件的数量

    pthread_mutex_t defer_mutex;  // 用于保护协程调度器中的延迟操作的互斥锁
    pthread_mutex_t resource_mutex;  // 用于保护协程调度器中的对资源操作的互斥锁

    nty_coroutine_queue ready;  // 准备好执行的协程队列
    nty_coroutine_queue defer;  // 被推迟执行的协程队列

    nty_coroutine_link busy;  // 当前正在执行的协程链表

    nty_coroutine_rbtree_sleep sleeping;  // 处于休眠状态的协程的红黑树
    nty_coroutine_rbtree_wait waiting;  // 处于等待状态的协程的红黑树
} nty_schedule;

// 协程
typedef struct _nty_coroutine {

#ifdef _USE_UCONTEXT
    ucontext_t ctx;  // 使用 ucontext_t 来保存协程的上下文
#else
    nty_cpu_ctx ctx;  // 使用 nty_cpu_ctx 来保存协程的 CPU 寄存器状态
#endif
    proc_coroutine func;  // 协程执行的函数指针,指向回调函数，当协程被调度时，这个函数会被执行
    void *arg;  // 传递给协程执行函数的参数
    void *data;  // 协程的额外数据字段
    size_t stack_size;  // 保存的栈内容的大小
    size_t last_stack_size;  // 协程的上一栈大小,用于记录栈的大小变化

    uint32_t status;  // 协程的当前状态
    nty_schedule *sched;  // 指向协程调度器（nty_schedule）的指针,每个协程都与一个调度器相关联

    uint64_t birth;  // 协程的创建时间
    uint64_t id;  // 协程的唯一标识符
#if CANCEL_FD_WAIT_UINT64
    // 等待一个文件描述符（如套接字）上的某个事件
    int fd;  // 文件描述符
    unsigned short events;  // 事件（例如，POLL_READ 或 POLL_WRITE）
#else
    int64_t fd_wait;  // 协程等待的文件描述符
#endif
    char funcname[64];  // 协程执行的函数的名称
    struct _nty_coroutine *co_join;  // 指向另一个协程的指针,用于表示当前协程等待其他协程结束

    void **co_exit_ptr;  // 指向退出值的指针,用于保存协程退出时返回的数据
    void *stack;  // 保存的栈内容，在堆内存
    void *ebp;  // 协程的帧指针
    uint32_t ops;  // 协程的操作标志
    uint64_t sleep_usecs;  // 协程的睡眠时间

    RB_ENTRY(_nty_coroutine) sleep_node;  // 红黑树节点，用于将协程按睡眠时间排序
    RB_ENTRY(_nty_coroutine) wait_node;  // 红黑树节点，用于将协程按等待事件排序

    LIST_ENTRY(_nty_coroutine) busy_next;  // 链表节点，用于将进入 BUSY 状态的协程加入到正在忙碌协程队列中

    TAILQ_ENTRY(_nty_coroutine) ready_next;  // 队列节点，用于将进入 READY 状态的协程添加到准备好执行协程队列中
    TAILQ_ENTRY(_nty_coroutine) defer_next;  // 队列节点，用于将因某些原因被延迟执行的协程添加到延迟协程队列中
    TAILQ_ENTRY(_nty_coroutine) cond_next;  // 队列节点，用于将等待某个条件满足时被挂起的协程加入到条件变量协程队列中
    TAILQ_ENTRY(_nty_coroutine) io_next;  // 队列节点，用于将进行 I/O 操作的协程加入到 I/O 等待协程队列中
    TAILQ_ENTRY(_nty_coroutine) compute_next;  // 队列节点，用于将计算密集型任务的协程加入到计算协程队列中

    // 协程的 I/O 操作相关信息
    struct {
        void *buf;  // 缓冲区，保存数据
        size_t nbytes;  // 要读取或写入的字节数
        int fd;  // 文件描述符，表示 I/O 操作的对象
        int ret;  // I/O 操作的返回值
        int err;  // I/O 操作的错误代码
    } io;

    int is_freed;  // 标志是否已释放，防止双重释

    struct _nty_coroutine_compute_sched *compute_sched;  // 指向计算调度器的指针
    int ready_fds;  // 协程准备好操作的文件描述符数量
    struct pollfd *pfds;  // 指向 pollfd 结构的指针,用于表示协程需要进行的 I/O 操作及其相关的文件描述符
    nfds_t nfds;  // pollfd 数组的大小,确定了需要处理的文件描述符数量
} nty_coroutine;

// 协程计算调度器
typedef struct _nty_coroutine_compute_sched {
#ifdef _USE_UCONTEXT
    ucontext_t ctx;  // 使用 ucontext_t 来保存调度器的上下文（如寄存器状态）
#else
    nty_cpu_ctx ctx;  // 使用 nty_cpu_ctx 来保存调度器的 CPU 寄存器状态
#endif
    nty_coroutine_queue coroutines;  // 计算协程队列

    nty_coroutine *curr_coroutine;  // 当前正在执行的协程指针

    pthread_mutex_t run_mutex;  // 用于控制计算协程调度器的互斥锁
    pthread_cond_t run_cond;  // 用于同步协程调度的条件变量,用于控制计算调度器的等待与唤醒操作

    pthread_mutex_t co_mutex;  // 协程互斥锁。用于保护对协程的访问，确保协程的状态和操作不会被并发访问所破坏
    LIST_ENTRY(_nty_coroutine_compute_sched) compute_next;  // 链表节点，用于将计算调度器连接到其他计算调度器队列中,将多个计算调度器按链表形式串联起来，从而形成一个调度器队列

    nty_coroutine_compute_status compute_status;  // 计算协程调度器的状态
} nty_coroutine_compute_sched;

extern pthread_key_t global_sched_key;  // 全局的线程局部存储（TLS）键 global_sched_key，用于存储与每个线程相关的调度器信息 (nty_schedule 结构体)

/* 获取当前线程关联的调度器（nty_schedule） */
static inline nty_schedule *nty_coroutine_get_sched(void) {
    return pthread_getspecific(global_sched_key);
}

/* 计算两个时间点（以微秒为单位）的差值 */
static inline uint64_t nty_coroutine_diff_usecs(uint64_t t1, uint64_t t2) {
    return t2 - t1;
}

/* 获取当前的时间戳（以微秒为单位) */
static inline uint64_t nty_coroutine_usec_now(void) {
    struct timeval t1 = {0, 0};
    gettimeofday(&t1, NULL);

    return t1.tv_sec * 1000000 + t1.tv_usec;
}

/***************************************
 * 协程调度器 (nty_schedule) 的核心 API
 **************************************/

/**
 * 创建一个 epoll 实例，用于高效的事件监听
 * 用于处理 I/O 事件的通知机制
 *
 * @return 成功返回 epoll 文件描述符，失败返回 -1
 */
int nty_epoller_create(void);

/**
 * 取消指定协程 (co) 的当前事件
 * 当一个协程不再需要等待某个事件时，可以调用此函数取消事件的调度
 *
 * @param co 需要取消事件的协程
 */
void nty_schedule_cancel_event(nty_coroutine *co);

/**
 * 将指定协程 (co) 调度为等待事件
 *
 * @param co 需要等待的协程
 * @param fd 要监听的文件描述符
 * @param e 事件类型（如读事件或写事件，NTY_COROUTINE_EV_READ 或 NTY_COROUTINE_EV_WRITE）
 * @param timeout 超时时间，如果超时没有发生事件，协程将被唤醒
 */
void nty_schedule_sched_event(nty_coroutine *co, int fd, nty_coroutine_event e, uint64_t timeout);

/**
 * 从调度器的睡眠队列中移除一个协程，取消其休眠状态
 *
 * @param co 需要移出的协程
 */
void nty_schedule_desched_sleepdown(nty_coroutine *co);

/**
 * 将指定协程 (co) 调度为休眠状态，直到超时或被唤醒
 *
 * @param co 需要休眠的协程
 * @param msecs 休眠时间（以毫秒为单位）
 */
void nty_schedule_sched_sleepdown(nty_coroutine *co, uint64_t msecs);

/**
 * 将定的文件描述符 (fd) 上某个事件的协程从等待队列中移除
 *
 * @param fd 要监听的文件描述符
 * @return 移出的协程
 */
nty_coroutine *nty_schedule_desched_wait(int fd);

/**
 * 将一个协程（co）注册到调度器的等待队列中，等待某个
 * 文件描述符（fd）上的特定事件（events）（如可读、可写），并可能会涉及到超时机制
 *
 * @param co 需要等待的协程
 * @param fd 要监听的文件描述符
 * @param events 需要监听的事件（例如，读事件 POLLIN 或写事件 POLLOUT）
 * @param timeout 超时时间（以毫秒为单位）
 */
void nty_schedule_sched_wait(nty_coroutine *co, int fd, unsigned short events, uint64_t timeout);

/**
 * 运行协程调度器，开始执行调度任务
 */
void nty_schedule_run(void);

/**
 * 注册一个触发器事件，用于唤醒 epoll 事件循环
 *
 * @return 返回一个整数，0 表示成功，负值表示失败
 */
int nty_epoller_ev_register_trigger(void);

/**
 * 等待指定的时间，用于在 epoll 事件循环中等待特定的事件发生，阻塞直到有
 * 事件或者等待时间到达。如果没有事件，等待会持续到 timespec 中指定的时间
 *
 * @param t 等待时间
 * @return 事件的数量，或者在超时的情况下返回 0
 */
int nty_epoller_wait(struct timespec t);

/**
 * 恢复一个挂起的协程，开始执行该协程
 *
 * @param co 要恢复的协程
 * @return 返回 0 表示成功，负值表示协程已退出
 */
int nty_coroutine_resume(nty_coroutine *co);

/**
 * 将当前协程挂起，让出执行权
 *
 * @param co 要挂起的协程
 */
void nty_coroutine_yield(nty_coroutine *co);

/**
 * 创建一个新的协程
 *
 * @param new_co 指向新的协程对象的指针
 * @param func 协程执行的回调函数
 * @param arg 传递给协程执行函数的参数
 * @return 返回 0 表示成功，负值表示调失败
 */
int nty_coroutine_create(nty_coroutine **new_co, proc_coroutine func, void *arg);

/**
 * 销毁协程并释放资源
 *
 * @param co 要销毁的协程
 */
void nty_coroutine_free(nty_coroutine *co);

/**
 * 使当前协程进入休眠状态，暂停执行指定的时间
 *
 * @param msecs 协程需要休眠的时间（以毫秒为单位）
 */
void nty_coroutine_sleep(uint64_t msecs);


/***************************************
 * 网络编程相关的封装函数,针对协程环境进行
 * 封装，以便更好地支持协程并发执行
 **************************************/

/**
 * 创建一个套接字
 *
 * @param domain 套接字域（如 AF_INET 表示 IPv4 地址族）
 * @param type 套接字类型（如 SOCK_STREAM 表示流套接字）
 * @param protocol 协议类型（如 IPPROTO_TCP 表示 TCP 协议）
 * @return 成功时返回一个新的套接字文件描述符；失败时返回 -1
 */
int nty_socket(int domain, int type, int protocol);

/**
 * 接受一个连接请求
 *
 * @param fd 监听套接字的文件描述符
 * @param addr 用于接收客户端地址的结构
 * @param len 地址长度的指针
 * @return 成功时返回新的套接字文件描述符；失败时返回 -1
 */
int nty_accept(int fd, struct sockaddr *addr, socklen_t *len);

/**
 * 从指定的套接字读取数据
 *
 * @param fd 套接字文件描述符
 * @param buf 接收数据的缓冲区
 * @param len 要接收的最大字节数
 * @param flags 标志位，通常为 0
 * @return 成功时返回接收到的字节数；失败时返回 -1 | 0
 */
ssize_t nty_recv(int fd, void *buf, size_t len, int flags);

/**
 * 向指定的套接字发送数据
 *
 * @param fd 套接字文件描述符
 * @param buf 包含要发送数据的缓冲区
 * @param len 要发送的字节数
 * @param flags 标志位，通常为 0
 * @return 成功时返回实际发送的字节数；失败时返回 -1 | 0
 */
ssize_t nty_send(int fd, const void *buf, size_t len, int flags);

/**
 * 向指定的文件描述符发送数据
 *
 * @param fd 文件描述符
 * @param buf 包含要发送数据的缓冲区
 * @param count 要发送的字节数
 * @return 成功时返回实际发送的字节数；失败时返回 -1 | 0
 */
ssize_t nty_write(int fd, const void *buf, size_t count);

/**
 * 从指定的文件描述符读取数据
 *
 * @param fd 文件描述符
 * @param buf 接收数据的缓冲区
 * @param count 要接收的最大字节数
 * @return 成功时返回接收到的字节数；失败时返回 -1 | 0
 */
ssize_t nty_read(int fd, void *buf, size_t count);

/**
 * 关闭文件描述符
 *
 * @param fd 要关闭的文件描述符
 * @return 成功时返回 0；失败时返回 -1
 */
int nty_close(int fd);

/**
 * 轮询一个或多个文件描述符，检查是否有 I/O 操作可以执行
 *
 * @param fds 指向 pollfd 结构数组的指针
 * @param nfds 数组的大小
 * @param timeout 超时时间
 * @return 成功时返回活动的文件描述符数量；失败时返回 -1
 */
int nty_poll(struct pollfd *fds, nfds_t nfds, int timeout);

/**
 * 发起连接请求（通常用于客户端）
 *
 * @param fd 套接字文件描述符
 * @param addr 目标地址信息
 * @param len 目标地址的长度
 * @return
 */
int nty_connect(int fd, const struct sockaddr *addr, socklen_t len);

/**
 * 向指定的地址发送数据（通常用于 UDP）
 *
 * @param fd 套接字文件描述符
 * @param buf 包含要发送的数据的缓冲区
 * @param len 要发送的字节数
 * @param flags 标志位，通常为 0
 * @param dest_addr 目标地址
 * @param addrlen 目标地址的长度
 * @return
 */
ssize_t nty_sendto(int fd, const void *buf, size_t len, int flags,
                   const struct sockaddr *dest_addr, socklen_t addrlen);

/**
 * 从指定的地址接收数据（通常用于 UDP）
 *
 * @param fd 套接字文件描述符
 * @param buf 接收数据的缓冲区
 * @param len 要接收的最大字节数
 * @param flags 标志位，通常为 0
 * @param src_addr 接收数据的源地址
 * @param addrlen 源地址的长度
 * @return
 */
ssize_t nty_recvfrom(int fd, void *buf, size_t len, int flags,
                     struct sockaddr *src_addr, socklen_t *addrlen);


#define COROUTINE_HOOK

#ifdef  COROUTINE_HOOK

/***************************************
 * 定义了一系列函数指针类型，并声明了与这些
 * 类型对应的外部函数指针变量。通过这些函数
 * 指针，可以在运行时灵活地替换这些系统调用
 * 的实现，用于钩子。
 **************************************/

/* socket 函数的替代 */
typedef int (*socket_t)(int domain, int type, int protocol);

extern socket_t socket_f;

/* connect 函数的替代 */
typedef int(*connect_t)(int, const struct sockaddr *, socklen_t);

extern connect_t connect_f;

/* read 函数的替代 */
typedef ssize_t(*read_t)(int, void *, size_t);

extern read_t read_f;

/* recv 函数的替代 */
typedef ssize_t(*recv_t)(int sockfd, void *buf, size_t len, int flags);

extern recv_t recv_f;

/* recvfrom_f 函数的替代 */
typedef ssize_t(*recvfrom_t)(int sockfd, void *buf, size_t len, int flags,
                             struct sockaddr *src_addr, socklen_t *addrlen);

extern recvfrom_t recvfrom_f;

/* write 函数的替代 */
typedef ssize_t(*write_t)(int, const void *, size_t);

extern write_t write_f;

/* send 函数的替代 */
typedef ssize_t(*send_t)(int sockfd, const void *buf, size_t len, int flags);

extern send_t send_f;

/* sendto 函数的替代 */
typedef ssize_t(*sendto_t)(int sockfd, const void *buf, size_t len, int flags,
                           const struct sockaddr *dest_addr, socklen_t addrlen);

extern sendto_t sendto_f;

/* accept 函数的替代 */
typedef int(*accept_t)(int sockfd, struct sockaddr *addr, socklen_t *addrlen);

extern accept_t accept_f;

/* close 函数的替代 */
typedef int(*close_t)(int);

extern close_t close_f;

/* 初始化钩子 */
int init_hook(void);

#endif


#endif
