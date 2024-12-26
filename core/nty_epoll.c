#include <sys/eventfd.h>
#include "nty_coroutine.h"

/* 创建一个 epoll */
int nty_epoller_create(void) {
    return epoll_create(1024);
}

/* 等待 I/O 事件的发生 */
int nty_epoller_wait(struct timespec t) {
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程关联的调度器
    int timeout_ms = t.tv_sec * 1000u + t.tv_nsec / 1000000u;  // 将秒和纳秒转换为毫秒
    return epoll_wait(sched->poller_fd, sched->eventlist, NTY_CO_MAX_EVENTS, timeout_ms);
}

/* 注册一个事件触发机制（基于 eventfd）到调度器的 epoll 中 */
int nty_epoller_ev_register_trigger(void) {
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程关联的调度器
    if (!sched->eventfd) {  // 尚未创建 eventfd
        sched->eventfd = eventfd(0, EFD_NONBLOCK);  // 创建一个初始值为 0 的 eventfd，并设置为非阻塞模式
        assert(sched->eventfd != -1);
    }
    struct epoll_event ev;
    ev.events = EPOLLIN;  // 表示监听 eventfd 上的可读事件（即等待 eventfd 被写入数据）
    ev.data.fd = sched->eventfd;  // 将 eventfd 的文件描述符关联到 ev 结构中
    int ret = epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, sched->eventfd, &ev);  // 将 eventfd 注册到 epoll 中
    assert(ret != -1);
    return 0;
}

