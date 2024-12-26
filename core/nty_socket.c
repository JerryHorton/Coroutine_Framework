#include "nty_coroutine.h"

#ifdef  COROUTINE_HOOK

// 保存相应系统调用的地址

socket_t socket_f = NULL;
read_t read_f = NULL;
recv_t recv_f = NULL;
recvfrom_t recvfrom_f = NULL;
write_t write_f = NULL;
send_t send_f = NULL;
sendto_t sendto_f = NULL;
accept_t accept_f = NULL;
connect_t connect_f = NULL;
close_t close_f = NULL;

/* 钩子函数，动态加载系统中的函数地址 */
int init_hook(void) {
    socket_f = (socket_t) dlsym(RTLD_NEXT, "socket");
    read_f = (read_t) dlsym(RTLD_NEXT, "read");
    recv_f = (recv_t) dlsym(RTLD_NEXT, "recv");
    recvfrom_f = (recvfrom_t) dlsym(RTLD_NEXT, "recvfrom");
    write_f = (write_t) dlsym(RTLD_NEXT, "write");
    send_f = (send_t) dlsym(RTLD_NEXT, "send");
    sendto_f = (sendto_t) dlsym(RTLD_NEXT, "sendto");
    accept_f = (accept_t) dlsym(RTLD_NEXT, "accept");
    close_f = (close_t) dlsym(RTLD_NEXT, "close");
    connect_f = (connect_t) dlsym(RTLD_NEXT, "connect");
    return 0;
}

/* 将 poll 事件标志转换为 epoll 事件标志 */
static uint32_t nty_pollevent_2epoll(short events) {
    uint32_t e = 0;
    if (events & POLLIN) {  // 文件描述符可读 POLLIN -> EPOLLIN
        e |= EPOLLIN;
    }
    if (events & POLLOUT) {  // 文件描述符可写 POLLOUT -> EPOLLOUT
        e |= EPOLLOUT;
    }
    if (events & POLLHUP) {  // 挂起事件 POLLHUP -> EPOLLHUP
        e |= EPOLLHUP;
    }
    if (events & POLLERR) {  // 错误事件 POLLERR -> EPOLLERR
        e |= EPOLLERR;
    }
    if (events & POLLRDNORM) {  // 普通数据的可读事件（用于区分优先级数据）POLLRDNORM -> EPOLLRDNORM
        e |= EPOLLRDNORM;
    }
    if (events & POLLWRNORM) {  // 普通数据的可写事件 POLLWRNORM -> EPOLLWRNORM
        e |= EPOLLWRNORM;
    }

    return e;
}

/* 将 epoll 事件标志转换为 poll 事件标志 */
static short nty_epollevent_2poll(uint32_t events) {
    short e = 0;
    if (events & EPOLLIN) {  // 文件描述符可读 EPOLLIN -> POLLIN
        e |= POLLIN;
    }
    if (events & EPOLLOUT) {  // 文件描述符可写 EPOLLOUT -> POLLOUT
        e |= POLLOUT;
    }
    if (events & EPOLLHUP) {  // 挂起事件 EPOLLHUP -> POLLHUP
        e |= POLLHUP;
    }
    if (events & EPOLLERR) {  // 错误事件 EPOLLERR -> POLLERR
        e |= POLLERR;
    }
    if (events & EPOLLRDNORM) {  // 普通数据的可读事件（用于区分优先级数据）PEOLLRDNORM -> POLLRDNORM
        e |= POLLRDNORM;
    }
    if (events & EPOLLWRNORM) {  // 普通数据的可写事件 EPOLLWRNORM -> POLLWRNORM
        e |= POLLWRNORM;
    }

    return e;
}

/* 模拟了系统调用 poll，通过协程调度器和 epoll 机制实现了事件的等待和处理（该函数由当前运行的协程调用） */
static int nty_poll_inner(struct pollfd *fds, nfds_t nfds, int timeout) {
    if (timeout == 0) {  // 超时时间为0，直接调用系统的poll函数，非阻塞模式
        return poll(fds, nfds, timeout);
    }
    if (timeout < 0) {  // 超时时间为负，设置为最大值（无限等待）
        timeout = INT_MAX;
    }

    nty_schedule * sched = nty_coroutine_get_sched();  // 获取当前线程的协程调度器
    if (sched == NULL) {  // 调度器不存在，说明程序未正确初始化协程环境，直接返回错误
        printf("scheduler not exit!\n");
        return -1;
    }

    nty_coroutine *co = sched->curr_thread;  // 获取当前协程
    int i;
    for (i = 0; i < nfds; i++) {  // 遍历 fds 中的文件描述符，逐个注册到 epoll
        struct epoll_event ev;
        ev.events = nty_pollevent_2epoll(fds[i].events);  // 将poll事件转换为epoll事件
        ev.data.fd = fds[i].fd;

        epoll_ctl(sched->poller_fd, EPOLL_CTL_ADD, fds[i].fd, &ev);  // 注册文件描述符

        co->events = fds[i].events;
        nty_schedule_sched_wait(co, fds[i].fd, fds[i].events, timeout);  // 将当前协程加入等待队列

        epoll_ctl(sched->poller_fd, EPOLL_CTL_DEL, fds[i].fd, &ev);  // 从epoll中移除文件描述符
        nty_schedule_desched_wait(fds[i].fd);  // 将文件描述符从调度器的等待队列中移除
    }

    return nfds;  // 处理的文件描述符数量
}

/* 创建并初始化套接字 */
int nty_socket(int domain, int type, int protocol) {
    if (socket_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int fd = socket_f(domain, type, protocol);  // 创建一个新的套接字
    if (fd == -1) {  // 创建失败
        printf("Failed to create a new socket\n");
        return -1;
    }

    int ret = fcntl(fd, F_SETFL, O_NONBLOCK);  // 设置套接字为非阻塞模式
    if (ret == -1) {  // 设置失败
        close(ret);  // 错误时关闭套接字
        return -1;
    }

    int reuse = 1;  // 启用 SO_REUSEADDR 选项
    ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &reuse, sizeof(reuse));  // 设置 SO_REUSEADDR 选项，允许重用地址
    if (ret == -1) {  // 设置失败
        close(fd);  // 错误时关闭套接字
        return -1;
    }

    return fd;
}

/* 非阻塞模式的 accept */
int nty_accept(int fd, struct sockaddr *addr, socklen_t *len) {
    if (accept_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int sockfd;
    while (1) {
        struct pollfd fds;
        fds.fd = fd;  // 监听的文件描述符，即服务器的监听套接字
        fds.events = POLLIN | POLLERR | POLLHUP;  // 设置监听事件，包括可读事件、错误事件和挂起事件
        nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可读

        sockfd = accept_f(fd, addr, len);  // 尝试接受客户端连接
        if (sockfd < 0) {  // accept 调用失败
            if (errno == EAGAIN) {  // 如果是资源暂时不可用（EAGAIN），继续尝试
                continue;
            } else if (errno == ECONNABORTED) {  // 连接已经被对端中止
                printf("accept : ECONNABORTED\n");
            } else if (errno == EMFILE || errno == ENFILE) {  // 进程已达到最大打开文件描述符数或已达到最大可打开文件描述符数
                printf("accept : EMFILE or ENFILE\n");
            }
            return -1;
        } else {
            break;
        }
    }

    int ret = fcntl(sockfd, F_SETFL, O_NONBLOCK);  // 将新连接的套接字设置为非阻塞模式
    if (ret == -1) {  // 设置失败
        close(sockfd);  // 错误时关闭套接字
        return -1;
    }

    int reuse = 1;  // 启用 SO_REUSEADDR 选项
    ret = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &reuse, sizeof(reuse));  // 设置 SO_REUSEADDR 选项，允许重用地址
    if (ret == -1) {  // 设置失败
        close(sockfd);  // 错误时关闭套接字
        return -1;
    }
    return sockfd;
}

/* 非阻塞模式的 connect */
int nty_connect(int fd, const struct sockaddr *addr, socklen_t addrlen) {
    if (connect_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int ret;
    while (1) {
        struct pollfd fds;
        fds.fd = fd;  // 监听的文件描述符，即用于连接的套接字
        fds.events = POLLOUT | POLLERR | POLLHUP;  // 设置监听事件，包括可写事件、错误事件和挂起事件
        nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可写

        ret = connect_f(fd, addr, addrlen);  // 发起连接
        if (ret == 0) {  // 连接成功
            break;
        }
        if (ret == -1 && (errno == EAGAIN ||  // 资源暂时不可用或连接正在进行中，继续尝试
                          errno == EWOULDBLOCK ||
                          errno == EINPROGRESS)) {
            continue;
        } else {  // 其他错误
            break;
        }
    }

    return ret;
}

/* 非阻塞模式的 recv */
ssize_t nty_recv(int fd, void *buf, size_t len, int flags) {
    if (recv_f == NULL) {  // 初始化钩子
        init_hook();
    }
    struct pollfd fds;
    fds.fd = fd;  // 监听的文件描述符，即期望读出数据的套接字
    fds.events = POLLIN | POLLERR | POLLHUP;  // 设置监听事件，包括可读事件、错误事件和挂起事件
    nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可读

    int ret = recv_f(fd, buf, len, flags);  // 读取数据
    if (ret <= 0) {  // 读取失败
        if (ret == 0) {  // 对端正常关闭连接
            printf("Connection closed by peer\n");
        } else {  // 错误处理
            if (errno == ECONNRESET) {  // 对端非正常关闭
                printf("Connection reset by peer\n");
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 数据暂时不可用，非阻塞模式下
            } else {
                printf("recv error: %s\n", strerror(errno));
            }
        }
    }

    return ret;
}

/* 非阻塞模式的 send */
ssize_t nty_send(int fd, const void *buf, size_t len, int flags) {
    if (send_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int sent = 0;
    int ret = send_f(fd, ((char *) buf) + sent, len - sent, flags);  // 单独尝试发送一次以优化性能
    if (ret <= 0) {  // 第一次发送失败，检查错误类型
        if (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
            return ret;  // 不可恢复的错误，直接返回
        }
    } else {
        sent += ret;  // 累计已发送字节数
    }

    while (sent < len) {  // 没有全部发送完毕
        struct pollfd fds;
        fds.fd = fd;  // 监听的文件描述符，即期望写入数据的套接字
        fds.events = POLLOUT | POLLERR | POLLHUP;  // 设置监听事件，包括可写事件、错误事件和挂起事件
        nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可写

        ret = send_f(fd, ((char *) buf) + sent, len - sent, flags);
        if (ret <= 0) {  // 再次发送失败，检查错误类型
            if (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                break;  // 不可恢复的错误，退出循环
            }
        } else {
            sent += ret;  // 累计已发送字节数
        }
    }
    if (ret <= 0 && sent == 0) {  // 未发送任何数据，返回错误
        return ret;
    }

    return sent;  // 返回已成功发送的字节数
}

/* 非阻塞模式的 sendto */
ssize_t nty_sendto(int fd, const void *buf, size_t len, int flags,
                   const struct sockaddr *dest_addr, socklen_t addrlen) {
    if (sendto_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int sent = 0;
    while (sent < len) {
        struct pollfd fds;
        fds.fd = fd;  // 监听的文件描述符，即期望写入数据的套接字
        fds.events = POLLOUT | POLLERR | POLLHUP;  // 设置监听事件，包括可写事件、错误事件和挂起事件
        nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可写

        int ret = sendto_f(fd, ((char *) buf) + sent, len - sent, flags, dest_addr, addrlen);
        if (ret <= 0) {  // 发送失败，检查错误类型
            if (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                break;  // 不可恢复的错误，退出循环
            }
        } else {
            sent += ret;  // 累计已发送字节数
        }
    }

    return sent;  // 返回成功发送的字节数
}

/* 非阻塞模式的 recvfrom */
ssize_t nty_recvfrom(int fd, void *buf, size_t len, int flags,
                     struct sockaddr *src_addr, socklen_t *addrlen) {
    if (recvfrom_f == NULL) {  // 初始化钩子
        init_hook();
    }
    struct pollfd fds;
    fds.fd = fd;  // 监听的文件描述符，即期望读出数据的套接字
    fds.events = POLLIN | POLLERR | POLLHUP;  // 设置监听事件，包括可读事件、错误事件和挂起事件
    nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可读

    int ret = recvfrom_f(fd, buf, len, flags, src_addr, addrlen);
    if (ret <= 0) {  // 读取失败
        if (ret == 0) {  // 对端正常关闭连接
            printf("Connection closed by peer\n");
        } else {  // 错误处理
            if (errno == ECONNRESET) {  // 对端非正常关闭
                printf("Connection reset by peer\n");
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 数据暂时不可用，非阻塞模式下
            } else {
                printf("recv error: %s\n", strerror(errno));
            }
        }
    }

    return ret;
}

/* 非阻塞模式的 read */
ssize_t nty_read(int fd, void *buf, size_t count) {
    if (read_f == NULL) {  // 初始化钩子
        init_hook();
    }
    struct pollfd fds;
    fds.fd = fd;  // 监听的文件描述符，即期望读出数据的套接字
    fds.events = POLLIN | POLLERR | POLLHUP;  // 设置监听事件，包括可读事件、错误事件和挂起事件
    nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可读

    int ret = read_f(fd, buf, count);
    if (ret <= 0) {  // 读取失败
        if (ret == 0) {  // 对端正常关闭连接
            printf("Connection closed by peer\n");
        } else {  // 错误处理
            if (errno == ECONNRESET) {  // 对端非正常关闭
                printf("Connection reset by peer\n");
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 数据暂时不可用，非阻塞模式下
            } else {
                printf("read error: %s\n", strerror(errno));
            }
        }
    }
    return ret;
}

/* 非阻塞模式的 write */
ssize_t nty_write(int fd, const void *buf, size_t count) {
    if (write_f == NULL) {  // 初始化钩子
        init_hook();
    }
    int sent = 0;
    int ret = write_f(fd, ((char *) buf) + sent, count - sent);
    if (ret <= 0) {  // 第一次发送失败，检查错误类型
        if (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
            return ret;  // 不可恢复的错误，直接返回
        }
    } else {
        sent += ret;  // 累计已发送字节数
    }

    while (sent < count) {  // 没有全部发送完毕
        struct pollfd fds;
        fds.fd = fd;  // 监听的文件描述符，即期望写入数据的套接字
        fds.events = POLLOUT | POLLERR | POLLHUP;  // 设置监听事件，包括可读事件、错误事件和挂起事件
        nty_poll_inner(&fds, 1, NO_TIMEOUT);  // 等待套接字可写

        ret = write_f(fd, ((char *) buf) + sent, count - sent);
        if (ret <= 0) {  // 再次发送失败，检查错误类型
            if (ret == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                break;  // 不可恢复的错误，退出循环
            }
        } else {
            sent += ret;  // 累计已发送字节数
        }
    }
    if (ret <= 0 && sent == 0) {  // 未发送任何数据，返回错误
        return ret;
    }

    return sent;  // 返回已成功发送的字节数
}

/* 关闭文件描述符 */
int nty_close(int fd) {
    if (close_f == NULL) {  // 初始化钩子
        init_hook();
    }
#if 1
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程的调度器
    nty_coroutine *co = sched->curr_thread;  // 获取当前协程
    if (co) {
        co->status |= BIT(NTY_COROUTINE_STATUS_FDEOF);  // 更新协程状态
        TAILQ_INSERT_TAIL(&nty_coroutine_get_sched()->ready, co, ready_next);  // 将协程插入就绪队列
    }
#endif
    return close_f(fd);
}

/* 扩展的 socket */
int socket(int domain, int type, int protocol) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return socket_f(domain, type, protocol);  // 调用原始 socket
    }

    return nty_socket(domain, type, protocol);  // 调用协程环境下的扩展 nty_socket
}

/* 扩展的 read */
ssize_t read(int fd, void *buf, size_t count) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return read_f(fd, buf, count);  // 调用原始 read
    }

    return nty_read(fd, buf, count);  // 调用协程环境下的扩展 nty_read
}

/* 扩展的 recv */
ssize_t recv(int fd, void *buf, size_t len, int flags) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return recv_f(fd, buf, len, flags);  // 调用原始 recv
    }

    return nty_recv(fd, buf, len, flags);  // 调用协程环境下的扩展 nty_recv
}

/* 扩展的 recvfrom */
ssize_t recvfrom(int fd, void *buf, size_t len, int flags,
                 struct sockaddr *src_addr, socklen_t *addrlen) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return recvfrom_f(fd, buf, len, flags, src_addr, addrlen);  // 调用原始 recvfrom
    }

    return nty_recvfrom(fd, buf, len, flags, src_addr, addrlen);  // 调用协程环境下的扩展 nty_recvfrom
}

/* 扩展的 write */
ssize_t write(int fd, const void *buf, size_t count) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return write_f(fd, buf, count);  // 调用原始 write
    }

    return nty_write(fd, buf, count);  // 调用协程环境下的扩展 nty_write
}

/* 扩展的 send */
ssize_t send(int fd, const void *buf, size_t len, int flags) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return send_f(fd, buf, len, flags);  // 调用原始 send
    }

    return nty_send(fd, buf, len, flags);  // 调用协程环境下的扩展 nty_send
}

/* 扩展的 sendto */
ssize_t sendto(int fd, const void *buf, size_t len, int flags,
               const struct sockaddr *dest_addr, socklen_t addrlen) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return sendto_f(fd, buf, len, flags, dest_addr, addrlen);  // 调用原始 sendto
    }

    return nty_sendto(fd, buf, len, flags, dest_addr, addrlen);  // 调用协程环境下的扩展 nty_sendto
}

/* 扩展的 accept */
int accept(int fd, struct sockaddr *addr, socklen_t *len) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return accept_f(fd, addr, len);  // 调用原始 accept
    }

    return nty_accept(fd, addr, len);  // 调用协程环境下的扩展 nty_accept
}

/* 扩展的 connect */
int connect(int fd, const struct sockaddr *addr, socklen_t addrlen) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return connect_f(fd, addr, addrlen);  // 调用原始 connect
    }

    return nty_connect(fd, addr, addrlen);  // 调用协程环境下的扩展 nty_connect
}

/* 扩展的 close */
int close(int fd) {
    nty_schedule * sched = nty_coroutine_get_sched();
    if (sched == NULL) {  // 非协程环境
        return close_f(fd);  // 调用原始 close
    }

    return nty_close(fd);  // 调用协程环境下的扩展 nty_close
}

#endif
