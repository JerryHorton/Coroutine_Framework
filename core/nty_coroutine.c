#include "nty_coroutine.h"

pthread_key_t global_sched_key;  // 全局的线程局部存储（TLS）键 global_sched_key，用于存储与每个线程相关的调度器信息 (nty_schedule 结构体)
static pthread_once_t sched_key_once = PTHREAD_ONCE_INIT;  // 保证初始化逻辑在整个程序生命周期中 仅执行一次

// 基于 _USE_UCONTEXT 的实现
/* 保存当前协程的栈内容到协程的结构体（nty_coroutine）中 */
static void _save_stack(nty_coroutine *co) {  // 注意栈的内存分配是从高地址向低地址增长的
    char *top = co->sched->stack + co->sched->stack_size;  // 获取当前协程结构体的栈顶地址
    char dummy = 0;  // 局部变量是分配在当前栈上的，所以 &dummy 可以用来获取当前栈指针的位置（即栈的实际运行位置）
    assert(top - &dummy <= NTY_CO_MAX_STACKSIZE);  // 检查当前栈的实际使用量是否超过了最大允许的栈大小
    if (co->stack_size < top - &dummy) {  // 协程的实际栈使用大小（top - &dummy）超过了之前保存的大小（co->stack_size）
        co->stack = realloc(co->stack, top - &dummy);  // 重新分配一块更大的内存
        assert(co->stack != NULL);
    }
    co->stack_size = top - &dummy;  // 更新协程的栈大小
    memcpy(co->stack, &dummy, co->stack_size);  // 将当前栈上的内容拷贝到协程的堆内存中
}

/* 恢复协程的栈内容，将协程之前保存的栈数据从堆内存复制回到原始的栈空间 */
static void _load_stack(nty_coroutine *co) {
    memcpy(co->sched->stack + co->sched->stack_size - co->stack_size,  // 栈的原始栈顶（协程恢复时应该恢复的地方）
           co->stack, co->stack_size);
}

/* 执行协程的逻辑，并在协程完成后进行适当的状态管理 */
static void _exec(void *lt) {
    nty_coroutine *co = (nty_coroutine *) lt;  // 当前正在执行的协程
    co->func(co->arg);  // 执行协程的主函数，启动协程的业务逻辑
    co->status |= (BIT(NTY_COROUTINE_STATUS_EXITED) | BIT(NTY_COROUTINE_STATUS_FDEOF) |
                   BIT(NTY_COROUTINE_STATUS_DETACH));  // 更新协程的状态
    nty_coroutine_yield(co);  // 将协程挂起
}



extern int nty_schedule_create(int stack_size);

/* 释放协程对象的内存资源 */
void nty_coroutine_free(nty_coroutine *co) {
    if (co == NULL) {
        return;
    }
    __sync_fetch_and_sub(&co->sched->spawned_coroutines, 1);  //  使用原子操作减少调度器的协程计数
    // 加锁防止并发释放同一协程
    pthread_mutex_lock(&co->sched->resource_mutex);
    // 标记协程已释放，防止双重释放
    if (co->is_freed) {
        pthread_mutex_unlock(&co->sched->resource_mutex);
        return;
    }
    co->is_freed = 1;
    if (co->stack) {  // 释放栈内存
        free(co->stack);
        co->stack = NULL;  // 防止之后误用悬空指针
    }
    pthread_mutex_unlock(&co->sched->resource_mutex);
    free(co);  // 释放协程对象本身
}

/* 初始化协程 */
static void nty_coroutine_init(nty_coroutine *co) {
    // 使用 ucontext 实现协程
    getcontext(&co->ctx);  // 获取当前上下文（包括堆栈等）
    co->ctx.uc_stack.ss_sp = co->sched->stack;  // 设置栈空间指针
    co->ctx.uc_stack.ss_size = co->sched->stack_size;  // 设置栈空间大小
    co->ctx.uc_link = &co->sched->ctx;  // 设置上下文链接，在协程退出时会切换回调度器上下文
    makecontext(&co->ctx, (void (*)(void)) _exec, 1, (void *) co);  // 设置协程函数（_exec）和参数

    co->status = BIT(NTY_COROUTINE_STATUS_READY);  // 将协程的状态设置为就绪态
}

/* 挂起协程 */
void nty_coroutine_yield(nty_coroutine *co) {
    co->ops = 0;  // 重置协程的操作标志，表示不再进行其他操作
    // 使用 ucontext 切换上下文 (swapcontext)
    if ((co->status & BIT(NTY_COROUTINE_STATUS_EXITED)) == 0) {  // 协程没有退出则需要保存协程的栈信息
        _save_stack(co);  // 保存协程的栈信息，确保栈的内容在协程切换时不丢失
    }
    swapcontext(&co->ctx, &co->sched->ctx);  // 切换上下文
}

/* 恢复或启动协程的执行 */
int nty_coroutine_resume(nty_coroutine *co) {
    if (co->status & BIT(NTY_COROUTINE_STATUS_NEW)) {  // 如果协程处于 "新建" 状态
        nty_coroutine_init(co);  // 初始化协程（主要是初始化栈、上下文等）
    }
    _load_stack(co);  // 如果使用 ucontext，加载栈信息（恢复协程栈）
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取调度器
    sched->curr_thread = co;  // 设置当前正在执行的线程为该协程
    swapcontext(&sched->ctx, &co->ctx);  // 使用 ucontext 切换上下文到协程的上下文
    sched->curr_thread = NULL;  // 协程执行完后，重置调度器的当前线程

    if (co->status & BIT(NTY_COROUTINE_STATUS_EXITED)) {  // 如果协程已退出
        if (co->status & BIT(NTY_COROUTINE_STATUS_DETACH)) {  // 协程已结束，不再需要管理
            nty_coroutine_free(co);  // 释放协程资源
        }
        return -1;  // 协程已退出，返回 -1
    }
    return 0;  // 协程成功恢复，返回 0
}

/* 通过操作计数 (ops) 来判断是否需要将当前协程的控制权交还给调度器，并将协程重新放回调度队列 */
void nty_coroutine_renice(nty_coroutine *co) {
    co->ops++;  // 增加协程的 ops 计数器
    if (co->ops < 5) {
        return;
    }
    TAILQ_INSERT_TAIL(&nty_coroutine_get_sched()->ready, co, ready_next);  // 当前协程插入到调度器的就绪队列 (ready) 的尾部
    nty_coroutine_yield(co);  // 让出当前协程的执行权，交还给调度器
}

/* 协程的睡眠机制 */
void nty_coroutine_sleep(uint64_t msecs) {
    nty_coroutine *co = nty_coroutine_get_sched()->curr_thread;  // 获取当前正在运行的协程
    if (msecs == 0) {  // 休眠时间为 0，协程只让出 CPU，但不进入睡眠状态，会在下一轮调度时再次执行
        TAILQ_INSERT_TAIL(&co->sched->ready, co, ready_next);  // 直接将当前协程插入调度器的就绪队列
        nty_coroutine_yield(co);  // 让出当前协程的执行权
    } else {
        nty_schedule_sched_sleepdown(co, msecs);  // 协程加入到调度器的睡眠队列中，并设置一个定时器，用于唤醒该协程
    }
}

/* 设置当前协程分离（detach）状态 */
void nty_coroutine_detach(void) {
    nty_coroutine *co = nty_coroutine_get_sched()->curr_thread;  // 获取当前正在运行的协程
    co->status |= BIT(NTY_COROUTINE_STATUS_DETACH);  // 将协程的状态标志位设置为分离状态
}

/* 释放协程调度器的资源 */
static void nty_coroutine_sched_key_destructor(void *data) {
    free(data);
}

/* 全局初始化操作 */
static void __attribute__((constructor(1000)))
nty_coroutine_sched_key_creator(void) {  // 标记函数为构造函数，会在程序执行 main 函数之前自动调用
    assert(pthread_key_create(&global_sched_key, nty_coroutine_sched_key_destructor) ==
           0);  // 创建一个线程局部存储（TLS）键 global_sched_key，为每个线程分配独立的存储空间
    assert(pthread_setspecific(global_sched_key, NULL) == 0);  // 初始化时明确指定键值为空，避免使用未初始化的值
}

/* 创建新的协程对象 */
int nty_coroutine_create(nty_coroutine **new_co, proc_coroutine func, void *arg) {
    assert(pthread_once(&sched_key_once, nty_coroutine_sched_key_creator) == 0);  // 确保调度器键 global_sched_key 和析构函数已经正确注册
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取当前线程的调度器

    if (sched == NULL) {  // 当前线程还没有关联调度器，则创建新的调度器
        nty_schedule_create(0);  // 创建调度器
        sched = nty_coroutine_get_sched();  // 重新获取当前线程调度器
        if (sched == NULL) {  // 调度器创建失败
            printf("Failed to create scheduler\n");
            return -1;
        }
    }
    nty_coroutine *co = calloc(1, sizeof(nty_coroutine));  // 分配协程对象
    if (co == NULL) {  // 内存分配失败
        printf("Failed to allocate memory for new coroutine\n");
        return -2;
    }
    // 使用 ucontext，栈由调度器共享栈实现，无需为协程分配独立栈
    co->stack = NULL;
    co->stack_size = 0;
    co->sched = sched;  // 关联调度器
    co->status = BIT(NTY_COROUTINE_STATUS_NEW);  // 设置新建状态
    co->id = sched->spawned_coroutines++;  // 为协程分配一个唯一的 ID
    co->func = func;  // 设置协程的执行函数
    co->fd = -1;  // 协程等待的文件描述符
    co->events = 0;  // 协程等待的事件类型
    co->arg = arg;  // 设置协程的执行函数的参数
    co->birth = nty_coroutine_usec_now();  // 记录协程创建时的时间戳
    *new_co = co;  // 创建的协程指针存储到 new_co 中，供调用者使用
    TAILQ_INSERT_TAIL(&co->sched->ready, co, ready_next);  // 将协程加入调度器的 ready 队列，等待被调度执行
    return 0;
}





