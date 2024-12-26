#include "nty_coroutine.h"

pthread_key_t global_sched_key;  // 全局的线程局部存储（TLS）键 global_sched_key，用于存储与每个线程相关的调度器信息 (nty_schedule 结构体)
static pthread_once_t sched_key_once = PTHREAD_ONCE_INIT;  // 保证初始化逻辑在整个程序生命周期中 仅执行一次

// https://github.com/halayli/lthread/blob/master/src/lthread.c#L58

#ifdef _USE_UCONTEXT  // 基于 _USE_UCONTEXT 的实现

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

#else

int _switch(nty_cpu_ctx *new_ctx, nty_cpu_ctx *cur_ctx);

#ifdef __i386__  // 32 位架构
__asm__ (
"    .text                                                             \n"
"    .p2align 2,,3                                                     \n"
".globl _switch                                                        \n"
"_switch:                                                              \n"
"__switch:                                                             \n"
"       movl 8(%esp), %edx      # 从当前栈中读取 cur_ctx 的指针到 edx      \n"
"       movl %esp, 0(%edx)      # 保存当前栈指针到 cur_ctx->esp           \n"
"       movl %ebp, 4(%edx)      # 保存帧指针到 cur_ctx->ebp               \n"
"       movl (%esp), %eax       # 读取当前栈顶（返回地址）到 eax            \n"
"       movl %eax, 8(%edx)      # 保存返回地址到 cur_ctx->eip             \n"
"       movl %ebx, 12(%edx)     # 保存寄存器 ebx 到 cur_ctx->ebx          \n"
"       movl %esi, 16(%edx)     # 保存寄存器 esi 到 cur_ctx->esi          \n"
"       movl %edi, 20(%edx)     # 保存寄存器 edi 到 cur_ctx->edi          \n"
"       movl 4(%esp), %edx      # 从栈中读取 new_ctx 的指针到 edx          \n"
"       movl 20(%edx), %edi     # 恢复目标协程的 edi 到寄存器 edi           \n"
"       movl 16(%edx), %esi     # 恢复目标协程的 esi 到寄存器 esi           \n"
"       movl 12(%edx), %ebx     # 恢复目标协程的 ebx 到寄存器 ebx           \n"
"       movl 0(%edx), %esp      # 恢复目标协程的栈指针到 esp                \n"
"       movl 4(%edx), %ebp      # 恢复目标协程的帧指针到 ebp                \n"
"       movl 8(%edx), %eax      # 恢复目标协程的返回地址到 eax               \n"
"       movl %eax, (%esp)       # 将返回地址写入栈顶                        \n"
"       ret                     # 跳转到目标协程的返回地址                   \n"
);

#elif defined(__x86_64__)
__asm__ (
"    .text                                                     \n"
"       .p2align 4,,15                                         \n"
".globl _switch                                                \n"
".globl __switch                                               \n"
"_switch:                                                      \n"
"__switch:                                                     \n"
"       movq %rsp, 0(%rsi)      # 保存栈指针到 cur_ctx->esp      \n"
"       movq %rbp, 8(%rsi)      # 保存帧指针到 cur_ctx->ebp      \n"
"       movq (%rsp), %rax       # 保存当前任务的返回地址           \n"
"       movq %rax, 16(%rsi)     # 保存到 cur_ctx->eip           \n"
"       movq %rbx, 24(%rsi)     # 保存 rbx 到 cur_ctx->ebx      \n"
"       movq %r12, 32(%rsi)     # 保存 r12 到 cur_ctx->r12      \n"
"       movq %r13, 40(%rsi)     # 保存 r13 到 cur_ctx->r13      \n"
"       movq %r14, 48(%rsi)     # 保存 r14 到 cur_ctx->r14       \n"
"       movq %r15, 56(%rsi)     # 保存 r15 到 cur_ctx->r15       \n"
"       movq 56(%rdi), %r15     # 恢复 new_ctx->r15 到 r15       \n"
"       movq 48(%rdi), %r14     # 恢复 new_ctx->r14 到 r14       \n"
"       movq 40(%rdi), %r13     # 恢复 new_ctx->r13 到 r13       \n"
"       movq 32(%rdi), %r12     # 恢复 new_ctx->12 到 r12       \n"
"       movq 24(%rdi), %rbx     # 恢复 new_ctx->ebx 到 rbx      \n"
"       movq 8(%rdi), %rbp      # 恢复 new_ctx->ebp 到帧指针     \n"
"       movq 0(%rdi), %rsp      # 恢复 new_ctx->esp 到栈指针     \n"
"       movq 16(%rdi), %rax     # 恢复指令指针到 %rax            \n"
"       movq %rax, (%rsp)       # 将返回地址写入栈顶              \n"
"       ret                     # 返回新任务的执行地址            \n"
);
#endif

/* 协程执行 */
static void _exec(void *lt) {

#if defined(__lvm__) && defined(__x86_64__)
    __asm__("movq 16(%%rbp), %[lt]" : [lt] "=r" (lt));  // 用汇编显式读取栈帧中的参数
#endif

    nty_coroutine *co = (nty_coroutine*)lt;  // 转换为协程类型指针
    co->func(co->arg);  // 传入参数，调用协程的任务函数
    co->status |= (BIT(NTY_COROUTINE_STATUS_EXITED) | BIT(NTY_COROUTINE_STATUS_FDEOF) | BIT(NTY_COROUTINE_STATUS_DETACH));  // 更新协程的状态，标记其已退出

    nty_coroutine_yield(co);  // 暂停当前协程的执行，并切换到调度器或其他协程
}

/* 优化协程的内存使用 */
static inline void nty_coroutine_madvise(nty_coroutine *co) {
    // 计算当前栈使用大小
    size_t current_stack = (co->stack + co->stack_size)  // 栈顶（高地址）
                           - co->ctx.esp;  // 协程运行时的栈顶位置（已使用栈区域的末端）
    assert(current_stack <= co->stack_size);

    if (current_stack < co->last_stack_size &&  // 当前使用的栈空间比上次记录的使用量更小，说明有一部分栈空间被释放
        co->last_stack_size > co->sched->page_size) {  // 上次的栈使用量超过了一个内存页大小，才考虑释放
        size_t tmp = current_stack + (-current_stack & (co->sched->page_size - 1));  // 对当前栈使用大小 current_stack 进行页对齐，计算出下一次可以释放的页边界位置
        assert(madvise(co->stack, co->stack_size-tmp, MADV_DONTNEED) == 0);  // 将未使用的内存的物理页回收
    }
    co->last_stack_size = current_stack; //  更新记录的栈大小，供下次调用时比较
}
#endif

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

#ifdef _USE_UCONTEXT  // 使用 ucontext 实现协程
    getcontext(&co->ctx);  // 获取当前上下文（包括堆栈等）
    co->ctx.uc_stack.ss_sp = co->sched->stack;  // 设置栈空间指针
    co->ctx.uc_stack.ss_size = co->sched->stack_size;  // 设置栈空间大小
    co->ctx.uc_link = &co->sched->ctx;  // 设置上下文链接，在协程退出时会切换回调度器上下文
    makecontext(&co->ctx, (void (*)(void)) _exec, 1, (void *) co);  // 设置协程函数（_exec）和参数

#else  // 使用自定义方式实现协程
    void **stack = (void **)(co->stack + co->stack_size);  // 指向协程栈的顶部
    stack[-3] = NULL;  // 保存空值，确保栈的正确性
    stack[-2] = (void *)co;  // 保存协程对象指针到栈中（用于执行时获取上下文）
    co->ctx.esp = (void*)stack - (4 * sizeof(void*));  // 设置栈指针
    co->ctx.ebp = (void*)stack - (3 * sizeof(void*));  // 设置帧指针
    co->ctx.eip = (void*)_exec;  // 设置程序计数器为 `_exec` 函数
#endif

    co->status = BIT(NTY_COROUTINE_STATUS_READY);  // 将协程的状态设置为就绪态
}

/* 挂起协程 */
void nty_coroutine_yield(nty_coroutine *co) {
    co->ops = 0;  // 重置协程的操作标志，表示不再进行其他操作
#ifdef _USE_UCONTEXT  // 使用 ucontext 切换上下文 (swapcontext)
    if ((co->status & BIT(NTY_COROUTINE_STATUS_EXITED)) == 0) {  // 协程没有退出则需要保存协程的栈信息
        _save_stack(co);  // 保存协程的栈信息，确保栈的内容在协程切换时不丢失
    }
    swapcontext(&co->ctx, &co->sched->ctx);  // 切换上下文
#else
    _switch(&co->sched->ctx, &co->ctx);  // 手动实现的上下文切换函数进行低级的协程切换
#endif
}

/* 恢复或启动协程的执行 */
int nty_coroutine_resume(nty_coroutine *co) {
    if (co->status & BIT(NTY_COROUTINE_STATUS_NEW)) {  // 如果协程处于 "新建" 状态
        nty_coroutine_init(co);  // 初始化协程（主要是初始化栈、上下文等）
    }
#ifdef _USE_UCONTEXT
    _load_stack(co);  // 如果使用 ucontext，加载栈信息（恢复协程栈）
#endif
    nty_schedule *sched = nty_coroutine_get_sched();  // 获取调度器
    sched->curr_thread = co;  // 设置当前正在执行的线程为该协程
#ifdef _USE_UCONTEXT
    swapcontext(&sched->ctx, &co->ctx);  // 使用 ucontext 切换上下文到协程的上下文
#else
    _switch(&co->ctx, &co->sched->ctx);  // 使用自定义的 _switch 切换到协程的上下文
    nty_coroutine_madvise(co);  // 执行栈的内存管理优化（如果需要）*/
#endif
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
    return;
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

#ifdef _USE_UCONTEXT  // 使用 ucontext，栈由调度器共享栈实现，无需为协程分配独立栈
    co->stack = NULL;
    co->stack_size = 0;
#else
    int ret = posix_memalign(&co->stack, getpagesize(), sched->stack_size);  // 为协程分配对齐到页面大小的独立栈
    if (ret) {  // 栈分配失败
        printf("Failed to allocate stack for new coroutine\n");
        free(co);
        return -3;
    }
    co->stack_size = sched->stack_size;
#endif
    co->sched = sched;  // 关联调度器
    co->status = BIT(NTY_COROUTINE_STATUS_NEW);  // 设置新建状态
    co->id = sched->spawned_coroutines++;  // 为协程分配一个唯一的 ID
    co->func = func;  // 设置协程的执行函数
#if CANCEL_FD_WAIT_UINT64  // 复杂的事件模型，则不使用简化的文件描述符等待字段
    co->fd = -1;  // 协程等待的文件描述符
    co->events = 0;  // 协程等待的事件类型
#else
    co->fd_wait = -1;  // 标记协程是否正在等待某个文件描述符的事件
#endif
    co->arg = arg;  // 设置协程的执行函数的参数
    co->birth = nty_coroutine_usec_now();  // 记录协程创建时的时间戳
    *new_co = co;  // 创建的协程指针存储到 new_co 中，供调用者使用
    TAILQ_INSERT_TAIL(&co->sched->ready, co, ready_next);  // 将协程加入调度器的 ready 队列，等待被调度执行
    return 0;
}





