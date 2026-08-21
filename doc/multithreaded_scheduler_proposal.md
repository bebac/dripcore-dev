# Multithreaded Coroutine Scheduler Proposal

## Executive Summary

This document proposes a redesign of DripCore's scheduler to support multi-core execution while introducing higher-level abstractions suitable for building production applications. The current implementation has successfully completed **Phase 1 and Phase 2** of the migration path, establishing the foundation for multi-core execution. Phase 3 and beyond remain to be implemented.

**Current Status:** Phase 1 ✅ | Phase 2 ✅ | Phase 3 ⏳ | Phase 4 ⏳ | Phase 5 ⏳

## Implementation Progress

### ✅ Completed (Phase 1 + 2)
- **Scheduler/Executor Separation**: `scheduler` class now wraps `executor`, with clear separation of concerns
- **Thread-Local Executor**: `executor::current()` provides thread-local accessor, eliminating raw scheduler pointers in tasks
- **Updated Awaiters**: All awaiters (`yield_awaiter`, `io_awaiter`, `sleep_awaiter`) use `executor::current()`
- **Task Spawning**: Tasks use `executor::current()->spawn<T>()` instead of holding scheduler pointers
- **Backward Compatibility**: Existing code continues to work with the new architecture

### ✅ Additional Features Implemented (Beyond Original Proposal)
- **Channel Support**: Full `channel<T>` implementation with `try_send()`, `send()` (awaitable/blocking), `try_recv()`, `recv()` (awaitable), `close()` support, and cancellation integration
- **Timer Support**: Timer wheel in executor with `register_timer()`, `sleep_for()` support via `sleep_awaiter`
- **Sleep Primitives**: `task::sleep_for()` method for convenient sleep operations
- **Cancellation & Shutdown**: `dripcore::cancelled` exception, `task_control` with `cancel()`, `stop()`, `cancelled()`, `exception()`; `scheduler::shutdown()` with graceful drain + forced frame-destroy (see `doc/shutdown_and_cancellation_design.md`)
- **Task Base Class**: `execute()` delegates to virtual `body()`/`teardown()`; `teardown()` is always-run async cleanup (see `doc/shutdown_and_cancellation_design.md §9`)
- **Exception Propagation**: Non-cancelled exceptions transferred from task promise to `task_control_block`, observable via `task_control::exception()` after `done()`
- **Awaitable Composition**: `dripcore::awaitable<T>` with symmetric transfer, single continuation, child-frame ownership (see `doc/dripcore_continuations_proposal.md`)

### ⏳ Remaining (Phase 3+)
- **Multi-Threading**: Multiple executor threads with work distribution
- **Work-Stealing**: Load balancing across executor threads
- **Cross-Thread Submission**: MPSC queues for task submission from any thread
- **Service Abstraction**: Long-running supervised components
- **Global Queue**: For overflow and external spawns

The current single-threaded scheduler is well-designed for I/O multiplexing but cannot leverage multiple CPU cores or elegantly express long-running services alongside short-lived tasks.

## Current Architecture Analysis

### Strengths
- **Clean coroutine integration**: The `coroutine` with `promise_type` correctly manages coroutine lifetime
- **Platform abstraction**: Unified interface over kqueue/epoll with ONESHOT semantics
- **Memory safety**: `shared_ptr` ownership prevents premature task destruction
- **Wake notification**: Event-based wakeup enables efficient ready-task signaling
- **Thread-local design**: `executor::current()` eliminates cross-thread pointer issues

### Resolved Limitations (Phase 1-2)

#### ✅ Raw Scheduler Pointer Problem - RESOLVED
**Original Issue**: Every `task` held `scheduler* scheduler_ptr_`. In a multithreaded design:
- Thread safety: Multiple threads accessing scheduler state requires locking
- Affinity ambiguity: Which thread should execute a resumed coroutine?
- Cross-thread operations: `spawn()`, `yield()`, and I/O registration from any task can race

**Solution Implemented**: 
- Removed raw scheduler pointers from tasks
- Tasks now use thread-local `executor::current()` accessor
- All awaiters (`yield_awaiter`, `io_awaiter`, `sleep_awaiter`) use `executor::current()`
- Clean separation: `scheduler` manages executors, `executor` manages per-thread state

#### ✅ Flat Task Abstraction - PARTIALLY RESOLVED
**Original Issue**: All coroutines were homogeneous with no distinction between I/O-bound and CPU-bound tasks.

**Progress**:
- Task hierarchy improved with proper base class and spawning methods
- Channel support enables better inter-task communication patterns
- Timer support enables time-based operations

**Remaining**: Service abstraction for long-running components still needed.

### Current Limitations

#### 1. Single-Threaded Execution
The scheduler still runs entirely on one CPU core. The `run()` loop is sequential:
```cpp
while (!tasks_.empty()) {
    process_ready_tasks();    // CPU work
    process_events();         // I/O wait
    cleanup_done_tasks();
}
```
Multi-core systems cannot be utilized. This is the primary remaining limitation.

#### 2. Missing Multi-Threading Infrastructure
- **Work stealing**: No mechanism to balance load across cores
- **Cross-thread submission**: No MPSC queues for external spawns
- **Multiple executors**: Only single executor supported currently
- **Service abstraction**: No supervised, long-running component support

#### 3. Partially Implemented Building Blocks
- **Channels**: ✅ FULLY IMPLEMENTED - `try_send()`, `send()` (awaitable), `try_recv()`, `recv()` (awaitable), `close()` support, cancellation integration
- **Cancellation & Shutdown**: ✅ FULLY IMPLEMENTED - `task_control` with `cancel()`, `stop()`, `cancelled()`, `exception()`; `scheduler::shutdown()` (see `doc/shutdown_and_cancellation_design.md`)
- **Services**: ❌ NOT IMPLEMENTED - No `service` or `service_supervisor` classes
- **Timers**: ✅ FULLY IMPLEMENTED - Timer wheel in executor, `sleep_for()` support
- **Work stealing**: ❌ NOT IMPLEMENTED - No work-stealing deques or global queue

## Current Architecture (Post Phase 1-2)

### Implemented Design Philosophy

**Separation of Concerns** (Partially Implemented):
1. **Scheduler** → ✅ Manages executor(s) - currently single executor, ready for multi-executor
2. **Executor** → ✅ Per-thread event loop - fully implemented with thread-local accessor
3. **Task** → ✅ Lightweight coroutine - uses `executor::current()` instead of raw pointers
4. **Service** → ❌ NOT YET IMPLEMENTED - Supervised, long-running component
5. **Channel** → ✅ FULLY IMPLEMENTED - Type-safe queues with `try_send()`, `send()` (awaitable), `try_recv()`, `recv()` (awaitable), `close()`
6. **Cancellation** → ✅ FULLY IMPLEMENTED - `task_control` with `cancel()`, `stop()`, `cancelled()`, `exception()`; `scheduler::shutdown()` (see `doc/shutdown_and_cancellation_design.md`)
7. **Awaitable** → ✅ FULLY IMPLEMENTED - `dripcore::awaitable<T>` with symmetric transfer (see `doc/dripcore_continuations_proposal.md`)

**Ownership Model** (Implemented):
- ✅ Tasks own their state, not raw scheduler pointers
- ✅ Executors are owned by the scheduler (via `unique_ptr`)
- ❌ Services own their tasks and resources (not yet implemented)

### Current Implementation Details

The current architecture successfully implements the foundation for multi-threading:

```cpp
// Current scheduler (Phase 2 - Single executor)
class scheduler {
public:
    explicit scheduler(size_t num_executors = 1);  // Currently ignores num_executors
    ~scheduler();
    
    template <typename T, typename... Args>
    void spawn(Args&&... args) {
        executor_->spawn<T>(std::forward<Args>(args)...);
    }
    
    void run() { executor_->run(); }
    
private:
    std::unique_ptr<executor> executor_;  // Currently single executor
};

// Current executor (Fully implemented with thread-local access)
class executor {
public:
    executor();
    ~executor();
    
    static executor* current();  // Thread-local accessor
    
    template <typename T, typename... Args>
    void spawn(Args&&... args);
    
    void register_event(int fd, short events, std::coroutine_handle<> h);
    void register_timer(std::chrono::steady_clock::time_point wake_time, 
                       std::coroutine_handle<> h);
    void ready_task(std::coroutine_handle<> h);
    void run();
    
private:
    std::unique_ptr<event_loop> event_loop_;
    std::list<coroutine> tasks_;
    std::multiset<timer_entry> timer_queue_;  // ✅ Timer wheel implemented
    std::vector<std::coroutine_handle<>> ready_queue_;
};
```

**Key Achievements**:
- Thread-local `current_executor` eliminates cross-thread pointer issues
- Timer wheel with efficient timeout handling
- Channel implementation with proper coroutine suspension/resumption
- Clean separation between scheduler and executor roles

## Proposed Full Architecture (Phase 3+)

### Design Philosophy

**Separation of Concerns**:
1. **Scheduler** → Manages threads and work distribution
2. **Executor** → Per-thread event loop (current role, needs multi-threading)
3. **Task** → Lightweight coroutine (unchanged at coroutine level)
4. **Service** → Supervised, long-running component
5. **Channel** → ✅ Already implemented - Type-safe, bounded queues with non-blocking send

### Core Components

#### 1. Thread-Per-Core Scheduler

```cpp
class scheduler {
public:
    scheduler(size_t num_threads = std::thread::hardware_concurrency());
    
    // Spawn a task on any available executor
    template<typename T, typename... Args>
    task_id spawn(Args&&... args);
    
    // Spawn on specific executor (for affinity)
    template<typename T, typename... Args>
    task_id spawn_on(executor_id exec_id, Args&&... args);
    
    // Register a long-running service
    template<typename T, typename... Args>
    service_handle register_service(Args&&... args);
    
    void run();      // Blocks until shutdown
    void shutdown(); // Graceful shutdown
    
private:
    std::vector<std::unique_ptr<executor>> executors_;
    std::atomic<bool> shutdown_requested_{false};
    
    // Work-stealing deques (one per executor)
    std::vector<work_stealing_deque<task_handle>> work_queues_;
    
    // Global queue for externally spawned tasks
    mpmc_queue<task_handle> global_queue_;
};
```

**Key Properties**:
- One `executor` per thread (pinned to CPU core)
- Work-stealing queues enable load balancing
- Global queue handles cross-thread spawns
- Shutdown signal propagates to all executors

#### 2. Executor (Per-Thread Event Loop)

```cpp
class executor {
public:
    explicit executor(executor_id id, scheduler* parent);
    
    // Run the event loop (called by scheduler thread)
    void run();
    
    // Submit task from another thread
    void submit(task_handle t);
    
    // Current executor (thread_local)
    static executor* current();
    
private:
    executor_id id_;
    scheduler* parent_;
    
    // Local ready queue (no locking needed)
    std::vector<std::coroutine_handle<>> ready_queue_;
    
    // I/O multiplexing fd (kqueue/epoll)
    platform_fd_type io_fd_;
    
    // Cross-thread submission queue (MPSC)
    mpsc_queue<task_handle> external_queue_;
    
    // Registered I/O events
    std::unordered_map<int, io_registration> io_events_;
    
    // Timer wheel for efficient timeout handling
    timer_wheel timers_;
    
    // Active tasks managed by this executor
    std::list<task_handle> tasks_;
    
    void process_ready_tasks();
    void process_io_events();
    void process_timers();
    void process_external_submissions();
    bool try_steal_work();
};
```

**Key Properties**:
- Each executor is single-threaded internally (no locks for local state)
- Cross-thread submissions use lock-free MPSC queue
- Work stealing pulls from other executors when idle
- Thread-local `current()` accessor avoids parameter passing

#### 3. Enhanced Task System

**Remove Raw Pointer**:
```cpp
class task {
protected:
    // OLD: scheduler* scheduler_ptr_;
    // NEW: Use thread-local executor
    
    template<typename T, typename... Args>
    void spawn(Args&&... args) {
        executor::current()->parent()->spawn<T>(std::forward<Args>(args)...);
    }
    
    auto yield() {
        return yield_awaiter{executor::current()};
    }
    
    auto readable(int fd) {
        return io_awaiter{fd, POLLIN, executor::current()};
    }
    
    // New: Spawn on current executor for locality
    template<typename T, typename... Args>
    void spawn_local(Args&&... args) {
        executor::current()->spawn<T>(std::forward<Args>(args)...);
    }
    
    // New: Sleep primitive
    auto sleep_for(std::chrono::milliseconds duration) {
        return sleep_awaiter{duration, executor::current()};
    }
};
```

**Awaiter Updates**:
```cpp
struct yield_awaiter {
    executor* exec_;
    
    bool await_ready() { return false; }
    
    void await_suspend(std::coroutine_handle<> h) {
        exec_->ready_task(h);
    }
    
    void await_resume() {}
};

struct io_awaiter {
    int fd_;
    short events_;
    executor* exec_;
    
    bool await_ready() { return false; }
    
    void await_suspend(std::coroutine_handle<> h) {
        exec_->register_io(fd_, events_, h);
    }
    
    short await_resume() { return events_; }
};

struct sleep_awaiter {
    std::chrono::milliseconds duration_;
    executor* exec_;
    
    bool await_ready() { return duration_.count() == 0; }
    
    void await_suspend(std::coroutine_handle<> h) {
        exec_->register_timer(duration_, h);
    }
    
    void await_resume() {}
};
```

#### 4. Service Abstraction

Services are supervised, restartable components with lifecycle hooks:

```cpp
class service {
public:
    virtual ~service() = default;
    
    // Lifecycle hooks
    virtual task_handle on_start() = 0;
    virtual task_handle on_stop() { co_return; }
    virtual task_handle on_restart() { co_return; }
    
    // Service metadata
    virtual const char* name() const = 0;
    virtual restart_policy policy() const { return restart_policy::permanent; }
};

enum class restart_policy {
    permanent,   // Always restart on failure
    transient,   // Restart only on abnormal termination
    temporary    // Never restart
};

class service_supervisor {
public:
    template<typename T, typename... Args>
    service_handle add_service(Args&&... args);
    
    void start_all();
    void stop_service(service_handle h);
    void stop_all();
    
private:
    struct supervised_service {
        std::unique_ptr<service> impl;
        service_handle handle;
        restart_policy policy;
        size_t restart_count;
        task_handle current_task;
    };
    
    std::vector<supervised_service> services_;
    executor* exec_;
};
```

**Example Service**:
```cpp
class http_server : public service {
public:
    http_server(uint16_t port) : port_(port) {}
    
    const char* name() const override { return "http_server"; }
    
    task_handle on_start() override {
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        listener_.bind(socket_address("0.0.0.0", port_));
        listener_.listen(128);
        listener_.nonblocking(true);
        
        while (true) {
            co_await readable(listener_);
            
            socket_address client_addr;
            auto client = listener_.accept(&client_addr);
            client.nonblocking(true);
            
            spawn_local<http_connection>(std::move(client));
        }
    }
    
    task_handle on_stop() override {
        listener_.close();
        co_return;
    }
    
private:
    uint16_t port_;
    socket listener_;
};
```

#### 5. Channel Implementation

Type-safe, bounded channels for inter-task communication:

**Note**: The channel is fully implemented with `try_send()`, `send()` (awaitable/blocking
with multiple waiting senders), `try_recv()`, `recv()` (awaitable), and `close()`.
Cancellation is integrated: suspended `recv()` and `send()` awaiters register with
the task control block and throw `dripcore::cancelled` on cancellation.

```cpp
template<typename T>
class channel {
public:
    explicit channel(size_t capacity);
    
    // Non-blocking send (returns false if full or closed)
    bool try_send(T value);
    
    // Awaitable/blocking send (suspends if full, resumes when space available)
    // Returns true if sent, false if channel closed.
    auto send(dripcore::task* task, T value);
    
    // Non-blocking receive (returns nullopt if empty)
    std::optional<T> try_recv();
    
    // Awaitable/blocking receive (suspends if empty, resumes when data available)
    // Returns nullopt if channel closed and buffer empty.
    auto recv(dripcore::task* task);
    
    // Lifecycle
    void close();  // wakes all waiting senders and receivers
    bool is_closed() const;
    size_t size() const;
    bool empty() const;
    
private:
    std::deque<T> buffer_;
    size_t capacity_;
    bool closed_{false};
    
    std::coroutine_handle<> waiting_receiver_ = nullptr;
    std::deque<send_awaiter*> waiting_senders_;
};
```

**Usage Example** (matches actual implementation):
```cpp
class producer : public dripcore::task {
public:
    producer(std::shared_ptr<dripcore::channel<int>> ch) : ch_(ch) {}
    
    dripcore::coroutine execute() {
        for (int i = 0; i < 100; ++i) {
            ch_->try_send(i);  // Non-blocking send
            co_await sleep_for(std::chrono::milliseconds(10));
        }
        ch_->close();
    }
    
private:
    std::shared_ptr<dripcore::channel<int>> ch_;
};

class consumer : public dripcore::task {
public:
    consumer(std::shared_ptr<dripcore::channel<int>> ch) : ch_(ch) {}
    
    dripcore::coroutine execute() {
        while (true) {
            auto val = co_await ch_->recv();  // Blocking receive
            if (!val) break;  // Channel closed
            process(*val);
        }
    }
    
private:
    std::shared_ptr<dripcore::channel<int>> ch_;
};
```

### Work Stealing Algorithm

**Per-Executor Deque**:
```
[Local Task Queue]
  Front (pop)  ←  executor pops from front
    ↓
  [T1][T2][T3][T4]
    ↑
  Back (push/steal)  ←  executor pushes to back, thieves steal from back
```

**Stealing Logic**:
```cpp
bool executor::try_steal_work() {
    // Try to steal from other executors
    for (size_t i = 1; i < parent_->executor_count(); ++i) {
        size_t victim_id = (id_ + i) % parent_->executor_count();
        auto& victim_queue = parent_->work_queue(victim_id);
        
        if (auto stolen = victim_queue.steal()) {
            tasks_.push_back(std::move(*stolen));
            return true;
        }
    }
    
    // Try global queue
    if (auto task = parent_->global_queue_.try_pop()) {
        tasks_.push_back(std::move(*task));
        return true;
    }
    
    return false;
}
```

**Load Balancing**:
- New tasks pushed to local queue back
- Executor pops from local queue front
- Idle executors steal from other queue backs
- Global queue catches overflow and external spawns

### Thread Safety Strategy

**Lock-Free Paths**:
- Executor local operations (no locks)
- Work-stealing deques (CAS-based)
- Channel operations (single mutex per channel)

**Synchronized Paths**:
- Cross-thread task submission (MPSC queue)
- Service supervisor (coarse-grained lock)
- Scheduler shutdown (atomic flag)

**Guarantees**:
- Tasks pinned to executor after first resume
- I/O operations always on same thread
- No lock contention on hot path (ready queue)

## Migration Path

### ✅ Phase 1: Refactor Current Scheduler - COMPLETED
- ✅ Rename `scheduler` → `executor` (conceptual separation achieved)
- ✅ Extract thread-local executor accessor (`executor::current()`)
- ✅ Update awaiters to use `executor::current()`
- ✅ Remove `scheduler_ptr_` from task (tasks now use thread-local executor)
- ✅ **Goal achieved**: Single-threaded but new architecture

### ✅ Phase 2: Introduce Scheduler Wrapper - COMPLETED
- ✅ Create new `scheduler` with single executor
- ✅ Implement work queues (ready_queue_ in executor)
- ✅ Test compatibility with existing code
- ✅ **Goal achieved**: New interfaces proven out

### ✅ Phase 2.5: Additional Features (Implemented Beyond Original Plan)
- ✅ **Channel Implementation**: Full `channel<T>` with `try_send()`, `send()` (awaitable), `try_recv()`, `recv()` (awaitable), close support
- ✅ **Timer Support**: Timer wheel in executor, `sleep_for()` via `sleep_awaiter`
- ✅ **Event System**: Cross-platform event notification (eventfd on Linux, self-pipe on macOS)
- ✅ **Cancellation & Shutdown**: `dripcore::cancelled`, `task_control` with `cancel()`/`stop()`/`cancelled()`/`exception()`, `scheduler::shutdown()` (see `doc/shutdown_and_cancellation_design.md`)
- ✅ **Task Base Class**: `execute()` → `body()`/`teardown()` wrapper with always-run async cleanup
- ✅ **Exception Propagation**: Non-cancelled exceptions transferred to `task_control_block`, observable via `task_control::exception()`
- ✅ **Awaitable Composition**: `dripcore::awaitable<T>` with symmetric transfer (see `doc/dripcore_continuations_proposal.md`)

### ⏳ Phase 3: Multi-Threading - NOT STARTED
- ❌ Spawn multiple executor threads
- ❌ Implement work-stealing deques
- ❌ Add cross-thread submission (MPSC queues)
- ❌ Add thread management and synchronization
- **Goal**: Utilize multiple cores

### ⏳ Phase 4: High-Level Abstractions - PARTIALLY COMPLETED
- ✅ Implement `channel<T>` - FULLY COMPLETED (with `send()`, `try_recv()`)
- ✅ Implement `task_control` with `cancel()`/`stop()`/`cancelled()`/`exception()` - FULLY COMPLETED
- ✅ Implement `scheduler::shutdown()` - FULLY COMPLETED
- ✅ Implement `awaitable<T>` composition - FULLY COMPLETED
- ❌ Implement `service` and `service_supervisor` - NOT STARTED
- ✅ Add timer wheel to executor - FULLY COMPLETED
- **Goal**: Production-ready building blocks

### ⏳ Phase 5: Optimization - NOT STARTED
- ❌ Profile lock contention
- ❌ Tune work-stealing parameters
- ❌ Add CPU affinity hints
- ❌ Implement backpressure mechanisms
- **Goal**: Production performance

## Updated Migration Path (From Current State)

Given that Phase 1 and 2 are complete, here's the **updated migration path** from the current state:

### Next: Phase 3 - Multi-Threading Implementation
1. **Multi-Executor Scheduler**: Modify `scheduler` to create and manage multiple `executor` instances
2. **Thread Management**: Create worker threads, each with its own executor
3. **Work Distribution**: Implement work-stealing deques for load balancing
4. **Cross-Thread Submission**: Add MPSC queues for task submission from any thread
5. **Synchronization**: Add necessary locks for shared state (global queue, shutdown signals)

### Then: Phase 4 - Service Abstraction
1. **Service Base Class**: Implement `service` with lifecycle hooks (`on_start`, `on_stop`, `on_restart`)
2. **Service Supervisor**: Implement `service_supervisor` for managing service lifecycle
3. **Restart Policies**: Implement different restart policies (permanent, transient, temporary)
4. **Integration**: Ensure services work seamlessly with multi-threaded scheduler

### Finally: Phase 5 - Optimization
1. **Performance Profiling**: Identify and address lock contention
2. **Work-Stealing Tuning**: Optimize stealing parameters and algorithms
3. **CPU Affinity**: Add CPU core pinning for executors
4. **Backpressure**: Implement mechanisms to handle overload gracefully

## Example Usage (Post-Migration)

```cpp
int main() {
    // Create scheduler with 4 worker threads
    dripcore::scheduler sched(4);
    
    // Create communication channels
    dripcore::channel<http_request> requests(100);
    dripcore::channel<http_response> responses(100);
    
    // Register services
    auto http = sched.register_service<http_server>(8080, requests);
    auto processor = sched.register_service<request_processor>(requests, responses);
    auto logger = sched.register_service<response_logger>(responses);
    
    // Start scheduler (blocks until shutdown)
    sched.run();
    
    return 0;
}
```

## Alternatives Considered

### Alternative 1: Thread Pool + Coroutines
**Description**: Use a general thread pool (e.g., `std::jthread` pool) with coroutines scheduled onto threads dynamically.

**Pros**: Simple, leverages std library
**Cons**: 
- Loses I/O multiplexing efficiency
- Context switching overhead
- No work stealing

**Verdict**: Not suitable for I/O-heavy workloads

### Alternative 2: Actor Model (Erlang-style)
**Description**: Tasks become actors with mailboxes, no shared state.

**Pros**: Natural for distributed systems, strong isolation
**Cons**:
- Requires full rewrite
- Message passing overhead for local communication
- Less flexible than coroutines

**Verdict**: Too divergent from current design

### Alternative 3: io_uring-Based (Linux only)
**Description**: Replace epoll with io_uring for kernel-level async I/O.

**Pros**: Cutting-edge performance on Linux
**Cons**:
- Linux-only (macOS still needs kqueue)
- Complex integration
- Kernel 5.1+ requirement

**Verdict**: Consider as future optimization, not architecture change

## Open Questions

1. **Task Priority**: Should tasks have priorities? How to integrate with work stealing?
2. **CPU Affinity**: Should services be pinned to specific cores?
3. **Backpressure**: How should channel send/recv handle overload?
4. **Error Handling**: How do service failures propagate to supervisor?
5. **Metrics**: What observability should be built-in (queue depths, task counts, etc.)?
6. **Cancellation**: ✅ **Resolved** — Fully implemented for single-executor. `task_control::cancel()` (fire-and-forget), `task_control::stop()` (cancel + co_await completion), `scheduler::shutdown()` (graceful + forced). See `doc/shutdown_and_cancellation_design.md`. Cross-thread `cancel()` deferred to Phase 3.

## Performance Expectations

### Microbenchmarks
- **Task spawn**: <100ns (current ~80ns, should stay similar)
- **Yield latency**: <1μs (currently ~0.5μs, may increase slightly with stealing)
- **I/O wakeup**: <10μs (platform dependent, should not regress)
- **Channel send/recv**: <200ns (new, target <1μs including contention)

### Scalability
- **Cores**: Linear speedup to 8 cores for CPU-bound tasks
- **Connections**: 100k+ concurrent connections (limited by FD count)
- **Throughput**: 1M+ tasks/sec on 8-core system

### Overheads
- **Memory**: +8KB per executor (I/O buffers, queues)
- **Threads**: N executor threads + main thread
- **Locking**: <5% time in locks under load

## Relationship with Continuations Proposal

The **dripcore_continuations_proposal.md** introduces `dripcore::awaitable<T>` for composable coroutine-based tasks. This proposal is **complementary** to the multithreading architecture and aligns well with the current implementation:

### ✅ Compatibility
- **Executor-Affinity**: The continuations proposal enforces executor-affinity, which aligns perfectly with the thread-local executor design
- **No Architecture Changes**: `awaitable<T>` requires no changes to the existing executor code (uses existing `ready_task()` method)
- **Building Blocks**: Awaitables are designed as composable helpers used within `body()` methods, fitting naturally with the task system

### 🔄 Integration Points
- **Multi-Threading Ready**: The executor-affinity enforcement in awaitables ensures they work correctly in a multi-threaded environment
- **Service Composition**: Services can use awaitables for complex async operations within their lifecycle methods
- **Channel Integration**: Awaitables can be used alongside channels for sophisticated communication patterns

### 📋 Implementation Order Recommendation
1. **Complete Phase 3** (Multi-threading) first - establishes the threading foundation
2. **Then implement continuations** - builds on the thread-local executor model
3. **Finally complete Phase 4** (Services) - can leverage both multi-threading and continuations

## Current Implementation Summary

### ✅ What's Done (Production-Ready)
- **Core Architecture**: Scheduler/Executor separation with thread-local access
- **Task System**: Clean coroutine-based tasks with `body()`/`teardown()` wrapper for always-run async cleanup
- **I/O Multiplexing**: Platform-abstracted event loops (kqueue/epoll) with ONESHOT semantics
- **Channels**: Full channel implementation with `try_send()`, `send()` (awaitable), `try_recv()`, `recv()` (awaitable), `close()`
- **Cancellation & Shutdown**: `task_control` with `cancel()`, `stop()`, `cancelled()`, `exception()`; `scheduler::shutdown()` with graceful drain + forced frame-destroy
- **Exception Propagation**: Non-cancelled exceptions transferred to `task_control_block`, observable via `task_control::exception()` after `done()`
- **Awaitable Composition**: `dripcore::awaitable<T>` with symmetric transfer for parent→child coroutine composition
- **Timers**: Efficient timer wheel with `sleep_for()` support
- **Memory Safety**: `shared_ptr` ownership model prevents premature destruction
- **Wake Notifications**: Event-based wakeup for efficient task scheduling

### 🚧 What's Next (Phase 3 Priority)
1. **Multi-Executor Support**: Enable `scheduler` to create and manage multiple executors
2. **Thread Pool**: Create worker threads with proper executor initialization
3. **Work Stealing**: Implement lock-free work-stealing deques for load balancing
4. **Cross-Thread Submission**: Add MPSC queues for safe task submission
5. **Synchronization**: Minimal locking for shared scheduler state

### 🎯 Benefits of Current State
Even without multi-threading, the current implementation provides significant value:
- **Clean Architecture**: Well-structured foundation for future multi-threading
- **Channel Support**: Enables sophisticated inter-task communication patterns
- **Timer Support**: Production-ready timer functionality
- **Thread Safety**: Thread-local design prevents cross-thread issues
- **Testability**: Single-threaded nature makes testing and debugging easier

## Conclusion

The multithreaded scheduler proposal has successfully completed **Phase 1 and Phase 2**, establishing a solid foundation for multi-core execution. The current implementation provides a clean, thread-local executor design that eliminates the raw scheduler pointer problem and enables sophisticated features like channels and timers.

**Remaining Work**: Phase 3 (Multi-threading) is the next priority, followed by Phase 4 (Service abstraction) and Phase 5 (Optimization). The **continuations proposal** (`awaitable<T>`) can be implemented in parallel with or after Phase 3, as it builds naturally on the existing thread-local executor model.

The migration path remains incremental, allowing validation at each phase. The final architecture will maintain zero-copy I/O, lock-free hot paths, and platform abstraction while adding the multi-core utilization and production-grade structure needed for serious applications.

**Key Achievement**: The foundation is now in place. The remaining work focuses on scaling the existing clean architecture to multiple cores rather than rearchitecting the core design.

## References

- **Tokio** (Rust): Work-stealing scheduler design
- **Go runtime**: Goroutine scheduling and channel implementation
- **Seastar** (C++): Thread-per-core architecture
- **Boost.Asio**: Proactor pattern for async I/O
- **C++20 Coroutines**: Lewis Baker's cppcoro library patterns
