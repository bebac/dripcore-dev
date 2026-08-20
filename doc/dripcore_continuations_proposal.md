---
title: Dripcore Continuation Support Proposal
tags: [dripcore, c++, coroutines, async, design, implementation, multithreading]
---

# Dripcore Continuation Support Proposal

## Summary

This document proposes adding **continuation support** to dripcore, enabling composable coroutine-based tasks that can `co_await` each other. This addresses the current limitation where `dripcore::task_handle` cannot be awaited, forcing all logic into monolithic `body()` methods.

## Problem Statement

Currently in dripcore:

```cpp
class my_task : public dripcore::task {
public:
    dripcore::awaitable<> body() override {
        // All logic must be in this single function
        // Cannot extract helper coroutines
        // Cannot co_await other tasks

        // 200+ lines of complex logic...
    }
};
```

**Key Limitations:**
- `dripcore::task_handle` is a handle wrapper, not an awaitable type
- No mechanism to wait for task completion
- No way to compose smaller coroutines
- Forces monolithic task implementations

## Solution (Implemented)

### 1. Awaitable Type: `dripcore::awaitable<T>`

`dripcore::awaitable<T>` is an **awaitable building block** for use within
`body()` methods of `dripcore::task` subclasses. It uses **symmetric transfer**
for parent→child composition and stores a **single continuation** in the
promise — see §Design Evolution below for why the original multi-continuation
vector was replaced.

```cpp
namespace dripcore {

template<typename T = void>
class awaitable {
public:
    // Awaitable interface
    bool await_ready() const noexcept;
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting_coroutine);
    auto await_resume();
    bool done() const;

    // Promise type for coroutine support
    using promise_type = awaitable_promise<T>;

    // Move-only: the handle is transferred, source is nullified.
    ~awaitable();
    awaitable(awaitable&& other) noexcept;
    awaitable(const awaitable&) = delete;
    awaitable& operator=(const awaitable&) = delete;

private:
    explicit awaitable(std::coroutine_handle<promise_type> handle);
    std::coroutine_handle<promise_type> handle_;
};

} // namespace dripcore
```

### 2. Implementation Details

#### Awaitable Interface

```cpp
// await_ready: Task is complete if coroutine handle is done
bool awaitable<T>::await_ready() const noexcept {
    return handle_.done();
}

// await_suspend: Store the continuation and symmetric-transfer to the child.
// The child runs immediately; when it completes, final_suspend schedules
// the parent (continuation_) on the ready queue.
std::coroutine_handle<> awaitable<T>::await_suspend(std::coroutine_handle<> awaiting) {
    assert(executor::current() == handle_.promise().creating_executor_ &&
           "Awaitable must be created and awaited on the same executor");
    handle_.promise().continuation_ = awaiting;
    return handle_;  // symmetric transfer: child runs immediately
}

// await_resume: Get result or throw exception
auto awaitable<T>::await_resume() {
    if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
    }
    if constexpr (!std::is_void_v<T>) {
        return std::move(handle_.promise().result_);
    }
}
```

#### Promise Type

```cpp
template<typename T>
struct awaitable_promise_base {
    std::shared_ptr<void> holder_ptr;
    std::exception_ptr exception_;
    std::coroutine_handle<> continuation_;  // single continuation
    executor* creating_executor_;

    awaitable_promise_base() : creating_executor_(executor::current()) {}

    std::suspend_always initial_suspend() noexcept { return {}; }

    // Custom final_suspend: schedule the continuation on the ready queue
    // and suspend. We cannot use symmetric transfer here because the
    // parent's ~awaitable() would destroy this (child) frame while
    // child.resume() is still on the call stack — a use-after-free.
    struct final_awaiter {
        awaitable_promise_base* promise;
        bool await_ready() noexcept { return false; }
        void await_suspend(std::coroutine_handle<>) noexcept {
            if (promise->continuation_) {
                executor::current()->ready_task(promise->continuation_);
                promise->continuation_ = {};
            }
        }
        void await_resume() noexcept {}
    };

    final_awaiter final_suspend() noexcept { return {this}; }

    void unhandled_exception() {
        exception_ = std::current_exception();
    }
};
```

#### Constructor — No Auto-Spawning

```cpp
// The child is resumed via symmetric transfer when the caller co_awaits
// this awaitable (await_suspend returns handle_). This avoids a
// ready-queue round-trip for the common case where the child completes
// without suspending.
awaitable<T>::awaitable(std::coroutine_handle<promise_type> handle)
    : handle_(handle) {
    if (handle_ && !handle_.done()) {
        assert(executor::current() == handle_.promise().creating_executor_ &&
               "Awaitable must be created and awaited on the same executor");
    }
}
```

#### Executor Modifications

**No changes needed!** The `ready_task` method is already public in `executor.h`, so the promise can call it directly:

```cpp
class executor {
public:
    void ready_task(std::coroutine_handle<> h);
};
```

## Usage Examples

### IMPORTANT: Awaitables are Building Blocks

**Awaitables are NOT standalone tasks** - they are **composable helpers** used *within* the `body()` method of a `dripcore::task` subclass. You **must** create a task class to spawn and execute the logic.

### Basic Task Composition

```cpp
// Helper awaitable - a reusable building block
dripcore::awaitable<> connect_task() {
    client_.reset();
    client_.connect(host_, port_);
    client_.set_non_blocking(true);
    connected_ = true;
    co_return;
}

// Another helper awaitable
dripcore::awaitable<> send_request_task(const api::api_request& req) {
    auto stream = open_stream(client_, req);
    init_request(stream, req);

    while (true) {
        auto out = client_.output_begin();
        if (out.empty()) break;

        int n = client_.write(out);
        if (n > 0) {
            client_.output_end(n);
        } else {
            int err = client_.get_error(n);
            if (err == SSL_ERROR_WANT_WRITE) {
                co_await writable(client_.fd());
            } else if (err == SSL_ERROR_WANT_READ) {
                co_await readable(client_.fd());
            }
        }
    }
    co_return;
}

// Task class that USES the awaitables
class network_task : public dripcore::task {
public:
    network_task(std::string host, int port)
        : host_(std::move(host)), port_(port) {}

    dripcore::awaitable<> body() override {
        while (auto req = co_await request_ch_->recv()) {
            if (!req) break;

            // Clean, composable logic - awaitables are helpers
            co_await connect_task();
            co_await send_request_task(*req);
            co_await receive_response_task();

            response_ch_->try_send({api::event_type::done, {}});
        }
    }

private:
    std::string host_;
    int port_;
    std::shared_ptr<dripcore::channel<api::api_request>> request_ch_;
    std::shared_ptr<dripcore::channel<api::api_event>> response_ch_;
};

// Spawning the task (traditional dripcore way)
executor::current()->spawn<network_task>("api.example.com", 443);
```

### Returning Values

```cpp
// Helper awaitable that returns a value
dripcore::awaitable<std::string> fetch_data(int fd) {
    co_await readable(fd);
    std::string result = read_all(fd);
    co_return result;
}

// Task class that uses the helper
class data_fetcher_task : public dripcore::task {
public:
    data_fetcher_task(int fd) : fd_(fd) {}

    dripcore::awaitable<> body() override {
        try {
            std::string data = co_await fetch_data(fd_);
            std::cout << "Got: " << data << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

private:
    int fd_;
};

// Spawn it
executor::current()->spawn<data_fetcher_task>(socket_fd);
```

### Error Handling

```cpp
// Helper awaitable that can fail
dripcore::awaitable<int> parse_response() {
    if (response_.status_code >= 400) {
        throw std::runtime_error("HTTP error: " + std::to_string(response_.status_code));
    }
    co_return response_.status_code;
}

// Task class with error handling
class api_task : public dripcore::task {
public:
    dripcore::awaitable<> body() override {
        try {
            int status = co_await parse_response();
            std::cout << "Status: " << status << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }
};

// Spawn it
executor::current()->spawn<api_task>();
```

## Multithreading Considerations

### Current dripcore Architecture

Dripcore uses **thread-local executors**:

```cpp
// From executor.cpp line 8
thread_local executor* current_executor = nullptr;
```

This means:
- Each thread has its own `executor` instance
- Coroutines are tied to their creating thread's executor
- `executor::current()` always returns the thread's own executor

### Executor-Affinity for Awaitables

**Important: Awaitables must be created and awaited on the same executor.**

This is enforced via debug assertions in `await_suspend` and the constructor:

```cpp
std::coroutine_handle<> awaitable<T>::await_suspend(std::coroutine_handle<> awaiting) {
    // Debug-time executor-affinity check
    assert(executor::current() == handle_.promise().creating_executor_ &&
           "Awaitable must be created and awaited on the same executor");
    handle_.promise().continuation_ = awaiting;
    return handle_;  // symmetric transfer
}
```

### Why Executor-Affinity?

| Model | Thread Safety | Complexity | Recommended for dripcore |
|-------|---------------|------------|---------------------------|
| **One executor per thread** | ✅ **Naturally safe** | Low | ✅ **YES** |
| **Shared executor across threads** | ❌ **Needs synchronization** | High | ❌ No |

**Benefits of Executor-Affinity:**
1. **No synchronization overhead**: The `continuation_` handle is only accessed from its creating executor
2. **Matches dripcore's architecture**: Thread-local executors mean coroutines naturally stay on their executor
3. **Clear semantics**: Awaitables are executor-local by design
4. **Zero runtime cost**: Assertions have no overhead in release builds
5. **Permits work-stealing**: Binding to the executor (not the OS thread) allows work-stealing of not-yet-started tasks while keeping `continuation_` single-threaded

### What If Cross-Thread Support is Needed Later?

If dripcore ever needs cross-thread awaitable sharing, synchronization would
be required on the continuation mechanism. See §Design Evolution below for
the `shared_awaitable<T>` concept, which would be the appropriate place to add
such synchronization rather than complicating the single-continuation `awaitable<T>`.

## Design Rationale

### Why Continuations in Promise?

**Initial Design (Rejected):**
- Used `std::unordered_map<std::coroutine_handle<>, std::vector<std::coroutine_handle<>>>` in executor
- Required hash table lookups
- Manual cleanup of map entries
- More complex executor code

**Improved Design:**
- Continuations stored directly in the promise of the awaited awaitable
- **O(1)** access - no lookups needed
- **No executor changes** beyond making `ready_task` accessible (already public!)
- More logically sound: continuations are a property of the awaitable being awaited

### Design Evolution: Multi-Continuation Vector → Single Continuation

#### Original Design (Vector-Based)

The original implementation stored a `std::vector<std::coroutine_handle<>>`
in the promise to support **multiple awaiters on a single awaitable** — the
"shared awaitable" pattern, analogous to `std::shared_future` or cppcoro's
`shared_task`:

```cpp
// Original: multiple coroutines could await the same awaitable
auto shared_work = slow_computation();

// Task A
co_await shared_work;  // registers continuation A

// Task B (elsewhere, same executor)
co_await shared_work;  // registers continuation B

// When slow_computation completes, both A and B are resumed
```

The constructor auto-spawned the child on the ready queue, and `final_suspend`
iterated over the vector to schedule all continuations.

#### Problem: Performance Overhead for the Common Case

In practice, every usage of `awaitable<T>` follows the **parent→child
composition** pattern — an awaitable is constructed as a temporary and
immediately `co_await`ed:

```cpp
co_await helper_coroutine();  // construct + await in one expression
```

No code ever shared a single awaitable across multiple awaiters. The
"multiple continuations" test listed in the testing strategy was never
written. The vector was an unused feature that imposed real costs:

| Cost | Vector (old) | Single handle (new) |
|-----|-------------|---------------------|
| **Heap allocation** | `std::vector` may allocate on first `push_back` | None |
| **Ready-queue round-trip** | Constructor auto-spawns → child runs later via queue | Symmetric transfer → child runs immediately |
| **Cache locality** | Vector indirection | Inline `coroutine_handle<>` in promise |
| **Memory** | 24+ bytes (vector overhead) per awaited awaitable | 8 bytes (one handle) |

#### Current Design (Single Continuation + Symmetric Transfer)

The current implementation replaces the vector with a single
`std::coroutine_handle<> continuation_` and uses **symmetric transfer**:

- `await_suspend` stores the parent's handle in `continuation_` and returns
  the child's handle. The compiler performs a tail-call to resume the child
  directly — no ready-queue round-trip, no stack growth.
- `final_suspend` schedules `continuation_` on the ready queue (not via
  symmetric transfer) to avoid a use-after-free: if the parent resumed
  immediately via symmetric transfer, `~awaitable()` would destroy the
  child frame while `child.resume()` is still on the call stack.
- The constructor no longer auto-spawns. The child is resumed only when
  the caller `co_awaits` the awaitable.

**Limitation:** A single awaitable can only be awaited by one coroutine.
If a second coroutine awaits an already-awaited awaitable, it silently
overwrites the first continuation — the first awaiter would never resume.
This is acceptable because `awaitable<T>` is move-only and consumed by
`co_await`, making the multi-awaiter pattern structurally impossible in
normal usage.

#### Future: `shared_awaitable<T>`

If the multi-awaiter pattern is ever needed, a separate `shared_awaitable<T>`
type should be added rather than complicating `awaitable<T>`:

```cpp
auto shared_work = make_shared_awaitable(slow_computation());

// Task A
co_await shared_work;  // registers continuation A

// Task B
co_await shared_work;  // registers continuation B

// When slow_computation completes, both A and B are resumed
```

| Aspect | `awaitable<T>` (current) | `shared_awaitable<T>` (hypothetical) |
|--------|--------------------------|--------------------------------------|
| **Continuations** | Single handle (no allocation) | Vector (heap-allocated) |
| **Transfer** | Symmetric transfer (tail-call) | Ready-queue scheduling only |
| **Ownership** | Move-only, consumed by `co_await` | Shareable (ref-counted or cloned) |
| **`final_suspend`** | Schedule one continuation | Iterate and schedule all |
| **Use case** | Parent→child composition | Multiple consumers of one result |

Separate types let each be optimal for its use case. The existing
`awaitable_promise_base` already isolates the continuation mechanism, so a
`shared_awaitable` could share the executor-affinity, exception, and result
machinery while swapping in a different continuation strategy.

### Why No Auto-Spawning?

The constructor does **not** schedule the child on the ready queue. The child
is resumed via symmetric transfer when the caller `co_awaits` the awaitable
(`await_suspend` returns the child's handle). This avoids a ready-queue
round-trip for the common case where the child completes without suspending:

```cpp
// Inside body() method:
co_await connect_task();  // child runs immediately via symmetric transfer
```

**Benefits:**
- Eliminates one ready-queue round-trip per composition
- Cleaner semantics: the child doesn't run until awaited
- Matches C++20 coroutine expectations (lazy coroutines)

## Integration with Existing Code

### Backward Compatibility

The existing `dripcore::task` base class and `dripcore::task_handle` type remain **completely unchanged**. The new `dripcore::awaitable<T>` is an **additional** type that provides awaitable behavior.

Existing code continues to work:

```cpp
// Old style still works perfectly
class old_task : public dripcore::task {
public:
    dripcore::awaitable<> body() override {
        // ... existing code ...
        co_await readable(fd);
        // ... more existing code ...
    }
};
```

### File Structure

```
lib/dripcore/include/dripcore/awaitable.h
```

## Benefits

1. **Composability**: Tasks can be broken into smaller, reusable components
2. **Readability**: Logic flows linearly with clear `co_await` points
3. **Error Handling**: Natural try/catch support for async operations
4. **Standard Compliance**: Follows C++20 coroutine best practices
5. **Backward Compatible**: Existing code continues to work unchanged
6. **Minimal Overhead**: Single continuation handle, no heap allocation
7. **Efficient**: Symmetric transfer avoids ready-queue round-trips
8. **Simple API**: No spawn_async needed, just co_await
9. **Executor-Safe by Design**: Executor-affinity prevents cross-executor issues

## Implementation Status

- [x] Define `dripcore::awaitable<T>` template class in `include/dripcore/awaitable.h`
- [x] Implement promise type with single continuation handle
- [x] Implement symmetric transfer in `await_suspend`
- [x] Implement custom `final_suspend` with ready-queue scheduling
- [x] Add executor-affinity checks (assertions)
- [x] Implement destructor for child-frame ownership
- [x] Write unit tests
- [x] Update documentation

## Testing Strategy

1. **Basic composition**: Awaitable A awaits Awaitable B ✅
2. **Error propagation**: Exceptions thrown in awaited awaitables ✅
3. **Value returning**: Awaitables that return values of various types ✅
4. **Nested awaiting**: Awaitable A awaits Awaitable B which awaits Awaitable C ✅
5. **Backward compatibility**: Existing tasks still work ✅
6. **Executor-affinity**: Verify awaitables work correctly within single executor ✅
7. **Stress test**: Many tasks with awaitables ✅
8. **Multiple continuations**: Removed — see §Design Evolution (would require `shared_awaitable<T>`)

## Performance Considerations

- **Continuation storage**: One `std::coroutine_handle<>` per awaited awaitable (8 bytes, no allocation)
- **Symmetric transfer**: `await_suspend` returns child handle → tail-call resume, no ready-queue round-trip
- **Final suspend**: Schedules parent continuation on ready queue (avoids use-after-free from symmetric transfer back to parent)
- **No overhead** for non-awaited awaitables (constructor does nothing)
- **No synchronization overhead**: Executor-affinity ensures single-executor access

## Related Work

This design is inspired by:
- [cppcoro](https://github.com/lewissbaker/cppcoro) task composition
- [std::coroutine](https://en.cppreference.com/w/cpp/language/coroutines) standard
- [Boost.Asio](https://www.boost.org/doc/libs/develop/libs/asio/) composed operations
- [Lewis Baker's "C++ Coroutines: Understanding Symmetric Transfer"](https://lewissbaker.github.io/2020/05/11/understanding_symmetric_transfer)

## Conclusion

`dripcore::awaitable<T>` enables modern, composable coroutine-based
programming while maintaining full backward compatibility. The design uses
**symmetric transfer** for zero-overhead parent→child composition and a
**single continuation** per awaitable, avoiding the heap allocation and
ready-queue round-trips of the original vector-based approach. If multi-awaiter
semantics are ever needed, a separate `shared_awaitable<T>` type can be added
without modifying `awaitable<T>`.

**Key Decisions:**
- Use `dripcore::awaitable<T>` (not `task<T>` to avoid naming conflict with existing `dripcore::task`)
- Store single continuation in promise type (not vector, not in executor map)
- Symmetric transfer in `await_suspend` (no auto-spawn in constructor)
- Custom `final_suspend` schedules continuation via ready queue (avoids use-after-free)
- No changes to existing executor code (ready_task already public)
- **Enforce executor-affinity** to ensure thread safety without synchronization overhead
- **Awaitables are building blocks** used within `body()` methods of `dripcore::task` subclasses

## Summary of Changes from Original Proposal

1. **Renamed `task<T>` → `awaitable<T>`** to avoid naming conflict with existing `dripcore::task`
2. **Clarified usage pattern**: Awaitables are helpers used within `body()` methods, not standalone spawnable tasks
3. **Added executor-affinity enforcement** for multithreading safety
4. **Added multithreading section** explaining the design decisions
5. **Corrected all examples** to show proper task/awaitable usage pattern
6. **Replaced vector-based continuations with single continuation + symmetric transfer** for performance (see §Design Evolution)
7. **Removed auto-spawning** from constructor — child resumes via symmetric transfer on `co_await`
8. **Added destructor** for child-frame ownership (fixes latent leak)