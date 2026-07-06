---
title: Dripcore Continuation Support Proposal
tags: [dripcore, c++, coroutines, async, design, implementation, multithreading]
---

# Dripcore Continuation Support Proposal

## Summary

This document proposes adding **continuation support** to dripcore, enabling composable coroutine-based tasks that can `co_await` each other. This addresses the current limitation where `dripcore::coroutine` cannot be awaited, forcing all logic into monolithic `execute()` methods.

## Problem Statement

Currently in dripcore:

```cpp
class my_task : public dripcore::task {
public:
    dripcore::coroutine execute() {
        // All logic must be in this single function
        // Cannot extract helper coroutines
        // Cannot co_await other tasks

        // 200+ lines of complex logic...
    }
};
```

**Key Limitations:**
- `dripcore::coroutine` is a handle wrapper, not an awaitable type
- No mechanism to wait for task completion
- No way to compose smaller coroutines
- Forces monolithic task implementations

## Proposed Solution

### 1. New Awaitable Type: `dripcore::awaitable<T>`

Add a new `dripcore::awaitable<T>` template class that is **an awaitable building block** for use within `execute()` methods. **Continuations are stored directly in the promise type** for efficiency and simplicity:

```cpp
namespace dripcore {

template<typename T = void>
class awaitable {
public:
    // Awaitable interface
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> awaiting_coroutine);
    T await_resume();

    // Check if done
    bool done() const;

    // Promise type for coroutine support
    struct promise_type {
        std::shared_ptr<void> holder_ptr;
        T result_;
        std::exception_ptr exception_;

        // Continuations are stored here - no executor map needed
        std::vector<std::coroutine_handle<>> continuations_;

        // Thread safety: thread-affinity enforcement (see Multithreading section)
        std::thread::id creating_thread_;

        awaitable get_return_object();
        std::suspend_always initial_suspend() noexcept;
        std::suspend_always final_suspend() noexcept;
        void return_value(T value);
        void return_void();
        void unhandled_exception();
    };

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

// await_suspend: Register this coroutine as waiting on the awaitable
// Continuations are stored directly in the promise
void awaitable<T>::await_suspend(std::coroutine_handle<> awaiting) {
    if (!handle_.done()) {
        handle_.promise().continuations_.push_back(awaiting);
    }
}

// await_resume: Get result or throw exception
T awaitable<T>::await_resume() {
    if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
    }
    return handle_.promise().result_;
}
```

#### Promise Type

```cpp
struct awaitable<T>::promise_type {
    std::shared_ptr<void> holder_ptr;
    T result_;
    std::exception_ptr exception_;
    std::vector<std::coroutine_handle<>> continuations_;
    std::thread::id creating_thread_;

    promise_type() : creating_thread_(std::this_thread::get_id()) {}

    awaitable get_return_object() {
        return awaitable{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() noexcept { return {}; }

    std::suspend_always final_suspend() noexcept {
        // Notify all continuations directly from the promise
        for (auto h : continuations_) {
            if (h && !h.done()) {
                executor::current()->ready_task(h);
            }
        }
        continuations_.clear();
        return {};
    }

    void return_value(T value) { result_ = std::move(value); }
    void return_void() {}

    void unhandled_exception() {
        exception_ = std::current_exception();
    }
};
```

#### Constructor with Auto-Spawning

```cpp
// Constructor automatically schedules the coroutine
awaitable<T>::awaitable(std::coroutine_handle<promise_type> handle)
    : handle_(handle) {
    if (handle_ && !handle_.done()) {
        // Thread-affinity check (see Multithreading section)
        assert(std::this_thread::get_id() == handle_.promise().creating_thread_ &&
               "Awaitable must be created and awaited on the same thread");
        executor::current()->ready_task(handle_);
    }
}
```

#### Executor Modifications

**No changes needed!** The `ready_task` method is already public in `executor.h` (line 56), so the promise can call it directly:

```cpp
class executor {
public:
    // Already public - no changes needed
    void ready_task(std::coroutine_handle<> h);
};
```

## Usage Examples

### IMPORTANT: Awaitables are Building Blocks

**Awaitables are NOT standalone tasks** - they are **composable helpers** used *within* the `execute()` method of a `dripcore::task` subclass. You **must** create a task class to spawn and execute the logic.

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

    dripcore::coroutine execute() {
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

    dripcore::coroutine execute() {
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
    dripcore::coroutine execute() {
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

### Thread-Affinity for Awaitables

**Important: Awaitables must be created and awaited on the same thread.**

This is enforced in the `awaitable` constructor:

```cpp
void await_suspend(std::coroutine_handle<> awaiting) {
    // Debug-time thread-affinity check
    assert(std::this_thread::get_id() == handle_.promise().creating_thread_ &&
           "Awaitable must be created and awaited on the same thread");
    if (!handle_.done()) {
        handle_.promise().continuations_.push_back(awaiting);
    }
}
```

### Why Thread-Affinity?

| Model | Thread Safety | Complexity | Recommended for dripcore |
|-------|---------------|------------|---------------------------|
| **One executor per thread** | ✅ **Naturally safe** | Low | ✅ **YES** |
| **Shared executor across threads** | ❌ **Needs synchronization** | High | ❌ No |

**Benefits of Thread-Affinity:**
1. **No synchronization overhead**: The `continuations_` vector is only accessed from its creating thread
2. **Matches dripcore's architecture**: Thread-local executors mean coroutines naturally stay on their thread
3. **Clear semantics**: Awaitables are thread-local by design
4. **Zero runtime cost**: Assertions have no overhead in release builds

### What If Cross-Thread Support is Needed Later?

If dripcore ever needs cross-thread awaitable sharing, two approaches are possible:

**Option A: Runtime Thread-Affinity Check**
```cpp
void await_suspend(std::coroutine_handle<> awaiting) {
    if (std::this_thread::get_id() != handle_.promise().creating_thread_) {
        throw std::runtime_error("Cross-thread awaiting not supported");
    }
    if (!handle_.done()) {
        handle_.promise().continuations_.push_back(awaiting);
    }
}
```

**Option B: Full Synchronization** (not recommended)
```cpp
struct promise_type {
    std::mutex mutex_;
    std::vector<std::coroutine_handle<>> continuations_;

    void await_suspend(std::coroutine_handle<> awaiting) {
        std::lock_guard lock(mutex_);
        continuations_.push_back(awaiting);
    }

    std::suspend_always final_suspend() noexcept {
        std::vector<std::coroutine_handle<>> to_resume;
        {
            std::lock_guard lock(mutex_);
            to_resume = std::move(continuations_);
        }
        for (auto h : to_resume) {
            executor::current()->ready_task(h);
        }
        return {};
    }
};
```

**Recommendation:** Start with thread-affinity (Option A) and only add synchronization if cross-thread support becomes a requirement.

## Design Rationale

### Why Continuations in Promise?

**Initial Design (Rejected):**
- Used `std::unordered_map<std::coroutine_handle<>, std::vector<std::coroutine_handle<>>>` in executor
- Required hash table lookups
- Manual cleanup of map entries
- More complex executor code

**Improved Design (Current):**
- Continuations stored directly in the promise of the awaited awaitable
- **O(1)** access - no lookups needed
- Automatic cleanup when awaitable completes (vector cleared in `final_suspend`)
- **No executor changes** beyond making `ready_task` accessible (already public!)
- More logically sound: continuations are a property of the awaitable being awaited

### Benefits of Promise-Based Continuations

| Aspect | Map in Executor | Continuations in Promise |
|--------|-----------------|--------------------------|
| **Memory** | O(N) map entries + vector overhead | O(1) per awaited awaitable |
| **Lookup** | Hash table lookup (O(1) avg, O(N) worst) | Direct access (O(1)) |
| **Cleanup** | Manual in executor | Automatic with awaitable destruction |
| **Logical** | Global tracking | Continuations belong to awaitable |
| **Complexity** | ~50 lines in executor | ~0 lines in executor |
| **Cache Locality** | Poor (map scattered) | Excellent (with awaitable) |

### Why Auto-Spawning?

**Decision: NO spawn_async needed**

The awaitable constructor automatically schedules the coroutine via `executor::current()->ready_task(handle_)`. This means:

```cpp
// Inside execute() method:
co_await connect_task();  // Just works! Auto-spawned.
```

**Benefits:**
- Cleaner API (1 line vs 3)
- Less error-prone (can't forget to spawn)
- More intuitive (matches C++20 coroutine expectations)
- Consistent with dripcore's design (all awaitables run on executor threads)

## Integration with Existing Code

### Backward Compatibility

The existing `dripcore::task` base class and `dripcore::coroutine` type remain **completely unchanged**. The new `dripcore::awaitable<T>` is an **additional** type that provides awaitable behavior.

Existing code continues to work:

```cpp
// Old style still works perfectly
class old_task : public dripcore::task {
public:
    dripcore::coroutine execute() {
        // ... existing code ...
        co_await readable(fd);
        // ... more existing code ...
    }
};
```

### File Structure

New file to be added:
```
third_party/dripcore/lib/dripcore/include/dripcore/awaitable.h
```

No modifications needed to existing files (except potentially adding the new header to CMakeLists.txt).

## Benefits

1. **Composability**: Tasks can be broken into smaller, reusable components
2. **Readability**: Logic flows linearly with clear `co_await` points
3. **Error Handling**: Natural try/catch support for async operations
4. **Standard Compliance**: Follows C++20 coroutine best practices
5. **Backward Compatible**: Existing code continues to work unchanged
6. **Minimal Overhead**: Continuations stored with awaitable, no extra allocations
7. **Efficient**: No hash table lookups, direct vector access
8. **Simple API**: No spawn_async needed, just co_await
9. **Thread-Safe by Design**: Thread-affinity prevents cross-thread issues

## Implementation Status

- [ ] Define `dripcore::awaitable<T>` template class in `include/dripcore/awaitable.h`
- [ ] Implement promise type with continuation vector
- [ ] Implement auto-spawning in constructor
- [ ] Add thread-affinity checks (assertions)
- [ ] Write unit tests
- [ ] Update documentation

## Testing Strategy

1. **Basic composition**: Awaitable A awaits Awaitable B
2. **Multiple continuations**: Multiple coroutines waiting on one awaitable (same thread)
3. **Error propagation**: Exceptions thrown in awaited awaitables
4. **Value returning**: Awaitables that return values of various types
5. **Nested awaiting**: Awaitable A awaits Awaitable B which awaits Awaitable C
6. **Backward compatibility**: Existing tasks still work
7. **Thread-affinity**: Verify awaitables work correctly within single thread
8. **Stress test**: Many awaitables with many continuations

## Performance Considerations

- **Continuation storage**: One `std::vector` per awaited awaitable (only when awaited)
- **Memory**: ~24 bytes per continuation (coroutine_handle is typically 8-16 bytes)
- **Time**: O(1) to add continuation, O(N) to notify all (N = number of continuations)
- **No overhead** for non-awaited awaitables
- **Cache friendly**: Continuations stored with the awaitable they await
- **No synchronization overhead**: Thread-affinity ensures single-threaded access

## Related Work

This design is inspired by:
- [cppcoro](https://github.com/lewissbaker/cppcoro) task composition
- [std::coroutine](https://en.cppreference.com/w/cpp/language/coroutines) standard
- [Boost.Asio](https://www.boost.org/doc/libs/develop/libs/asio/) composed operations

## Conclusion

Adding continuation support to dripcore via `dripcore::awaitable<T>` would enable modern, composable coroutine-based programming while maintaining full backward compatibility. The **improved design with continuations in the promise type** and **auto-spawning in the constructor** is more efficient, simpler, and more elegant than alternative approaches. The implementation is focused (~150 lines), with significant benefits for code maintainability and expressiveness.

**Key Decisions:**
- Use `dripcore::awaitable<T>` (not `task<T>` to avoid naming conflict with existing `dripcore::task`)
- Store continuations in promise type (not in executor map)
- Auto-spawn in constructor (no spawn_async needed)
- No changes to existing executor code (ready_task already public)
- **Enforce thread-affinity** to ensure thread safety without synchronization overhead
- **Awaitables are building blocks** used within `execute()` methods of `dripcore::task` subclasses

## Summary of Changes from Original Proposal

1. **Renamed `task<T>` → `awaitable<T>`** to avoid naming conflict with existing `dripcore::task`
2. **Clarified usage pattern**: Awaitables are helpers used within `execute()` methods, not standalone spawnable tasks
3. **Added thread-affinity enforcement** for multithreading safety
4. **Added multithreading section** explaining the design decisions
5. **Corrected all examples** to show proper task/awaitable usage pattern
