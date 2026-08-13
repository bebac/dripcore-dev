#ifndef DRIPCORE_AWAITABLE_H
#define DRIPCORE_AWAITABLE_H

#include <dripcore/executor.h>

#include <coroutine>
#include <vector>
#include <exception>
#include <memory>
#include <cassert>
#include <type_traits>

namespace dripcore
{
  // Forward declarations
  template<typename T = void>
  class awaitable;

  template<typename T>
  struct awaitable_promise;

  // Primary template for awaitable - defined first
  template<typename T>
  class awaitable final
  {
  public:
    // Awaitable interface
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> awaiting_coroutine);
    auto await_resume();
    bool done() const;

    // Promise type for coroutine support
    using promise_type = awaitable_promise<T>;

    // Ownership: the awaitable owns its child coroutine frame. The destructor
    // destroys it, fixing the leak where final_suspend (suspend_always) keeps
    // the frame alive forever. See doc/shutdown_and_cancellation_design.md §7.
    ~awaitable();

    // Move-only: the handle is transferred, source is nullified.
    awaitable(awaitable&& other) noexcept;
    awaitable(const awaitable&) = delete;
    awaitable& operator=(const awaitable&) = delete;

  private:
    explicit awaitable(std::coroutine_handle<promise_type> handle);
    std::coroutine_handle<promise_type> handle_;

    // Friend declaration for promise_type to access private constructor
    friend struct awaitable_promise<T>;
  };

  // Base promise type with common functionality
  template<typename T>
  struct awaitable_promise_base
  {
    std::shared_ptr<void> holder_ptr;
    std::exception_ptr exception_;
    std::vector<std::coroutine_handle<>> continuations_;

    // Executor-affinity (not thread-affinity): the executor on which this
    // awaitable was created. Under the executor-pinning model a task never
    // migrates executors after first resume, so the awaitable is only ever
    // awaited on this same executor. Binding to the executor (rather than the
    // OS thread) is what permits work-stealing of not-yet-started tasks while
    // keeping continuations_ single-threaded and lock-free.
    // See doc/shutdown_and_cancellation_design.md (Design Principle 3).
    executor* creating_executor_;

    awaitable_promise_base() : creating_executor_(executor::current()) {}

    std::suspend_always initial_suspend() noexcept { return {}; }

    std::suspend_always final_suspend() noexcept
    {
      // Notify all continuations directly from the promise.
      // Intentionally use executor::current() (not creating_executor_): under
      // executor-pinning the two are equal, and routing through current()
      // avoids manufacturing a cross-executor scheduling path.
      for (auto h : continuations_)
      {
        if (h && !h.done())
        {
          executor::current()->ready_task(h);
        }
      }
      continuations_.clear();
      return {};
    }

    void unhandled_exception()
    {
      exception_ = std::current_exception();
    }
  };

  // Promise type for non-void awaitables
  template<typename T>
  struct awaitable_promise final : awaitable_promise_base<T>
  {
    T result_;

    awaitable<T> get_return_object()
    {
      return awaitable<T>{std::coroutine_handle<awaitable_promise>::from_promise(*this)};
    }

    void return_value(T value)
    {
      this->result_ = std::move(value);
    }
  };

  // Promise type for void awaitables
  template<> 
  struct awaitable_promise<void> final : awaitable_promise_base<void>
  {
    awaitable<void> get_return_object()
    {
      return awaitable<void>{std::coroutine_handle<awaitable_promise>::from_promise(*this)};
    }

    void return_void() {}
  };

  // ============================================================================
  // Implementation of awaitable methods
  // ============================================================================

  template<typename T>
  bool awaitable<T>::await_ready() const noexcept
  {
    return handle_.done();
  }

  template<typename T>
  void awaitable<T>::await_suspend(std::coroutine_handle<> awaiting)
  {
    // Executor-affinity check - MUST be awaited on the executor it was created on.
    assert(executor::current() == handle_.promise().creating_executor_ &&
           "Awaitable must be created and awaited on the same executor");

    if (!handle_.done())
    {
      handle_.promise().continuations_.push_back(awaiting);
    }
  }

  template<typename T>
  auto awaitable<T>::await_resume()
  {
    if (handle_.promise().exception_)
    {
      std::rethrow_exception(handle_.promise().exception_);
    }
    if constexpr (!std::is_void_v<T>)
    {
      return std::move(handle_.promise().result_);
    }
  }

  template<typename T>
  bool awaitable<T>::done() const
  {
    return handle_.done();
  }

  // Constructor with auto-spawning
  template<typename T>
  awaitable<T>::awaitable(std::coroutine_handle<promise_type> handle)
      : handle_(handle)
  {
    if (handle_ && !handle_.done())
    {
      // Executor-affinity check
      assert(executor::current() == handle_.promise().creating_executor_ &&
             "Awaitable must be created and awaited on the same executor");

      // Auto-spawn: Schedule the coroutine on the current executor
      executor::current()->ready_task(handle_);
    }
  }

  // Destructor: destroy the child frame. In normal operation the awaitable is
  // always co_awaited, so the child has reached final_suspend by the time this
  // runs. During forced shutdown the frame may be suspended at an earlier
  // point; destroy() runs in-scope destructors (leaf awaiter deregistration,
  // RAII cleanup) without resuming. See §7.
  template<typename T>
  awaitable<T>::~awaitable()
  {
    if (handle_)
    {
      handle_.destroy();
    }
  }

  // Move constructor: transfer ownership of the frame.
  template<typename T>
  awaitable<T>::awaitable(awaitable&& other) noexcept
      : handle_(other.handle_)
  {
    other.handle_ = nullptr;
  }

} // namespace dripcore

#endif // DRIPCORE_AWAITABLE_H
