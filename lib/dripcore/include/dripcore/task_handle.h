#ifndef DRIPCORE_TASK_HANDLE_H
#define DRIPCORE_TASK_HANDLE_H

#include <dripcore/yield_awaiter.h>
#include <dripcore/io_awaiter.h>

#include <coroutine>
#include <memory>
#include <iostream>
#include <exception>

#include <poll.h>

namespace dripcore
{
  class executor;

#if defined(__linux__)
  using platform_fd_type = int;
#elif defined(__APPLE__)
  using platform_fd_type = int;
#endif

  class task_handle
  {
    friend class executor;
  public:
    struct promise_type
    {
      std::shared_ptr<void> holder_ptr;
      std::exception_ptr exception_;

      task_handle get_return_object() {
        return task_handle{std::coroutine_handle<promise_type>::from_promise(*this)};
      }
      std::suspend_always initial_suspend() noexcept { return {}; }
      std::suspend_always final_suspend() noexcept { return {}; }
      void return_void() {}
      void unhandled_exception() {
        // Capture exception instead of terminating.
        // This enables cancellation (dripcore::cancelled) and error handling.
        // See doc/shutdown_and_cancellation_design.md §4.
        exception_ = std::current_exception();
      }
    };
  public:
    // Constructors.
    task_handle();
    explicit task_handle(std::coroutine_handle<promise_type> handle);
    // Destructors.
    virtual ~task_handle();
  public:
    // Copy Constructors.
    task_handle(const task_handle &) = delete;
    task_handle &operator=(const task_handle &) = delete;
    task_handle(task_handle &&other) noexcept;
  public:
    bool done() const;
  private:
    void set_holder(std::shared_ptr<void> ptr);
  private:
    std::coroutine_handle<promise_type> handle_;
  };
}

#endif // DRIPCORE_COROUTINE_H
