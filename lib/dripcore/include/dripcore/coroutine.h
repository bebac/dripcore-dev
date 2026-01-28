#ifndef DRIPCORE_COROUTINE_H
#define DRIPCORE_COROUTINE_H

#include <dripcore/yield_awaiter.h>
#include <dripcore/io_awaiter.h>

#include <coroutine>
#include <memory>
#include <iostream>

#include <poll.h>

namespace dripcore
{
  class executor;

#if defined(__linux__)
  using platform_fd_type = int;
#elif defined(__APPLE__)
  using platform_fd_type = int;
#endif

  class coroutine
  {
    friend class executor;
  public:
    struct promise_type
    {
      std::shared_ptr<void> holder_ptr;
      coroutine get_return_object() {
        return coroutine{std::coroutine_handle<promise_type>::from_promise(*this)};
      }
      std::suspend_always initial_suspend() noexcept { return {}; }
      std::suspend_always final_suspend() noexcept { return {}; }
      void return_void() {}
      void unhandled_exception() { std::terminate(); }
    };
  public:
    // Constructors.
    coroutine();
    explicit coroutine(std::coroutine_handle<promise_type> handle);
    // Destructors.
    virtual ~coroutine();
  public:
    // Copy Constructors.
    coroutine(const coroutine &) = delete;
    coroutine &operator=(const coroutine &) = delete;
    coroutine(coroutine &&other) noexcept;
  public:
    bool is_done() const;
  private:
    void set_holder(std::shared_ptr<void> ptr);
  private:
    std::coroutine_handle<promise_type> handle_;
  };
}

#endif // DRIPCORE_COROUTINE_H
