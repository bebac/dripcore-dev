#ifndef DRIPCORE_IO_AWAITER_H
#define DRIPCORE_IO_AWAITER_H

#include <coroutine>

namespace dripcore
{
  class scheduler;

  struct io_awaiter
  {
    int fd_;
    short events_;         // POLLIN | POLLOUT
    scheduler* scheduler_ptr_;

    // Return false to indicate we want to suspend
    bool await_ready() const noexcept { return false; }

    // Called just before the coroutine is suspended
    void await_suspend(std::coroutine_handle<> h);

    // Called when the coroutine is resumed
    short await_resume() noexcept
    {
      return events_;
    }
  };
}

#endif // DRIPCORE_IO_AWAITER_H
