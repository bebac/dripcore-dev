#ifndef DRIPCORE_SLEEP_AWAITER_H
#define DRIPCORE_SLEEP_AWAITER_H

#include <coroutine>
#include <chrono>

namespace dripcore
{
  class executor;

  struct sleep_awaiter
  {
    executor *exec_ptr_;
    std::chrono::steady_clock::time_point wake_time_;

    sleep_awaiter(executor *exec, std::chrono::steady_clock::duration duration)
      :
      exec_ptr_(exec),
      wake_time_(std::chrono::steady_clock::now() + duration)
    {
    }

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h);
    void await_resume() noexcept {}
  };
}

#endif // DRIPCORE_SLEEP_AWAITER_H
