#ifndef DRIPCORE_SLEEP_AWAITER_H
#define DRIPCORE_SLEEP_AWAITER_H

#include <coroutine>
#include <chrono>

namespace dripcore
{
  class executor;
  struct task_control_block;

  struct sleep_awaiter
  {
    executor* exec_ptr_;
    task_control_block* control_block_;
    std::chrono::steady_clock::time_point wake_time_;

    sleep_awaiter(executor* exec, task_control_block* cb, std::chrono::steady_clock::duration duration)
      :
      exec_ptr_(exec),
      control_block_(cb),
      wake_time_(std::chrono::steady_clock::now() + duration)
    {
    }

    ~sleep_awaiter();

    void cancel();

    // Return true if task was cancelled, false to suspend
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> h);
    // Throws dripcore::cancelled if the sleep was cancelled. Out-of-line.
    void await_resume();
  };
}

#endif // DRIPCORE_SLEEP_AWAITER_H
