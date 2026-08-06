#ifndef DRIPCORE_YIELD_AWAITER_H
#define DRIPCORE_YIELD_AWAITER_H

#include <coroutine>

namespace dripcore
{
  class executor;
  struct task_control_block;

  struct yield_awaiter
  {
    executor* exec_ptr_;
    task_control_block* control_block_;

    // Return true if task was cancelled, false to suspend
    bool await_ready();
    void await_suspend(std::coroutine_handle<> h);
    void await_resume();
  };
}

#endif // DRIPCORE_YIELD_AWAITER_H
