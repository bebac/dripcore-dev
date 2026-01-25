#ifndef DRIPCORE_YIELD_AWAITER_H
#define DRIPCORE_YIELD_AWAITER_H

#include <coroutine>

namespace dripcore
{
  class scheduler;

  struct yield_awaiter
  {
    scheduler* scheduler_ptr;
    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> h);
    void await_resume() {}
  };
}

#endif // DRIPCORE_YIELD_AWAITER_H
