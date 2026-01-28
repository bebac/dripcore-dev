#ifndef DRIPCORE_YIELD_AWAITER_H
#define DRIPCORE_YIELD_AWAITER_H

#include <coroutine>

namespace dripcore
{
  class executor;

  struct yield_awaiter
  {
    executor* exec_ptr;
    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> h);
    void await_resume() {}
  };
}

#endif // DRIPCORE_YIELD_AWAITER_H
