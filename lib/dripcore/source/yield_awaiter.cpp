#include "dripcore/yield_awaiter.h"
#include "dripcore/scheduler.h"

#include <iostream>

namespace dripcore
{
  void yield_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (scheduler_ptr) {
      scheduler_ptr->ready_task(h);
    }
  }
}