#include "dripcore/yield_awaiter.h"
#include "dripcore/executor.h"

#include <iostream>

namespace dripcore
{
  void yield_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (exec_ptr) {
      exec_ptr->ready_task(h);
    }
  }
}
