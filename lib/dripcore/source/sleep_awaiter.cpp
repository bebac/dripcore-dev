#include "dripcore/sleep_awaiter.h"
#include "dripcore/executor.h"

namespace dripcore
{
  void sleep_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (exec_ptr_) {
      exec_ptr_->register_timer(wake_time_, h);
    }
  }
}
