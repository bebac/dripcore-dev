#include "dripcore/io_awaiter.h"
#include "dripcore/scheduler.h"

namespace dripcore
{
  void io_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (scheduler_ptr_) {
      scheduler_ptr_->register_event(fd_, events_, h);
    }
  }
}