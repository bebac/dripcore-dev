#include "dripcore/io_awaiter.h"
#include "dripcore/executor.h"

namespace dripcore
{
  void io_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (exec_ptr_) {
      exec_ptr_->register_event(fd_, events_, h);
    }
  }
}
