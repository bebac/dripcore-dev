#include "dripcore/io_awaiter.h"
#include "dripcore/executor.h"
#include "dripcore/cancelled.h"

namespace dripcore
{
  void io_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    handle_ = h;
    if (exec_ptr_) {
      exec_ptr_->register_io(this);
    }
  }

  short io_awaiter::await_resume()
  {
    if (cancelled_) {
      throw cancelled{};
    }
    return events_;
  }

  io_awaiter::~io_awaiter()
  {
    // If the frame is destroyed while still armed (e.g. forced shutdown),
    // remove the kernel registration so a later event cannot resume freed
    // memory. No-op if already unlinked by the normal resume/cancel path.
    if (exec_ptr_) {
      exec_ptr_->deregister_io(this);
    }
  }
}
