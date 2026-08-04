#include "dripcore/sleep_awaiter.h"
#include "dripcore/executor.h"
#include "dripcore/cancelled.h"

namespace dripcore
{
  void sleep_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (exec_ptr_) {
      // Pass `this` so the executor can mark us cancelled via the timer entry.
      exec_ptr_->register_timer(wake_time_, h, this);
    }
  }

  void sleep_awaiter::await_resume()
  {
    if (cancelled_) {
      throw cancelled{};
    }
  }
}
