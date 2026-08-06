#include "dripcore/yield_awaiter.h"
#include "dripcore/executor.h"
#include "dripcore/cancelled.h"
#include "dripcore/task_control_block.h"

namespace dripcore
{
  namespace
  {
    void cancel_yield_awaiter(void* ptr)
    {
      auto* yield = static_cast<yield_awaiter*>(ptr);
      if (yield->exec_ptr_ && yield->control_block_) {
        yield->exec_ptr_->ready_task(yield->control_block_);
      }
    }
  }

  bool yield_awaiter::await_ready()
  {
    if (control_block_) {
      return control_block_->cancelled_.load(std::memory_order_acquire);
    }
    return false;
  }

  void yield_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (control_block_) {
      control_block_->handle_ = h;
      control_block_->register_awaiter(&cancel_yield_awaiter, this);
    }

    if (exec_ptr_) {
      exec_ptr_->ready_task(h);
    }
  }

  void yield_awaiter::await_resume()
  {
    if (control_block_) {
      control_block_->clear_awaiter();
    }
    if (control_block_ && control_block_->cancelled_) {
      throw cancelled{};
    }
  }
}
