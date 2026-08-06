#include "dripcore/sleep_awaiter.h"
#include "dripcore/executor.h"
#include "dripcore/cancelled.h"
#include "dripcore/task_control_block.h"

namespace dripcore
{
  namespace
  {
    void cancel_sleep_awaiter(void* ptr)
    {
      auto* sleep = static_cast<sleep_awaiter*>(ptr);
      if (sleep->exec_ptr_ && sleep->control_block_)
      {
        sleep->exec_ptr_->deregister_timer(sleep);
        sleep->exec_ptr_->ready_task(sleep->control_block_);
      }
    }
  }

  sleep_awaiter::~sleep_awaiter()
  {
    if (control_block_) {
      control_block_->clear_awaiter();
    }
    if (exec_ptr_) {
      exec_ptr_->deregister_timer(this);
    }
  }

  void sleep_awaiter::cancel()
  {
    if (control_block_) {
      control_block_->cancelled_.store(true);
    }
  }

  bool sleep_awaiter::await_ready() const noexcept
  {
    if (control_block_) {
      return control_block_->cancelled_.load(std::memory_order_acquire);
    }
    return false;
  }

  void sleep_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (control_block_) {
      control_block_->handle_ = h;
      control_block_->register_awaiter(&cancel_sleep_awaiter, this);
    }

    if (exec_ptr_) {
      exec_ptr_->register_timer(wake_time_, h, this);
    }
  }

  void sleep_awaiter::await_resume()
  {
    if (control_block_) {
      control_block_->clear_awaiter();
    }
    if (control_block_ && control_block_->cancelled_) {
      throw cancelled{};
    }
  }
}
