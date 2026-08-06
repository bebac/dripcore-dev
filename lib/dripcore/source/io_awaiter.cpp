#include "dripcore/io_awaiter.h"
#include "dripcore/executor.h"
#include "dripcore/cancelled.h"
#include "dripcore/task_control_block.h"
#include "dripcore/task_handle.h"

namespace dripcore
{
  namespace
  {
    void cancel_io_awaiter(void* ptr)
    {
      auto* io = static_cast<io_awaiter*>(ptr);
      if (io->exec_ptr_ && io->control_block_)
      {
        io->exec_ptr_->deregister_io(io);
        io->exec_ptr_->ready_task(io->control_block_);
      }
    }
  }

  void io_awaiter::cancel()
  {
    if (control_block_) {
      control_block_->cancelled_.store(true);
    }
  }

  bool io_awaiter::await_ready() const noexcept
  {
    if (control_block_) {
      return control_block_->cancelled_.load(std::memory_order_acquire);
    }
    return false;
  }

  void io_awaiter::await_suspend(std::coroutine_handle<> h)
  {
    if (control_block_) {
      control_block_->handle_ = h;
      control_block_->register_awaiter(&cancel_io_awaiter, this);
    }

    if (exec_ptr_) {
      exec_ptr_->register_io(this);
    }
  }

  short io_awaiter::await_resume()
  {
    if (control_block_) {
      control_block_->clear_awaiter();
    }
    if (control_block_ && control_block_->cancelled_) {
      throw cancelled{};
    }
    return events_;
  }

  io_awaiter::~io_awaiter()
  {
    if (control_block_) {
      control_block_->clear_awaiter();
    }
    if (exec_ptr_) {
      exec_ptr_->deregister_io(this);
    }
  }
}
