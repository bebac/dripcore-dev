#include "dripcore/task_handle.h"
#include "dripcore/executor.h"

namespace dripcore
{
  void task_handle::promise_type::final_awaiter::await_suspend(
    std::coroutine_handle<>
  ) noexcept {
    if (promise->control_block_) {
      promise->control_block_->finished_.store(true, std::memory_order_release);
      if (promise->control_block_->stop_continuation_) {
        auto h = promise->control_block_->stop_continuation_;
        promise->control_block_->stop_continuation_ = nullptr;
        if (promise->control_block_->executor_) {
          promise->control_block_->executor_->ready_task(h);
        }
      }
    }
  }

  task_handle::task_handle()
    : handle_(nullptr)
  {
  }

  task_handle::task_handle(std::coroutine_handle<promise_type> handle)
    : handle_(handle)
  {
  }

  task_handle::~task_handle()
  {
    if (handle_)
    {
      handle_.destroy();
    }
  }

  task_handle::task_handle(task_handle &&other) noexcept
    : handle_(other.handle_)
  {
    other.handle_ = nullptr;
  }

  bool task_handle::done() const
  {
    return handle_.done();
  }

  void task_handle::set_holder(std::shared_ptr<void> ptr)
  {
    auto &promise = handle_.promise();
    promise.holder_ptr = ptr;
  }
}