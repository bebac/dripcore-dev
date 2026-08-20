#ifndef DRIPCORE_TASK_CONTROL_H
#define DRIPCORE_TASK_CONTROL_H

#include <dripcore/task_control_block.h>

#include <coroutine>
#include <memory>
#include <atomic>
#include <cstddef>

namespace dripcore
{
  // Awaitable returned by task_control::stop(). Cancels the target
  // task and resumes the caller when the target has finished.
  struct stop_awaiter {
    task_control_block* cb_;
    std::coroutine_handle<> handle_ = nullptr;

    bool await_ready() noexcept {
      if (!cb_) return true;
      return cb_->finished_.load(std::memory_order_acquire);
    }

    void await_suspend(std::coroutine_handle<> h) {
      handle_ = h;
      cb_->stop_continuation_ = h;
      cb_->cancelled_.store(true);
      if (cb_->cancel_func_ && cb_->awaiter_ptr_) {
        cb_->cancel_func_(cb_->awaiter_ptr_);
      }
    }

    void await_resume() noexcept {
      if (handle_ && cb_ && cb_->stop_continuation_ == handle_) {
        cb_->stop_continuation_ = nullptr;
      }
    }

    ~stop_awaiter() {
      if (handle_ && cb_ && cb_->stop_continuation_ == handle_) {
        cb_->stop_continuation_ = nullptr;
      }
    }
  };

  class task_control
  {
  public:
    task_control() = default;
    explicit task_control(std::shared_ptr<task_control_block> cb) noexcept
      : control_block_(std::move(cb)) {}

    // Copyable
    task_control(const task_control&) = default;
    task_control& operator=(const task_control&) = default;

    // Movable
    task_control(task_control&&) = default;
    task_control& operator=(task_control&&) = default;

    // Check if the controlled task has completed
    bool done() const;

    // Request cancellation of the task (fire-and-forget).
    // Phase 2: must be called from the executor's thread.
    void cancel();

    // Cancel the target task and wait for it to finish.
    // Must be co_awaited from a coroutine on the same executor.
    auto stop() {
      return stop_awaiter{control_block_.get()};
    }

  private:
    std::shared_ptr<task_control_block> control_block_;
  };
}

#endif // DRIPCORE_TASK_CONTROL_H