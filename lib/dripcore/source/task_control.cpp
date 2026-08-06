#include "dripcore/task_control.h"
#include "dripcore/task_control_block.h"
#include "dripcore/cancelled.h"
#include "dripcore/io_awaiter.h"
#include "dripcore/sleep_awaiter.h"
#include "dripcore/executor.h"

#include <cassert>

namespace dripcore
{
  bool task_control::done() const
  {
    if (!control_block_) return true;
    // Must not inspect handle_: the frame may already have been destroyed
    // while this control block lives on. finished_ is published by the task
    // promise's final_suspend().
    return control_block_->finished_.load(std::memory_order_acquire);
  }

  void task_control::cancel()
  {
    if (!control_block_) return;
    if (control_block_->finished_.load(std::memory_order_acquire)) return;

    // Phase 2: same-thread only
    // In Phase 3, this will need cross-thread support via MPSC queue
    assert(executor::current()); // Must be called from executor thread

    // Mark as cancelled in the control block
    control_block_->cancelled_ = true;

    // Cancel the current awaiter if task is suspended
    if (control_block_->cancel_func_ && control_block_->awaiter_ptr_)
    {
      control_block_->cancel_func_(control_block_->awaiter_ptr_);
    }
  }
}