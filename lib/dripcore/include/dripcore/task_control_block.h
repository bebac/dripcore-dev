#ifndef DRIPCORE_TASK_CONTROL_BLOCK_H
#define DRIPCORE_TASK_CONTROL_BLOCK_H

#include <atomic>
#include <memory>
#include <cstddef>
#include <coroutine>

namespace dripcore
{
  class executor;

  struct task_control_block
  {
    // Owning executor. Currently only used to identify the task's executor;
    // Phase 3 will use it to post cross-thread cancellation requests.
    executor* executor_ = nullptr;

    // The task's coroutine handle. IMPORTANT: only valid while finished_ is
    // false. The frame is owned by the task_handle in executor::tasks_ and is
    // destroyed when the task is reaped, whereas this control block can
    // outlive it (a task_control may be retained by the caller). Never touch
    // handle_ without first checking finished_.
    std::coroutine_handle<> handle_;

    // Set from the task promise's final_suspend() when the coroutine
    // completes. After this, handle_ must be treated as dangling.
    std::atomic<bool> finished_{false};

    std::atomic<bool> cancelled_{false};

    // Type-erased cancellation: function pointer + context
    void (*cancel_func_)(void*) = nullptr;
    void* awaiter_ptr_ = nullptr;

    void register_awaiter(void (*func)(void*), void* ptr)
    {
      cancel_func_ = func;
      awaiter_ptr_ = ptr;
    }

    void clear_awaiter()
    {
      cancel_func_ = nullptr;
      awaiter_ptr_ = nullptr;
    }
  };
}

#endif // DRIPCORE_TASK_CONTROL_BLOCK_H
