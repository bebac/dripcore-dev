#include <dripcore/executor.h>
#include <system_error>
#include <algorithm>

namespace dripcore
{
  // Thread-local storage for current executor
  thread_local executor* current_executor = nullptr;

  executor::executor()
    : event_loop_(create_event_loop())
  {
    current_executor = this;
    event_loop_->setup_wake_notification();
  }

  executor::~executor()
  {
    // event_loop_ automatically cleaned up by unique_ptr
  }

  executor* executor::current()
  {
    return current_executor;
  }

  void executor::register_event(int fd, short events, std::coroutine_handle<> h)
  {
    event_loop_->register_event(fd, events, h);
  }

  void executor::ready_task(std::coroutine_handle<> h)
  {
    bool was_empty = ready_queue_.empty();
    ready_queue_.push_back(h);

    // Wake up the event loop if it was waiting
    if (was_empty) {
      event_loop_->trigger_wake_notification();
    }
  }

  bool executor::process_ready_tasks()
  {
    // Use a local swap to avoid infinite loops if tasks yield back immediately
    std::vector<std::coroutine_handle<>> to_run;
    to_run.swap(ready_queue_);

    for (auto h : to_run)
    {
      if (h && !h.done())
      {
        h.resume();
      }
    }

    return !to_run.empty();
  }

  void executor::run()
  {
    while (!tasks_.empty())
    {
      bool had_ready_tasks = process_ready_tasks();

      // Use zero timeout if we had ready tasks or more were added during processing.
      // This ensures we don't block when there's CPU work to do, while still
      // checking for I/O events to prevent starvation.
      bool use_zero_timeout = had_ready_tasks || !ready_queue_.empty();
      process_events(use_zero_timeout);

      tasks_.remove_if([](const auto &t) { return t.done(); });
    }
  }

  void executor::process_events(bool use_zero_timeout)
  {
    event_loop_->poll(use_zero_timeout, [this](void* ptr) {
      resume_ptr(ptr);
    });
  }

  void executor::resume_ptr(void *ptr)
  {
    if (ptr) {
      std::coroutine_handle<>::from_address(ptr).resume();
    }
  }
}
