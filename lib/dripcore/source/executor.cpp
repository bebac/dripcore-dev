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
    timer_queue_.clear();
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

  void executor::register_timer(std::chrono::steady_clock::time_point wake_time,
                                std::coroutine_handle<> h)
  {
    timer_entry entry{wake_time, h};
    timer_queue_.insert(entry);

    // Wake up the event loop so it can recalculate the poll timeout
    event_loop_->trigger_wake_notification();
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

  void executor::process_expired_timers()
  {
    auto now = std::chrono::steady_clock::now();

    // Process all timers that have expired
    while (!timer_queue_.empty())
    {
      auto it = timer_queue_.begin();
      if (it->wake_time > now)
      {
        break; // No more expired timers
      }

      // Resume the coroutine
      if (it->handle && !it->handle.done())
      {
        ready_task(it->handle);
      }

      timer_queue_.erase(it);
    }
  }

  int executor::calculate_poll_timeout() const
  {
    if (timer_queue_.empty()) {
      return 1000; // Default 1 second timeout
    }

    auto now = std::chrono::steady_clock::now();
    auto next_wake = timer_queue_.begin()->wake_time;

    if (next_wake <= now) {
      return 0; // Timer already expired, don't block
    }

    // Round up so a sub-millisecond remainder still blocks instead of
    // busy-spinning with a zero timeout.
    auto diff = std::chrono::ceil<std::chrono::milliseconds>(next_wake - now);

    // Clamp to reasonable range (0 to 1000ms)
    auto timeout_ms = static_cast<int>(diff.count());
    if (timeout_ms < 0) return 0;
    if (timeout_ms > 1000) return 1000;
    return timeout_ms;
  }

  bool executor::process_ready_tasks()
  {
    // Use a local swap to avoid infinite loops if tasks yield back immediately
    std::vector<std::coroutine_handle<>> to_run;
    to_run.swap(ready_queue_);

    for (auto h : to_run)
    {
      if (h && !h.done()) {
        resume(h);
      }
    }

    return !to_run.empty();
  }

  void executor::run()
  {
    while (!tasks_.empty())
    {
      bool had_ready_tasks = process_ready_tasks();

      process_expired_timers();  // Catch timers expired between iterations.

      // Calculate if we should use zero timeout
      // Use zero timeout if we had ready tasks or more were added during processing,
      // OR if we have timers that need to be checked soon
      int timeout_ms = (had_ready_tasks || !ready_queue_.empty()) ? 0 : calculate_poll_timeout();

      process_events(timeout_ms);
      process_expired_timers();

      tasks_.remove_if([](const auto &t) { return t.done(); });
    }
  }

  void executor::process_events(int timeout_ms)
  {
    event_loop_->poll(timeout_ms, [this](void* ptr) {
      resume_ptr(ptr);
    });
  }

  void executor::resume_ptr(void *ptr)
  {
    if (ptr) {
      auto h = std::coroutine_handle<>::from_address(ptr);
      resume(h);
    }
  }

  void executor::resume(std::coroutine_handle<> h)
  {
    // Body exceptions are captured by the coroutine promise's
    // unhandled_exception(). Because initial_suspend() is suspend_always, the
    // body always executes inside the coroutine's implicit try block on the
    // first resume, so resume() does not rethrow for a well-formed task.
    // This single choke point is kept for the upcoming cancellation
    // double-resume guard.
    h.resume();
  }
}
