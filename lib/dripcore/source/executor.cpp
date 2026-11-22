#include <dripcore/executor.h>
#include <dripcore/blocking_awaiter.h>
#include <dripcore/blocking_pool.h>
#include <dripcore/io_awaiter.h>
#include <dripcore/sleep_awaiter.h>
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

  void executor::register_timer(std::chrono::steady_clock::time_point wake_time,
                                std::coroutine_handle<> h,
                                sleep_awaiter* awaiter)
  {
    timer_entry entry{wake_time, h, awaiter};
    timer_queue_.insert(entry);

    // Wake up the event loop so it can recalculate the poll timeout
    event_loop_->trigger_wake_notification();
  }

  void executor::register_io(io_awaiter* a)
  {
    // Link into the intrusive pending-io list (O(1), single-threaded).
    a->prev_ = nullptr;
    a->next_ = io_head_;
    if (io_head_) {
      io_head_->prev_ = a;
    }
    io_head_ = a;
    a->linked_ = true;

    // Arm the kernel with the awaiter as udata so poll() hands it back.
    event_loop_->register_event(a->fd_, a->events_, a);
  }

  bool executor::unlink_io(io_awaiter* a)
  {
    if (!a->linked_) {
      return false;
    }
    if (a->prev_) {
      a->prev_->next_ = a->next_;
    } else {
      io_head_ = a->next_;
    }
    if (a->next_) {
      a->next_->prev_ = a->prev_;
    }
    a->prev_ = a->next_ = nullptr;
    a->linked_ = false;
    return true;
  }

  void executor::deregister_io(io_awaiter* a)
  {
    // Called from ~io_awaiter. If still armed, remove the registration from the
    // event loop (kqueue EV_DELETE / epoll EPOLL_CTL_DEL).
    if (unlink_io(a)) {
      event_loop_->deregister_event(a->fd_, a->events_);
    }
  }

  // ---- Blocking offload (see doc/blocking_offload_proposal.md) ----

  void executor::resume_external(std::coroutine_handle<> h)
  {
    {
      std::lock_guard<std::mutex> lock(external_mutex_);
      external_queue_.push_back(h);
    }
    event_loop_->trigger_wake_notification();
  }

  void executor::drain_external()
  {
    // Move handles enqueued by worker threads onto the ready queue.
    // Runs on the executor thread at the top of the run loop.
    std::deque<std::coroutine_handle<>> local;
    {
      std::lock_guard<std::mutex> lock(external_mutex_);
      local.swap(external_queue_);
    }
    for (auto h : local)
    {
      if (h && !h.done())
      {
        ready_task(h);
      }
    }
  }

  blocking_pool& executor::get_blocking_pool()
  {
    if (!blocking_pool_)
    {
      blocking_pool_ = std::make_unique<blocking_pool>();
    }
    return *blocking_pool_;
  }

  void executor::register_blocking(blocking_awaiter_base* a)
  {
    a->prev_ = nullptr;
    a->next_ = blocking_head_;
    if (blocking_head_)
    {
      blocking_head_->prev_ = a;
    }
    blocking_head_ = a;
    a->linked_ = true;
  }

  bool executor::unlink_blocking(blocking_awaiter_base* a)
  {
    if (!a->linked_)
    {
      return false;
    }
    if (a->prev_)
    {
      a->prev_->next_ = a->next_;
    }
    else
    {
      blocking_head_ = a->next_;
    }
    if (a->next_)
    {
      a->next_->prev_ = a->prev_;
    }
    a->prev_ = a->next_ = nullptr;
    a->linked_ = false;
    return true;
  }

  void executor::deregister_blocking(blocking_awaiter_base* a)
  {
    unlink_blocking(a);
  }

  void executor::register_deadline(std::chrono::steady_clock::time_point wake_time,
                                   void (*expire)(void*), void* ptr)
  {
    timer_entry entry{wake_time, {}, nullptr, expire, ptr};
    timer_queue_.insert(entry);

    // Wake up the event loop so it can recalculate the poll timeout
    event_loop_->trigger_wake_notification();
  }

  void executor::deregister_deadline(void* ptr)
  {
    for (auto it = timer_queue_.begin(); it != timer_queue_.end(); ++it)
    {
      if (it->expire_func && it->expire_ptr == ptr)
      {
        timer_queue_.erase(it);
        break;
      }
    }
  }

  void executor::on_io_event(io_awaiter* a)
  {
    // A readiness event was delivered. The backend disarmed this registration
    // on delivery (kqueue EV_ONESHOT removes it; epoll EPOLLONESHOT disables
    // it), so it will not fire again until re-armed. Once-only guard: only
    // resume if still linked, so a concurrent cancel in the same poll batch
    // cannot cause a double resume.
    if (unlink_io(a)) {
      if (a->control_block_) {
        resume(a->control_block_->handle_);
      }
    }
  }

  std::size_t executor::cancel_pending()
  {
    std::size_t count = 0;

    // Cancel all pending I/O waits: mark cancelled in control block, remove the kernel
    // registration, then schedule the coroutine to resume (it will throw
    // dripcore::cancelled from io_awaiter::await_resume).
    while (io_head_) {
      io_awaiter* a = io_head_;
      a->cancel();
      unlink_io(a);                                     // advances io_head_
      event_loop_->deregister_event(a->fd_, a->events_);
      if (a->control_block_) {
        ready_task(a->control_block_);
      }
      ++count;
    }

    // Cancel all pending blocking offloads: mark the task cancelled and
    // claim the shared state so a concurrently completing worker cannot
    // also resume the frame. The resumed frame throws dripcore::cancelled
    // from blocking_awaiter::await_resume; the worker thread finishes its
    // syscall and discards the result.
    while (blocking_head_) {
      blocking_awaiter_base* a = blocking_head_;
      a->cancel();
      unlink_blocking(a);  // cancel() does not unlink; advance the list
      ++count;
    }

    // Cancel all pending timers (sleepers). Deadline entries route
    // through their expiry hook so the single-resume claim stays in one
    // place; their blocking awaiter was normally already cancelled (and
    // the claim lost) by the blocking walk above.
    for (auto it = timer_queue_.begin(); it != timer_queue_.end(); ++it) {
      if (it->expire_func) {
        it->expire_func(it->expire_ptr);
      } else {
        if (it->awaiter) {
          it->awaiter->cancel();
        }
        if (it->handle && !it->handle.done()) {
          ready_task(it->handle);
        }
      }
      ++count;
    }
    timer_queue_.clear();

    return count;
  }

  void executor::shutdown(std::chrono::milliseconds timeout)
  {
    shutdown_requested_.store(true, std::memory_order_release);
    shutdown_deadline_ = std::chrono::steady_clock::now() + timeout;

    // Cancel all suspended tasks (deregister + mark + schedule).
    cancel_pending();

    // Mark every task's control block as cancelled, including running tasks
    // that are not currently suspended. This ensures a running task throws
    // dripcore::cancelled at its next suspension point (await_ready()
    // checks the flag and returns true, await_resume() throws).
    for (auto& t : tasks_) {
      if (t.handle_) {
        auto& promise = t.handle_.promise();
        if (promise.control_block_) {
          promise.control_block_->cancelled_.store(true, std::memory_order_release);
        }
      }
    }
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

  void executor::ready_task(task_control_block* cb)
  {
    if (cb) {
      ready_task(cb->handle_);
    }
  }

  void executor::deregister_timer(sleep_awaiter* awaiter)
  {
    for (auto it = timer_queue_.begin(); it != timer_queue_.end(); ++it) {
      if (it->awaiter == awaiter) {
        timer_queue_.erase(it);
        break;
      }
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

      if (it->expire_func)
      {
        // Deadline entry: invoke the expiry hook instead of resuming;
        // the hook owns the single-resume claim.
        it->expire_func(it->expire_ptr);
      }
      else if (it->handle && !it->handle.done())
      {
        // Resume the coroutine
        ready_task(it->handle);
      }

      timer_queue_.erase(it);
    }
  }

  int executor::calculate_poll_timeout() const
  {
    auto now = std::chrono::steady_clock::now();

    // Compute timer-based timeout
    int timeout_ms = 1000; // Default 1 second timeout
    if (!timer_queue_.empty()) {
      auto next_wake = timer_queue_.begin()->wake_time;
      if (next_wake <= now) {
        timeout_ms = 0;
      } else {
        // Round up so a sub-millisecond remainder still blocks instead of
        // busy-spinning with a zero timeout.
        auto diff = std::chrono::ceil<std::chrono::milliseconds>(next_wake - now);
        timeout_ms = static_cast<int>(diff.count());
        if (timeout_ms < 0) timeout_ms = 0;
        if (timeout_ms > 1000) timeout_ms = 1000;
      }
    }

    // If shutting down, bound the poll timeout by the remaining time
    // until the shutdown deadline so the forced phase fires promptly.
    if (shutdown_requested_.load(std::memory_order_acquire)) {
      if (now >= shutdown_deadline_) return 0;
      auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
        shutdown_deadline_ - now);
      int remaining_ms = static_cast<int>(remaining.count());
      if (remaining_ms <= 0) return 0;
      return std::min(remaining_ms, timeout_ms);
    }

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
      // Forced shutdown: deadline expired, destroy remaining frames.
      // This runs synchronous RAII destructors inside each coroutine frame
      // but does NOT resume the coroutines, so catch-based async cleanup
      // is skipped (see doc/shutdown_and_cancellation_design.md §5).
      if (shutdown_requested_.load(std::memory_order_acquire) &&
          std::chrono::steady_clock::now() >= shutdown_deadline_)
      {
        for (auto& t : tasks_)
        {
          if (t.handle_)
          {
            auto& promise = t.handle_.promise();
            if (promise.control_block_)
            {
              promise.control_block_->finished_.store(true, std::memory_order_release);
            }
          }
        }
        tasks_.clear();
        break;
      }

      drain_external();  // handles resumed from blocking-pool workers

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

    // If shutdown was requested, reset the state so the executor can be reused.
    if (shutdown_requested_.load(std::memory_order_acquire)) {
      shutdown_requested_.store(false, std::memory_order_release);
    }
  }

  void executor::process_events(int timeout_ms)
  {
    event_loop_->poll(timeout_ms, [this](void* ptr) {
      // udata is the io_awaiter* registered in register_io().
      on_io_event(static_cast<io_awaiter*>(ptr));
    });
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
