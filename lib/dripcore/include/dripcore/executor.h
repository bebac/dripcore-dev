#ifndef DRIPCORE_EXECUTOR_H
#define DRIPCORE_EXECUTOR_H

#include <dripcore/task_handle.h>
#include <dripcore/event_loop.h>
#include <dripcore/task_control.h>
//#include <dripcore/event.h>
#include <vector>
#include <list>
#include <atomic>
#include <functional>
#include <queue>
#include <chrono>
#include <deque>
#include <optional>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <cstddef>

namespace dripcore
{
  struct io_awaiter;
  struct sleep_awaiter;
  struct blocking_awaiter_base;
  class blocking_pool;

  class executor
  {
  public:
    executor();
    ~executor();

    // Get the current thread's executor (thread-local)
    static executor* current();
  public:
    template <typename T, typename... Args>
    task_control spawn(Args&&... args) {
      // 1. Create the task object (e.g., greeter)
      // We use a shared_ptr to keep the task object alive as long
      // as the coroutine (task_handle) is alive.
      auto obj = std::make_shared<T>(std::forward<Args>(args)...);

      // 2. Start the coroutine.
      // We assume the convention that T has a method 'execute()'
      // which returns a dripcore::coroutine.
      auto t = obj->execute();

      // 3. Register the task
      //t.set_scheduler(this);
      t.set_holder(obj);

      // 4. Set up control block. The promise also holds it so final_suspend()
      // can mark the task finished before the frame is reaped.
      obj->control_block_->executor_ = this;
      obj->control_block_->handle_ = t.handle_;
      t.handle_.promise().control_block_ = obj->control_block_;

      // 5. Keep the task in the execution list and add it to the
      // ready list.
      auto& r = tasks_.emplace_back(std::move(t));
      ready_task(r.handle_);

      // 6. Return task_control for per-task cancellation
      return task_control{obj->control_block_};
    }
  public:
    void register_timer(std::chrono::steady_clock::time_point wake_time,
                        std::coroutine_handle<> h,
                        sleep_awaiter* awaiter = nullptr);
    void deregister_timer(sleep_awaiter* awaiter);
    void ready_task(std::coroutine_handle<> h);
    void ready_task(task_control_block* cb);
    bool process_ready_tasks();
    void run();

    // --- Blocking offload (see doc/blocking_offload_proposal.md) ---
    // Thread-safe: enqueue a handle for resumption on this executor's
    // thread, waking poll() via the wake-notification path. Called from
    // blocking-pool worker threads; this is the only cross-thread entry
    // point into the executor.
    void resume_external(std::coroutine_handle<> h);

    // The lazily created blocking pool used by task::run_blocking().
    blocking_pool& get_blocking_pool();

    // Pending-blocking registry (single-threaded, like the io registry):
    // lets cancel_pending() reach blocking awaiters at shutdown.
    void register_blocking(blocking_awaiter_base* a);
    void deregister_blocking(blocking_awaiter_base* a);

    // Deadline timer: a timer-wheel entry carrying an expiry hook that
    // is invoked on the executor thread when the deadline passes,
    // instead of a handle to resume. Used by blocking_awaiter to enforce
    // run_blocking() timeouts; the hook owns the single-resume claim.
    void register_deadline(std::chrono::steady_clock::time_point wake_time,
                           void (*expire)(void*), void* ptr);
    void deregister_deadline(void* ptr);

    // --- I/O wait registry (used by io_awaiter) ---
    // Arm the event loop and link the awaiter into the pending-io list.
    void register_io(io_awaiter* a);
    // Unlink + EV_DELETE if still armed. Called from ~io_awaiter.
    void deregister_io(io_awaiter* a);

    // --- Cancellation ---
    // Cancel every pending I/O wait and timer: mark each awaiter cancelled,
    // remove its kernel/timer registration, and schedule it to resume (where
    // it will throw dripcore::cancelled). Returns the number cancelled.
    // This is the primitive the shutdown protocol and task_control build on.
    std::size_t cancel_pending();

    // --- Shutdown ---
    // Initiate a two-phase shutdown (see
    // doc/shutdown_and_cancellation_design.md §5).
    // Graceful: sets the shutdown flag, calls cancel_pending(), and marks
    // every task's control block as cancelled so that running tasks throw
    // at their next suspension point. The run() loop then drains until all
    // tasks reach done() or the deadline expires.
    // Forced (deadline expiry): run() destroys remaining frames, running
    // synchronous RAII destructors only.
    void shutdown(std::chrono::milliseconds timeout);
  private:
    void process_events(int timeout_ms);
    void on_io_event(io_awaiter* a);   // poll() callback: once-only resume
    bool unlink_io(io_awaiter* a);     // returns true if it was linked
    void resume(std::coroutine_handle<> h);
    auto calculate_poll_timeout() const -> int;
    void process_expired_timers();
  private:
    std::unique_ptr<event_loop> event_loop_;
    std::list<task_handle> tasks_;
    struct timer_entry {
      std::chrono::steady_clock::time_point wake_time;
      std::coroutine_handle<> handle;
      sleep_awaiter* awaiter = nullptr;   // for cancellation (may be null)

      // Deadline entries (run_blocking timeouts) carry an expiry hook
      // instead of a sleep awaiter: it is invoked on the executor thread
      // at expiry and owns the single-resume claim
      // (blocking_awaiter::cancel). Entries with a null hook resume
      // their handle directly, as sleep timers always have.
      void (*expire_func)(void*) = nullptr;
      void* expire_ptr = nullptr;

      bool operator<(const timer_entry& other) const {
        return wake_time < other.wake_time;
      }
    };
    std::multiset<timer_entry> timer_queue_;
    std::vector<std::coroutine_handle<>> ready_queue_;
    io_awaiter* io_head_ = nullptr;   // head of intrusive pending-io list

    // --- Blocking offload state (see doc/blocking_offload_proposal.md) ---
    void drain_external();
    bool unlink_blocking(blocking_awaiter_base* a);

    std::mutex external_mutex_;
    std::deque<std::coroutine_handle<>> external_queue_;
    blocking_awaiter_base* blocking_head_ = nullptr;  // intrusive pending-blocking list
    std::unique_ptr<blocking_pool> blocking_pool_;

    // Shutdown state (see doc/shutdown_and_cancellation_design.md §5)
    std::atomic<bool> shutdown_requested_{false};
    std::chrono::steady_clock::time_point shutdown_deadline_;
  };
}

#endif // DRIPCORE_EXECUTOR_H
