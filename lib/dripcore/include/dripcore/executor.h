#ifndef DRIPCORE_EXECUTOR_H
#define DRIPCORE_EXECUTOR_H

#include <dripcore/task_handle.h>
#include <dripcore/event_loop.h>
//#include <dripcore/event.h>
#include <vector>
#include <list>
#include <atomic>
#include <functional>
#include <queue>
#include <chrono>
#include <optional>
#include <iostream>
#include <memory>
#include <set>

namespace dripcore
{
  class executor
  {
  public:
    executor();
    ~executor();

    // Get the current thread's executor (thread-local)
    static executor* current();
  public:
    template <typename T, typename... Args>
    void spawn(Args&&... args) {
      // 1. Create the task object (e.g., greeter)
      // We use a shared_ptr to keep the task object alive as long
      // as the coroutine (task_handle) is alive.
      auto obj = std::make_shared<T>(std::forward<Args>(args)...);

      // 2. (REMOVED) No longer needed - tasks use executor::current() instead

      // 3. Start the coroutine.
      // We assume the convention that T has a method 'execute()'
      // which returns a dripcore::coroutine.
      auto t = obj->execute();

      // 4. Register the task
      //t.set_scheduler(this);
      t.set_holder(obj);

      // 5. Keep the task in the execution list and add it to the
      // ready list.
      auto& r = tasks_.emplace_back(std::move(t));

      ready_task(r.handle_);
    }
  public:
    void register_event(int fd, short events, std::coroutine_handle<> h);
    void register_timer(std::chrono::steady_clock::time_point wake_time, std::coroutine_handle<> h);
    void ready_task(std::coroutine_handle<> h);
    bool process_ready_tasks();
    void run();
  private:
    void process_events(int timeout_ms);
    void resume_ptr(void* ptr);
    void resume(std::coroutine_handle<> h);
    auto calculate_poll_timeout() const -> int;
    void process_expired_timers();
  private:
    std::unique_ptr<event_loop> event_loop_;
    std::list<task_handle> tasks_;
    struct timer_entry {
      std::chrono::steady_clock::time_point wake_time;
      std::coroutine_handle<> handle;

      bool operator<(const timer_entry& other) const {
        return wake_time < other.wake_time;
      }
    };
    std::multiset<timer_entry> timer_queue_;
    std::vector<std::coroutine_handle<>> ready_queue_;
  };
}

#endif // DRIPCORE_EXECUTOR_H
