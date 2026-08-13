#ifndef DRIPCORE_SCHEDULER_H
#define DRIPCORE_SCHEDULER_H

#include <dripcore/executor.h>
#include <dripcore/task_control.h>
#include <memory>
#include <cstddef>
#include <chrono>

namespace dripcore
{
  // High-level scheduler that manages executor(s)
  // Phase 2: Single executor (preparation for multi-threading)
  // Phase 3+: Multiple executors with work-stealing
  class scheduler
  {
  public:
    // Create scheduler with specified number of executors
    // For Phase 2, num_executors is always 1 (ignored)
    explicit scheduler(size_t num_executors = 1);
    ~scheduler();

    // Spawn a task on the scheduler
    // Currently delegates to the single executor
    template <typename T, typename... Args>
    task_control spawn(Args&&... args) {
      return executor_->spawn<T>(std::forward<Args>(args)...);
    }

    // Run the scheduler until all tasks complete
    void run();

    // Shut down the scheduler: cancel all tasks, drain with a deadline,
    // then force-destroy any remaining frames. See
    // doc/shutdown_and_cancellation_design.md §5.
    void shutdown(std::chrono::milliseconds timeout = std::chrono::seconds(5));

  private:
    std::unique_ptr<executor> executor_;
  };
}

#endif // DRIPCORE_SCHEDULER_H
