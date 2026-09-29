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

    // Spawn a task on the scheduler. Defaults to co-resident placement
    // (currently: the single executor) so that top-level tasks wired
    // together by shared channels are not silently scattered once the
    // scheduler owns multiple executors. See
    // doc/multithreading_readiness_review.md "Task and Channel Locality
    // API Design".
    template <typename T, typename... Args>
    task_control spawn(Args&&... args) {
      return executor_->spawn<T>(std::forward<Args>(args)...);
    }

    // Spawn a task with no placement affinity. Identical to spawn() with a
    // single executor; once the scheduler owns multiple executors, this is
    // where the distribution policy (round-robin/least-loaded/stealable
    // queue) applies. Use only when the task shares no state with other
    // top-level tasks spawned via spawn().
    template <typename T, typename... Args>
    task_control spawn_distributed(Args&&... args) {
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
