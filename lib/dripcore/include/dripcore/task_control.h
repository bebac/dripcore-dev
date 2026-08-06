#ifndef DRIPCORE_TASK_CONTROL_H
#define DRIPCORE_TASK_CONTROL_H

#include <memory>
#include <atomic>
#include <cstddef>

namespace dripcore
{
  struct task_control_block;

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

    // Request cancellation of the task.
    // Phase 2: must be called from the executor's thread.
    void cancel();

  private:
    std::shared_ptr<task_control_block> control_block_;
  };
}

#endif // DRIPCORE_TASK_CONTROL_H