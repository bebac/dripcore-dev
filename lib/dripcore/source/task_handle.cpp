#include "dripcore/task_handle.h"
#include "dripcore/executor.h"

namespace dripcore
{
  task_handle::task_handle()
    : handle_(nullptr)
  {
  }

  task_handle::task_handle(std::coroutine_handle<promise_type> handle)
    : handle_(handle)
  {
  }

  task_handle::~task_handle()
  {
    if (handle_)
    {
      handle_.destroy();
    }
  }

  task_handle::task_handle(task_handle &&other) noexcept
    : handle_(other.handle_)
  {
    other.handle_ = nullptr;
  }

  bool task_handle::done() const
  {
    return handle_.done();
  }

  void task_handle::set_holder(std::shared_ptr<void> ptr)
  {
    auto &promise = handle_.promise();
    promise.holder_ptr = ptr;
  }
}