#include "dripcore/coroutine.h"
#include "dripcore/executor.h"

namespace dripcore
{
  coroutine::coroutine()
    : handle_(nullptr)
  {
  }

  coroutine::coroutine(std::coroutine_handle<promise_type> handle)
    : handle_(handle)
  {
  }

  coroutine::~coroutine()
  {
    if (handle_)
    {
      handle_.destroy();
    }
  }

  coroutine::coroutine(coroutine &&other) noexcept
    : handle_(other.handle_)
  {
    other.handle_ = nullptr;
  }

  bool coroutine::is_done() const
  {
    return handle_.done();
  }

  void coroutine::set_holder(std::shared_ptr<void> ptr)
  {
    auto &promise = handle_.promise();
    promise.holder_ptr = ptr;
  }
}