#ifndef DRIPCORE_TASK_H
#define DRIPCORE_TASK_H

#include <dripcore/scheduler.h>

namespace dripcore
{
  class task
  {
  protected:
    template <typename T, typename... Args>
    void spawn(Args &&...args)
    {
      scheduler_ptr_->spawn<T>(std::forward<Args>(args)...);
    }
  protected:
    auto yield()
    {
      return yield_awaiter{scheduler_ptr_};
    }
    auto readable(int fd)
    {
      return io_awaiter{fd, POLLIN, scheduler_ptr_};
    }
    auto writable(int fd)
    {
      return io_awaiter{fd, POLLOUT, scheduler_ptr_};
    }
  public: // TODO should not be public.
    scheduler *scheduler_ptr_ = nullptr;
  };
}

#endif // DRIPCORE_TASK_H
