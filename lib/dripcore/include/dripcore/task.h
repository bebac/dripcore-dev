#ifndef DRIPCORE_TASK_H
#define DRIPCORE_TASK_H

#include <dripcore/scheduler.h>
#include <concepts>

namespace dripcore
{
  // Concept: any type that has a platform_handle() method returning an int
  template<typename T>
  concept HasPlatformHandle = requires(const T& obj) {
    { obj.platform_handle() } -> std::convertible_to<int>;
  };

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
    
    // Raw file descriptor overload
    auto readable(int fd)
    {
      return io_awaiter{fd, POLLIN, scheduler_ptr_};
    }
    auto writable(int fd)
    {
      return io_awaiter{fd, POLLOUT, scheduler_ptr_};
    }
    
    // Generic overload for any type with platform_handle()
    template<HasPlatformHandle T>
    auto readable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLIN, scheduler_ptr_};
    }
    template<HasPlatformHandle T>
    auto writable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLOUT, scheduler_ptr_};
    }
    
  public: // TODO should not be public.
    scheduler *scheduler_ptr_ = nullptr;
  };
}

#endif // DRIPCORE_TASK_H
