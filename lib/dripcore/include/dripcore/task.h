#ifndef DRIPCORE_TASK_H
#define DRIPCORE_TASK_H

#include <dripcore/executor.h>
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
      executor::current()->spawn<T>(std::forward<Args>(args)...);
    }
  protected:
    auto yield()
    {
      return yield_awaiter{executor::current()};
    }
    
    // Raw file descriptor overload
    auto readable(int fd)
    {
      return io_awaiter{fd, POLLIN, executor::current()};
    }
    auto writable(int fd)
    {
      return io_awaiter{fd, POLLOUT, executor::current()};
    }
    
    // Generic overload for any type with platform_handle()
    template<HasPlatformHandle T>
    auto readable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLIN, executor::current()};
    }
    template<HasPlatformHandle T>
    auto writable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLOUT, executor::current()};
    }
  };
}

#endif // DRIPCORE_TASK_H
