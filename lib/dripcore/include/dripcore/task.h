#ifndef DRIPCORE_TASK_H
#define DRIPCORE_TASK_H

#include <dripcore/executor.h>
#include <dripcore/sleep_awaiter.h>
#include <dripcore/task_control_block.h>
#include <concepts>
#include <memory>

namespace dripcore
{
  // Concept: any type that has a platform_handle() method returning an int
  template<typename T>
  concept HasPlatformHandle = requires(const T& obj) {
    { obj.platform_handle() } -> std::convertible_to<int>;
  };

  class task
  {
  public:
    task() : control_block_(std::make_shared<task_control_block>()) {}

    std::shared_ptr<task_control_block> control_block_;

    template <typename T, typename... Args>
    task_control spawn(Args &&...args)
    {
      return executor::current()->spawn<T>(std::forward<Args>(args)...);
    }
  protected:
    auto yield()
    {
      return yield_awaiter{executor::current(), control_block_.get()};
    }

    template <typename Rep, typename Period>
    auto sleep_for(std::chrono::duration<Rep, Period> duration)
    {
      return sleep_awaiter{
        executor::current(),
        control_block_.get(),
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(duration)
      };
    }

    // Raw file descriptor overload
    auto readable(int fd)
    {
      return io_awaiter{fd, POLLIN, executor::current(), control_block_.get()};
    }
    auto writable(int fd)
    {
      return io_awaiter{fd, POLLOUT, executor::current(), control_block_.get()};
    }

    // Generic overload for any type with platform_handle()
    template<HasPlatformHandle T>
    auto readable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLIN, executor::current(), control_block_.get()};
    }
    template<HasPlatformHandle T>
    auto writable(const T& obj)
    {
      return io_awaiter{obj.platform_handle(), POLLOUT, executor::current(), control_block_.get()};
    }
  };
}

#endif // DRIPCORE_TASK_H
