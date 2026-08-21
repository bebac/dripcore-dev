#ifndef DRIPCORE_TASK_H
#define DRIPCORE_TASK_H

#include <dripcore/executor.h>
#include <dripcore/awaitable.h>
#include <dripcore/sleep_awaiter.h>
#include <dripcore/task_control_block.h>
#include <dripcore/cancelled.h>
#include <concepts>
#include <memory>
#include <exception>

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

    // Fixed entry point the executor spawns. Delegates to body() and
    // teardown() so that async cleanup always runs, even on cancellation
    // or exception. See doc/shutdown_and_cancellation_design.md §9.
    task_handle execute()
    {
      std::exception_ptr ep;
      try {
        co_await body();
      } catch (const dripcore::cancelled&) {
        // Cancellation is normal termination. Mark the control block
        // so task_control::cancelled() returns true. The exception is
        // not rethrown, so no exception is stored in the control block.
        // See doc/shutdown_and_cancellation_design.md §4.
        if (control_block_) {
          control_block_->cancelled_.store(true, std::memory_order_release);
        }
      } catch (...) {
        ep = std::current_exception();
      }
      co_await teardown();
      if (ep) std::rethrow_exception(ep);
    }

    template <typename T, typename... Args>
    task_control spawn(Args &&...args)
    {
      return executor::current()->spawn<T>(std::forward<Args>(args)...);
    }
  protected:
    // Task's main logic. Override in derived classes.
    virtual awaitable<> body() = 0;

    // Always-run async cleanup (the "finally" equivalent). Runs on every
    // exit path: normal completion, error, and cancellation. Default is
    // a no-op. May co_await. Not guaranteed to complete under forced
    // shutdown (frame destruction).
    virtual awaitable<> teardown() { co_return; }

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
