#include <dripcore/event_loop_epoll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <poll.h>
#include <system_error>

namespace dripcore
{
  event_loop_epoll::event_loop_epoll()
  {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      throw std::system_error(errno, std::system_category(), "epoll creation failed");
    }
  }

  event_loop_epoll::~event_loop_epoll()
  {
    if (wake_event_fd_ >= 0) {
      close(wake_event_fd_);
    }
    if (epoll_fd_ >= 0) {
      close(epoll_fd_);
    }
  }

  void event_loop_epoll::register_event(int fd, short events, std::coroutine_handle<> h)
  {
    struct epoll_event ev;

    ev.events = ((events & POLLIN) ? EPOLLIN : EPOLLOUT) | EPOLLONESHOT;
    ev.data.ptr = h.address();

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
      if (errno == EEXIST) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
      }
    }
  }

  int event_loop_epoll::poll(bool use_zero_timeout, std::function<void(void*)> resume_callback)
  {
    struct epoll_event evs[32];

    // Use zero timeout for non-blocking poll (when we have CPU work),
    // otherwise use 1 second timeout (wake notification will interrupt)
    int timeout_ms = use_zero_timeout ? 0 : 1000;

    int n = epoll_wait(epoll_fd_, evs, 32, timeout_ms);

    for (int i = 0; i < n; i++)
    {
      // Skip wake event fd - it's just for notification
      if (evs[i].data.fd == wake_event_fd_)
      {
        // Consume the eventfd value to reset it
        uint64_t val;
        read(wake_event_fd_, &val, sizeof(val));
        continue;
      }
      resume_callback(evs[i].data.ptr);
    }

    return n;
  }

  void event_loop_epoll::setup_wake_notification()
  {
    // Create an eventfd for wake notifications
    wake_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_event_fd_ < 0) {
      throw std::system_error(errno, std::system_category(), "eventfd creation failed");
    }

    // Register the eventfd with epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = wake_event_fd_;  // Store fd so we can identify it
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_event_fd_, &ev) == -1) {
      throw std::system_error(errno, std::system_category(), "epoll_ctl for eventfd failed");
    }
  }

  void event_loop_epoll::trigger_wake_notification()
  {
    // Write to eventfd to wake up epoll_wait
    uint64_t val = 1;
    // Ignore write errors (EAGAIN is fine, means it's already triggered)
    (void)write(wake_event_fd_, &val, sizeof(val));
  }

  // Factory function implementation for Linux
  event_loop* create_event_loop()
  {
    return new event_loop_epoll();
  }
}
