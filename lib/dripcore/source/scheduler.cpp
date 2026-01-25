#include <dripcore/scheduler.h>
#include <system_error>
#include <algorithm>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/epoll.h>
#endif

namespace dripcore
{
  scheduler::scheduler()
  {
#ifdef __APPLE__
    scheduler_fd_ = kqueue();
#else
    scheduler_fd_ = epoll_create1(EPOLL_CLOEXEC);
#endif
    if (scheduler_fd_ < 0)
    {
      throw std::system_error(errno, std::system_category());
    }
  }

  scheduler::~scheduler()
  {
    if (scheduler_fd_ >= 0) {
      close(scheduler_fd_);
    }
  }

  void scheduler::register_event(int fd, short events, std::coroutine_handle<> h)
  {
#if defined(__linux__)
    struct epoll_event ev;
    ev.events = ((events & POLLIN) ? EPOLLIN : EPOLLOUT) | EPOLLONESHOT;
    ev.data.ptr = h.address();
    if (epoll_ctl(scheduler_fd_, EPOLL_CTL_ADD, fd, &ev) == -1)
    {
      if (errno == EEXIST)
        epoll_ctl(scheduler_fd_, EPOLL_CTL_MOD, fd, &ev);
    }
#elif defined(__APPLE__)
    struct kevent sev;
    int filter = (events & POLLIN) ? EVFILT_READ : EVFILT_WRITE;
    EV_SET(&sev, fd, filter, EV_ADD | EV_ONESHOT, 0, 0, h.address());
    kevent(scheduler_fd_, &sev, 1, nullptr, 0, nullptr);
#endif
  }

  void scheduler::ready_task(std::coroutine_handle<> h)
  {
    ready_queue_.push_back(h);
  }

  void scheduler::process_ready_tasks()
  {
    // Use a local swap to avoid infinite loops if tasks yield back immediately
    std::vector<std::coroutine_handle<>> to_run;
    to_run.swap(ready_queue_);

    for (auto h : to_run)
    {
      if (h && !h.done())
      {
        h.resume();
      }
    }
  }

  void scheduler::run()
  {
    while (!tasks_.empty())
    {
      process_ready_tasks();
      process_events();

      tasks_.remove_if([](const auto &t) { return t.is_done(); });
    }
  }

  void scheduler::process_events()
  {
#if defined(__linux__)
    struct epoll_event evs[32];
    int n = epoll_wait(scheduler_fd_, evs, 32, 10);
    for (int i = 0; i < n; i++) {
      resume_ptr(evs[i].data.ptr);
    }
#elif defined(__APPLE__)
    struct kevent evs[32];
    struct timespec ts = {0, 10000000};
    int n = kevent(scheduler_fd_, nullptr, 0, evs, 32, &ts);
    for (int i = 0; i < n; i++) {
      resume_ptr(evs[i].udata);
    }
#endif
  }

  void scheduler::resume_ptr(void *ptr)
  {
    if (ptr) {
      std::coroutine_handle<>::from_address(ptr).resume();
    }
  }
}
