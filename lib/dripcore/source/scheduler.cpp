#include <dripcore/scheduler.h>
#include <system_error>
#include <algorithm>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/epoll.h>
#include <sys/eventfd.h>
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

    setup_wake_notification();
  }

  scheduler::~scheduler()
  {
#if defined(__linux__)
    if (wake_event_fd_ >= 0) {
      close(wake_event_fd_);
    }
#endif
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
    bool was_empty = ready_queue_.empty();
    ready_queue_.push_back(h);
    
    // Wake up the event loop if it was waiting
    if (was_empty) {
      trigger_wake_notification();
    }
  }

  bool scheduler::process_ready_tasks()
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
    
    return !to_run.empty();
  }

  void scheduler::run()
  {
    while (!tasks_.empty())
    {
      bool had_ready_tasks = process_ready_tasks();
      
      // Use zero timeout if we had ready tasks or more were added during processing.
      // This ensures we don't block when there's CPU work to do, while still
      // checking for I/O events to prevent starvation.
      bool use_zero_timeout = had_ready_tasks || !ready_queue_.empty();
      process_events(use_zero_timeout);

      tasks_.remove_if([](const auto &t) { return t.is_done(); });
    }
  }

  void scheduler::process_events(bool use_zero_timeout)
  {
#if defined(__linux__)
    struct epoll_event evs[32];
    // Use zero timeout for non-blocking poll (when we have CPU work),
    // otherwise use 1 second timeout (wake notification will interrupt)
    int timeout_ms = use_zero_timeout ? 0 : 1000;
    int n = epoll_wait(scheduler_fd_, evs, 32, timeout_ms);
    for (int i = 0; i < n; i++) {
      // Skip wake event fd - it's just for notification
      if (evs[i].data.fd == wake_event_fd_) {
        // Consume the eventfd value to reset it
        uint64_t val;
        read(wake_event_fd_, &val, sizeof(val));
        continue;
      }
      resume_ptr(evs[i].data.ptr);
    }
#elif defined(__APPLE__)
    struct kevent evs[32];
    // Use zero timeout for non-blocking poll (when we have CPU work),
    // otherwise use 1 second timeout (wake notification will interrupt)
    struct timespec ts = use_zero_timeout ? 
      (struct timespec){0, 0} :      // zero (non-blocking)
      (struct timespec){1, 0};       // 1 second
    int n = kevent(scheduler_fd_, nullptr, 0, evs, 32, &ts);
    for (int i = 0; i < n; i++) {
      // Skip wake notification event (EVFILT_USER)
      if (evs[i].filter == EVFILT_USER) {
        continue;
      }
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

  void scheduler::setup_wake_notification()
  {
#if defined(__linux__)
    // Create an eventfd for wake notifications
    wake_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_event_fd_ < 0) {
      throw std::system_error(errno, std::system_category(), "eventfd creation failed");
    }

    // Register the eventfd with epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = wake_event_fd_;  // Store fd so we can identify it
    if (epoll_ctl(scheduler_fd_, EPOLL_CTL_ADD, wake_event_fd_, &ev) == -1) {
      throw std::system_error(errno, std::system_category(), "epoll_ctl for eventfd failed");
    }
#elif defined(__APPLE__)
    // Setup EVFILT_USER for wake notifications
    // Use identifier 0 for the wake notification
    struct kevent kev;
    EV_SET(&kev, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (kevent(scheduler_fd_, &kev, 1, nullptr, 0, nullptr) == -1) {
      throw std::system_error(errno, std::system_category(), "kevent EVFILT_USER setup failed");
    }
#endif
  }

  void scheduler::trigger_wake_notification()
  {
#if defined(__linux__)
    // Write to eventfd to wake up epoll_wait
    uint64_t val = 1;
    // Ignore write errors (EAGAIN is fine, means it's already triggered)
    (void)write(wake_event_fd_, &val, sizeof(val));
#elif defined(__APPLE__)
    // Trigger the EVFILT_USER event
    struct kevent kev;
    EV_SET(&kev, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    // Ignore errors - if kevent fails, the next iteration will catch it
    (void)kevent(scheduler_fd_, &kev, 1, nullptr, 0, nullptr);
#endif
  }
}
