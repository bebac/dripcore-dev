#include <dripcore/event_loop_kqueue.h>
#include <sys/event.h>
#include <unistd.h>
#include <poll.h>
#include <system_error>

namespace dripcore
{
  event_loop_kqueue::event_loop_kqueue()
  {
    kqueue_fd_ = kqueue();
    if (kqueue_fd_ < 0) {
      throw std::system_error(errno, std::system_category(), "kqueue creation failed");
    }
  }

  event_loop_kqueue::~event_loop_kqueue()
  {
    if (kqueue_fd_ >= 0) {
      close(kqueue_fd_);
    }
  }

  void event_loop_kqueue::register_event(int fd, short events, void* udata)
  {
    // knotes are keyed by (fd, filter), so a second waiter on the same fd and
    // direction silently replaces the first one's udata. event_loop's contract
    // allows at most one waiter per (fd, direction); unlike the epoll backend
    // this one has no registry and so cannot detect a violation.
    struct kevent kev;
    int filter = (events & POLLIN) ? EVFILT_READ : EVFILT_WRITE;
    EV_SET(&kev, fd, filter, EV_ADD | EV_ONESHOT, 0, 0, udata);
    kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr);
  }

  void event_loop_kqueue::deregister_event(int fd, short events, void* /*udata*/)
  {
    // kqueue knotes are keyed by (fd, filter), so the waiter is implied by
    // fd+events and the udata is not needed here; readable and writable
    // waits on the same fd are separate knotes and do not interfere.
    struct kevent kev;
    int filter = (events & POLLIN) ? EVFILT_READ : EVFILT_WRITE;
    EV_SET(&kev, fd, filter, EV_DELETE, 0, 0, nullptr);
    // Ignore errors: with EV_ONESHOT the knote may already be gone, or the fd
    // may never have been armed. ENOENT here is expected and harmless.
    (void)kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr);
  }

  int event_loop_kqueue::poll(int timeout_ms, std::function<void(void*)> resume_callback)
  {
    struct kevent evs[32];

    // timeout_ms: -1 = infinite (NULL timespec), 0 = non-blocking, >0 = wait up to timeout_ms
    struct timespec ts;
    struct timespec* ts_ptr = nullptr;
    if (timeout_ms >= 0) {
      ts.tv_sec = timeout_ms / 1000;
      ts.tv_nsec = (timeout_ms % 1000) * 1000000;
      ts_ptr = &ts;
    }

    int n = kevent(kqueue_fd_, nullptr, 0, evs, 32, ts_ptr);

    for (int i = 0; i < n; i++)
    {
      // Skip wake notification event (EVFILT_USER)
      if (evs[i].filter == EVFILT_USER) {
        continue;
      }
      resume_callback(evs[i].udata);
    }

    return n;
  }

  void event_loop_kqueue::setup_wake_notification()
  {
    // Setup EVFILT_USER for wake notifications
    // Use identifier 0 for the wake notification
    struct kevent kev;
    EV_SET(&kev, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr) == -1) {
      throw std::system_error(errno, std::system_category(), "kevent EVFILT_USER setup failed");
    }
  }

  void event_loop_kqueue::trigger_wake_notification()
  {
    // Trigger the EVFILT_USER event
    struct kevent kev;
    EV_SET(&kev, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    // Ignore errors - if kevent fails, the next iteration will catch it
    (void)kevent(kqueue_fd_, &kev, 1, nullptr, 0, nullptr);
  }

  // Factory function implementation for macOS
  event_loop* create_event_loop()
  {
    return new event_loop_kqueue();
  }
}
