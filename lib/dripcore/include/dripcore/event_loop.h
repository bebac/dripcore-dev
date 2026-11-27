#ifndef DRIPCORE_EVENT_LOOP_H
#define DRIPCORE_EVENT_LOOP_H

#include <coroutine>
#include <functional>

namespace dripcore
{
  // Abstract interface for platform-specific I/O event multiplexing
  class event_loop
  {
  public:
    virtual ~event_loop() = default;

    // Register an I/O event (read or write) on a file descriptor.
    // udata is an opaque pointer handed back via poll()'s callback when the
    // event fires. dripcore passes the io_awaiter* so the executor can locate
    // its registry entry and enforce the once-only resume guard.
    //
    // At most one waiter may be registered per (fd, direction): one readable
    // and one writable waiter on the same fd are both fine and are armed
    // together, but two readable waiters on the same fd are not. Neither
    // backend can represent that - epoll keeps one interest-list entry per fd
    // and kqueue one knote per (fd, filter) - and two tasks racing to consume
    // the same readiness would leave one of them suspended forever. The epoll
    // backend rejects the second registration with std::logic_error; the
    // kqueue backend cannot detect it and silently replaces the first.
    virtual void register_event(int fd, short events, void* udata) = 0;

    // Remove a previously registered waiter, identified by (fd, udata)
    // (kqueue EV_DELETE / epoll EPOLL_CTL_DEL). Must be safe to call even if
    // the event already fired (ONESHOT) or was never armed; implementations
    // ignore "not found" errors.
    virtual void deregister_event(int fd, short events, void* udata) = 0;

    // Poll for I/O events and invoke callbacks for ready events
    // timeout_ms: maximum time to wait in milliseconds (0 = non-blocking, -1 = infinite)
    // Returns number of events processed
    virtual int poll(
      int timeout_ms,
      std::function<void(void *)> resume_callback
    ) = 0;

    // Setup wake notification mechanism (called once during initialization)
    virtual void setup_wake_notification() = 0;

    // Trigger wake notification to interrupt poll() from another thread
    virtual void trigger_wake_notification() = 0;
  };

  // Factory function - creates the appropriate event_loop for the current platform
  event_loop* create_event_loop();
}

#endif // DRIPCORE_EVENT_LOOP_H
