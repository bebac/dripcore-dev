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
    virtual void register_event(int fd, short events, void* udata) = 0;

    // Remove a previously registered I/O event (EV_DELETE / EPOLL_CTL_DEL).
    // Must be safe to call even if the event already fired (ONESHOT) or was
    // never armed; implementations ignore "not found" errors.
    virtual void deregister_event(int fd, short events) = 0;

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
