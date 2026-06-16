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

    // Register an I/O event (read or write) on a file descriptor
    // The coroutine handle will be resumed when the event is ready
    virtual void register_event(int fd, short events, std::coroutine_handle<> h) = 0;

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
