#ifndef DRIPCORE_EVENT_LOOP_KQUEUE_H
#define DRIPCORE_EVENT_LOOP_KQUEUE_H

#include <dripcore/event_loop.h>

namespace dripcore
{
  // macOS/BSD implementation using kqueue
  class event_loop_kqueue : public event_loop
  {
  public:
    event_loop_kqueue();
    ~event_loop_kqueue() override;

    void register_event(int fd, short events, std::coroutine_handle<> h) override;
    int poll(int timeout_ms, std::function<void(void *)> resume_callback) override;
    void setup_wake_notification() override;
    void trigger_wake_notification() override;

  private:
    int kqueue_fd_ = -1;
  };
}

#endif // DRIPCORE_EVENT_LOOP_KQUEUE_H
