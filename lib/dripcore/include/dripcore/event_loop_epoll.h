#ifndef DRIPCORE_EVENT_LOOP_EPOLL_H
#define DRIPCORE_EVENT_LOOP_EPOLL_H

#include <dripcore/event_loop.h>

namespace dripcore
{
  // Linux implementation using epoll
  class event_loop_epoll : public event_loop
  {
  public:
    event_loop_epoll();
    ~event_loop_epoll() override;

    void register_event(int fd, short events, std::coroutine_handle<> h) override;
    int poll(int timeout_ms, std::function<void(void *)> resume_callback) override;
    void setup_wake_notification() override;
    void trigger_wake_notification() override;

  private:
    int epoll_fd_ = -1;
    int wake_event_fd_ = -1; // eventfd for wake notifications
  };
}

#endif // DRIPCORE_EVENT_LOOP_EPOLL_H
