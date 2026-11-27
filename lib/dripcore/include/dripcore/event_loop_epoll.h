#ifndef DRIPCORE_EVENT_LOOP_EPOLL_H
#define DRIPCORE_EVENT_LOOP_EPOLL_H

#include <dripcore/event_loop.h>

#include <cstdint>
#include <unordered_map>

namespace dripcore
{
  // Linux implementation using epoll.
  //
  // epoll keeps a single interest-list entry per fd, so it cannot give two
  // waiters on the same fd their own registration. This backend tracks one
  // read waiter and one write waiter per fd and arms epoll once with the
  // union of their masks - the same contract the kqueue backend gets for free
  // from knotes being keyed by (fd, filter).
  class event_loop_epoll : public event_loop
  {
  public:
    event_loop_epoll();
    ~event_loop_epoll() override;

    void register_event(int fd, short events, void* udata) override;
    void deregister_event(int fd, short events, void* udata) override;
    int poll(int timeout_ms, std::function<void(void *)> resume_callback) override;
    void setup_wake_notification() override;
    void trigger_wake_notification() override;

  private:
    // At most one waiter per direction; register_event() rejects a second.
    struct fd_entry
    {
      std::uint32_t generation = 0;
      void* read_waiter = nullptr;
      void* write_waiter = nullptr;
    };

    static std::uint32_t interest_mask(const fd_entry& entry);
    static std::uint64_t event_token(int fd, std::uint32_t generation);

    // Arm or re-arm an fd with EPOLLONESHOT. New entries use ADD, with a MOD
    // fallback for a kernel entry left over from an earlier registration.
    void arm_fd(int fd, const fd_entry& entry, bool add);

    int epoll_fd_ = -1;
    int wake_event_fd_ = -1; // eventfd for wake notifications
    std::unordered_map<int, fd_entry> entries_;
    std::uint32_t next_generation_ = 0;
  };
}

#endif // DRIPCORE_EVENT_LOOP_EPOLL_H
