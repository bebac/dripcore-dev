#include <dripcore/event_loop_epoll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <poll.h>
#include <cstdint>
#include <stdexcept>
#include <system_error>

namespace dripcore
{
  namespace
  {
    // Sentinel stored in epoll_event::data.u64 for the wake eventfd. Normal
    // I/O events encode an fd and generation, and valid fds are non-negative,
    // so this value cannot collide with an I/O token.
    constexpr std::uint64_t wake_sentinel = ~std::uint64_t{0};
  }

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

  std::uint32_t event_loop_epoll::interest_mask(const fd_entry& entry)
  {
    std::uint32_t mask = 0;
    if (entry.read_waiter) mask |= EPOLLIN;
    if (entry.write_waiter) mask |= EPOLLOUT;
    return mask;
  }

  std::uint64_t event_loop_epoll::event_token(int fd, std::uint32_t generation)
  {
    return (static_cast<std::uint64_t>(generation) << 32)
      | static_cast<std::uint32_t>(fd);
  }

  void event_loop_epoll::arm_fd(int fd, const fd_entry& entry, bool add)
  {
    struct epoll_event ev{};

    ev.events = interest_mask(entry) | EPOLLONESHOT;
    ev.data.u64 = event_token(fd, entry.generation);

    if (epoll_ctl(epoll_fd_, add ? EPOLL_CTL_ADD : EPOLL_CTL_MOD, fd, &ev) == -1) {
      // EPOLLONESHOT disables a fired entry but leaves it in the interest
      // set, so an ADD for a replacement entry can still require a MOD.
      if (add && errno == EEXIST) {
        (void)epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
      }
    }
  }

  void event_loop_epoll::register_event(int fd, short events, void* udata)
  {
    auto [it, inserted] = entries_.try_emplace(fd);
    auto& entry = it->second;
    if (inserted) {
      entry.generation = ++next_generation_;
    }

    void*& slot = (events & POLLIN) ? entry.read_waiter : entry.write_waiter;

    if (slot) {
      // Re-registering the same waiter is harmless: it is already armed.
      if (slot == udata) {
        return;
      }
      // A second waiter on the same direction is a programming error - two
      // tasks racing to consume the same readiness, one of which would stay
      // suspended forever. Fail loudly instead. A freshly inserted entry has
      // null slots, so this path never leaves a half-built entry behind.
      throw std::logic_error(
        "dripcore: two waiters registered on the same fd and direction"
      );
    }

    slot = udata;

    // Adding a waiter always widens the union mask, so the fd must be armed:
    // ADD for a new entry, MOD for one already in the interest set.
    arm_fd(fd, entry, inserted);
  }

  void event_loop_epoll::deregister_event(int fd, short events, void* udata)
  {
    auto it = entries_.find(fd);
    if (it == entries_.end()) {
      return;
    }

    auto& entry = it->second;
    void*& slot = (events & POLLIN) ? entry.read_waiter : entry.write_waiter;

    // Only the waiter we actually armed may be removed. A mismatch means the
    // caller's bookkeeping and ours have diverged; leave the registration
    // alone rather than disarming an unrelated task.
    if (slot != udata) {
      return;
    }
    slot = nullptr;

    if (entry.read_waiter == nullptr && entry.write_waiter == nullptr) {
      // Unlike kqueue's EV_ONESHOT (which removes the knote on delivery),
      // EPOLLONESHOT only disables the fd, so an explicit DEL is required to
      // fully deregister.
      (void)epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
      entries_.erase(it);
    }
    else {
      // The other direction is still waiting: drop this one from the union.
      // The MOD also re-arms the fd if its previous EPOLLONESHOT event has
      // already been delivered.
      arm_fd(fd, entry, false);
    }
  }

  int event_loop_epoll::poll(int timeout_ms, std::function<void(void*)> resume_callback)
  {
    struct epoll_event evs[32];

    // timeout_ms: -1 = infinite, 0 = non-blocking, >0 = wait up to timeout_ms
    int n = epoll_wait(epoll_fd_, evs, 32, timeout_ms);

    for (int i = 0; i < n; i++)
    {
      // Skip wake event fd - it's just for notification.
      if (evs[i].data.u64 == wake_sentinel)
      {
        // Consume the eventfd value to reset it.
        uint64_t val;
        [[maybe_unused]] auto res = read(wake_event_fd_, &val, sizeof(val));
        continue;
      }

      const auto token = evs[i].data.u64;
      const auto fd = static_cast<int>(static_cast<std::uint32_t>(token));
      const auto generation = static_cast<std::uint32_t>(token >> 32);
      auto it = entries_.find(fd);

      // A callback earlier in this batch may have deregistered this fd, or
      // replaced it with a new registration. Never deliver an old event to
      // the replacement waiter.
      if (it == entries_.end() || it->second.generation != generation) {
        continue;
      }

      auto& entry = it->second;
      const std::uint32_t fired = evs[i].events;
      const bool wake_all = fired & (EPOLLERR | EPOLLHUP);

      // Take the fired waiters out of the entry before invoking callbacks, so
      // a resumed task that immediately re-awaits the same fd lands in a clean
      // slot instead of being clobbered by this dispatch.
      void* fired_reader = nullptr;
      void* fired_writer = nullptr;
      if (wake_all || (fired & EPOLLIN)) {
        fired_reader = entry.read_waiter;
        entry.read_waiter = nullptr;
      }
      if (wake_all || (fired & EPOLLOUT)) {
        fired_writer = entry.write_waiter;
        entry.write_waiter = nullptr;
      }

      // Re-arm for whichever waiter did not fire. EPOLLONESHOT disabled the
      // entry on delivery, so this MOD re-enables it.
      if (entry.read_waiter == nullptr && entry.write_waiter == nullptr) {
        (void)epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        entries_.erase(it);
      }
      else {
        arm_fd(fd, entry, false);
      }

      if (fired_reader) {
        resume_callback(fired_reader);
      }
      if (fired_writer) {
        resume_callback(fired_writer);
      }
    }

    return n;
  }

  void event_loop_epoll::setup_wake_notification()
  {
    // Create the eventfd for wake notifications.
    wake_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_event_fd_ < 0) {
      throw std::system_error(errno, std::system_category(), "eventfd creation failed");
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.u64 = wake_sentinel;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_event_fd_, &ev) == -1) {
      throw std::system_error(errno, std::system_category(), "epoll_ctl for eventfd failed");
    }
  }

  void event_loop_epoll::trigger_wake_notification()
  {
    // Write to eventfd to wake up epoll_wait.
    uint64_t val = 1;
    // Ignore write errors (EAGAIN is fine, means it's already triggered).
    [[maybe_unused]] auto res = write(wake_event_fd_, &val, sizeof(val));
  }

  // Factory function implementation for Linux.
  event_loop* create_event_loop()
  {
    return new event_loop_epoll();
  }
}
