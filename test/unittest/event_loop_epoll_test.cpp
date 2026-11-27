#include <doctest/doctest.h>

#ifdef __linux__

#include <dripcore/event_loop_epoll.h>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <stdexcept>
#include <vector>

namespace
{
  bool make_socketpair(int sv[2])
  {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0;
  }
}

// One readable and one writable waiter on the same fd must both be armed and
// both delivered. Before the fix the second registration re-armed the fd with
// its own mask and udata, silently dropping the first waiter.
TEST_CASE("epoll: readable and writable waiters on one fd are both armed"
  * doctest::timeout(5))
{
  int sv[2];
  REQUIRE(make_socketpair(sv));

  dripcore::event_loop_epoll event_loop;
  int reader;
  int writer;
  std::vector<void*> resumed;

  event_loop.register_event(sv[0], POLLIN, &reader);
  event_loop.register_event(sv[0], POLLOUT, &writer);

  REQUIRE(write(sv[1], "x", 1) == 1);

  event_loop.poll(100, [&resumed](void* waiter) {
    resumed.push_back(waiter);
  });

  REQUIRE(resumed.size() == 2);
  CHECK(resumed[0] == &reader);
  CHECK(resumed[1] == &writer);

  close(sv[0]);
  close(sv[1]);
}

// The interface allows at most one waiter per (fd, direction). A second one
// would leave a task suspended forever, so the backend must reject it - and
// must leave the first waiter armed.
TEST_CASE("epoll: a second waiter on the same direction is rejected"
  * doctest::timeout(5))
{
  int sv[2];
  REQUIRE(make_socketpair(sv));

  dripcore::event_loop_epoll event_loop;
  int first;
  int second;
  std::vector<void*> resumed;

  event_loop.register_event(sv[0], POLLIN, &first);
  CHECK_THROWS_AS(event_loop.register_event(sv[0], POLLIN, &second),
                  std::logic_error);

  REQUIRE(write(sv[1], "x", 1) == 1);
  event_loop.poll(100, [&resumed](void* waiter) {
    resumed.push_back(waiter);
  });

  REQUIRE(resumed.size() == 1);
  CHECK(resumed[0] == &first);

  close(sv[0]);
  close(sv[1]);
}

// A callback earlier in the same epoll_wait batch can deregister an fd and
// register a replacement. The already-queued event for the old registration
// must not be delivered to the new waiter.
TEST_CASE("epoll: stale batch event does not reach a replacement registration"
  * doctest::timeout(5))
{
  int sv_1[2];
  int sv_2[2];
  REQUIRE(make_socketpair(sv_1));
  REQUIRE(make_socketpair(sv_2));

  dripcore::event_loop_epoll event_loop;
  int waiter_1;
  int waiter_2;
  int replacement;
  int callbacks = 0;

  event_loop.register_event(sv_1[0], POLLIN, &waiter_1);
  event_loop.register_event(sv_2[0], POLLIN, &waiter_2);
  REQUIRE(write(sv_1[1], "x", 1) == 1);
  REQUIRE(write(sv_2[1], "x", 1) == 1);

  // Whichever fd is delivered first, replace the other registration. The
  // other old event is already in this epoll_wait batch and must be ignored,
  // even though the fd number is now present in entries_ again.
  event_loop.poll(100, [&](void* waiter) {
    ++callbacks;
    if (waiter == &waiter_1) {
      event_loop.deregister_event(sv_2[0], POLLIN, &waiter_2);
      event_loop.register_event(sv_2[0], POLLIN, &replacement);
    } else {
      CHECK(waiter == &waiter_2);
      event_loop.deregister_event(sv_1[0], POLLIN, &waiter_1);
      event_loop.register_event(sv_1[0], POLLIN, &replacement);
    }
  });

  CHECK(callbacks == 1);

  // The replacement is still armed and receives a fresh event on the next
  // poll, proving that rejecting the stale event did not disarm it.
  event_loop.poll(100, [&](void* waiter) {
    CHECK(waiter == &replacement);
    ++callbacks;
  });
  CHECK(callbacks == 2);

  close(sv_1[0]);
  close(sv_1[1]);
  close(sv_2[0]);
  close(sv_2[1]);
}

#endif
