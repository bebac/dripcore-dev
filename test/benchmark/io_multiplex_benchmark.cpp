// Benchmarks for the "two tasks waiting on one fd" path.
//
// event_loop allows at most one waiter per (fd, direction), so the case worth
// measuring is a reader and a writer task suspended on the same fd at the same
// time. The end-to-end scenarios use only the public scheduler/task API; the
// backend scenarios drive the event_loop interface directly to isolate
// register/deregister and dispatch cost.
//
// Run with:
//   taskset -c 2 ./benchmark --test-case="benchmark: io multiplex*"
//   taskset -c 2 ./benchmark --test-case="benchmark: backend*"
//
// The backend scenarios use a small compatibility shim so the same source
// still builds against trees with the older two-argument deregister_event().

#include "doctest/doctest.h"

#include "dripcore/event_loop.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <vector>

namespace
{
  using clock_type = std::chrono::steady_clock;

  long long ns_since(clock_type::time_point start)
  {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             clock_type::now() - start).count();
  }

  long long median_of(std::vector<long long> values)
  {
    if (values.empty())
    {
      return 0;
    }
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
  }

  // Non-blocking AF_UNIX socketpair.
  bool make_socketpair(int sv[2])
  {
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
    {
      return false;
    }
    for (int i = 0; i < 2; ++i)
    {
      const int flags = ::fcntl(sv[i], F_GETFL, 0);
      if (flags >= 0)
      {
        ::fcntl(sv[i], F_SETFL, flags | O_NONBLOCK);
      }
    }
    return true;
  }

  void close_socketpair(int sv[2])
  {
    ::close(sv[0]);
    ::close(sv[1]);
  }

  // event_loop::deregister_event() gained a udata argument. Pick whichever
  // overload exists so a single benchmark source builds against both the old
  // and the new interface.
  template <typename Loop>
  void backend_deregister(Loop& loop, int fd, short events, void* udata)
  {
    if constexpr (requires { loop.deregister_event(fd, events, udata); })
    {
      loop.deregister_event(fd, events, udata);
    }
    else
    {
      loop.deregister_event(fd, events);
    }
  }

  // -------------------------------------------------------------------------
  // End-to-end scenario: one reader task + one writer task
  // -------------------------------------------------------------------------

  // Reader: waits for the fd to become readable, consumes exactly one byte,
  // then repeats. An EAGAIN just re-arms the wait (defensive; level-triggered
  // epoll should never deliver a readable event with no data behind it).
  class ping_reader : public dripcore::task
  {
  public:
    ping_reader(int fd, int iterations, std::atomic<int>& done)
      : fd_(fd), iterations_(iterations), done_(done) {}

    dripcore::awaitable<> body() override
    {
      char buf[1];
      for (int i = 0; i < iterations_; ++i)
      {
        for (;;)
        {
          co_await readable(fd_);
          const ssize_t n = ::read(fd_, buf, sizeof(buf));
          if (n > 0)
          {
            break;
          }
          if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
          {
            continue;
          }
          co_return;
        }
        done_.fetch_add(1, std::memory_order_relaxed);
      }
      co_return;
    }

  private:
    int fd_;
    int iterations_;
    std::atomic<int>& done_;
  };

  // Writer: waits for wait_fd to become writable, then pushes one byte into
  // write_fd so the reader's fd becomes readable.
  class ping_writer : public dripcore::task
  {
  public:
    ping_writer(int wait_fd, int write_fd, int iterations, std::atomic<int>& done)
      : wait_fd_(wait_fd), write_fd_(write_fd), iterations_(iterations), done_(done) {}

    dripcore::awaitable<> body() override
    {
      const char byte = 'x';
      for (int i = 0; i < iterations_; ++i)
      {
        co_await writable(wait_fd_);
        if (::write(write_fd_, &byte, 1) != 1)
        {
          co_return;
        }
        done_.fetch_add(1, std::memory_order_relaxed);
      }
      co_return;
    }

  private:
    int wait_fd_;
    int write_fd_;
    int iterations_;
    std::atomic<int>& done_;
  };

  struct ping_stats
  {
    long long min_ns = 0;
    long long median_ns = 0;
    int reader_done = 0;
    int writer_done = 0;
  };

  struct ping_pass
  {
    long long elapsed_ns = 0;
    int reader_done = 0;
    int writer_done = 0;
    bool ok = false;
  };

  // same_fd == true  -> reader and writer both wait on data_sv[0]: one fd with
  //                     two waiters, the scenario both patches fix.
  // same_fd == false -> the writer waits on its own socketpair: two fds with
  //                     one waiter each, the no-multiplexing control.
  ping_pass ping_pass_once(bool same_fd, int iterations)
  {
    ping_pass pass;

    int data_sv[2];
    int wait_sv[2];
    if (!make_socketpair(data_sv) || !make_socketpair(wait_sv))
    {
      return pass;
    }

    std::atomic<int> reader_done{0};
    std::atomic<int> writer_done{0};

    dripcore::scheduler scheduler;
    const int writer_wait_fd = same_fd ? data_sv[0] : wait_sv[0];

    const auto start = clock_type::now();
    scheduler.spawn<ping_reader>(data_sv[0], iterations, reader_done);
    scheduler.spawn<ping_writer>(writer_wait_fd, data_sv[1], iterations, writer_done);
    scheduler.run();
    pass.elapsed_ns = ns_since(start);

    pass.reader_done = reader_done.load(std::memory_order_relaxed);
    pass.writer_done = writer_done.load(std::memory_order_relaxed);
    pass.ok = true;

    close_socketpair(data_sv);
    close_socketpair(wait_sv);
    return pass;
  }

  ping_stats run_ping_bench(bool same_fd, int iterations, int runs)
  {
    ping_stats stats;

    // Untimed warm-up: first-touch page faults and CPU frequency ramp make the
    // first pass several times slower than the steady state.
    ping_pass_once(same_fd, iterations);
    ping_pass_once(same_fd, iterations);

    std::vector<long long> samples;
    samples.reserve(static_cast<std::size_t>(runs));

    for (int run = 0; run < runs; ++run)
    {
      const ping_pass pass = ping_pass_once(same_fd, iterations);
      if (!pass.ok)
      {
        std::printf("  socketpair() failed\n");
        return stats;
      }

      samples.push_back(pass.elapsed_ns);
      stats.reader_done = pass.reader_done;
      stats.writer_done = pass.writer_done;

      std::printf("  Run %d: %9.3f ms  (%8.1f ns/round-trip)  reader %d/%d  writer %d/%d\n",
                  run + 1,
                  static_cast<double>(pass.elapsed_ns) / 1e6,
                  static_cast<double>(pass.elapsed_ns) / iterations,
                  pass.reader_done, iterations,
                  pass.writer_done, iterations);
    }

    stats.min_ns = *std::min_element(samples.begin(), samples.end());
    stats.median_ns = median_of(samples);
    return stats;
  }
}

// ---------------------------------------------------------------------------
// The common path: a reader and a writer task both suspended on the same fd.
// This is the scenario that hangs before either patch is applied.
// ---------------------------------------------------------------------------

TEST_CASE("benchmark: io multiplex - reader and writer on the SAME fd")
{
  const int iterations = 5000;
  const int runs = 7;

  std::printf("\n=== [same fd] 2 tasks (reader + writer) waiting on one fd, "
              "%d round trips ===\n", iterations);

  const ping_stats stats = run_ping_bench(true, iterations, runs);

  std::printf("  min:    %.3f ms  (%.1f ns/round-trip)\n",
              static_cast<double>(stats.min_ns) / 1e6,
              static_cast<double>(stats.min_ns) / iterations);
  std::printf("  median: %.3f ms  (%.1f ns/round-trip)\n",
              static_cast<double>(stats.median_ns) / 1e6,
              static_cast<double>(stats.median_ns) / iterations);

  CHECK(stats.reader_done == iterations);
  CHECK(stats.writer_done == iterations);
}

// ---------------------------------------------------------------------------
// Control: the same two tasks, but each waiting on its own fd. The delta
// against the case above is the cost of multiplexing waiters per fd.
// ---------------------------------------------------------------------------

TEST_CASE("benchmark: io multiplex - reader and writer on SEPARATE fds (control)")
{
  const int iterations = 5000;
  const int runs = 7;

  std::printf("\n=== [separate fds] reader + writer each on their own fd, "
              "%d round trips (control) ===\n", iterations);

  const ping_stats stats = run_ping_bench(false, iterations, runs);

  std::printf("  min:    %.3f ms  (%.1f ns/round-trip)\n",
              static_cast<double>(stats.min_ns) / 1e6,
              static_cast<double>(stats.min_ns) / iterations);
  std::printf("  median: %.3f ms  (%.1f ns/round-trip)\n",
              static_cast<double>(stats.median_ns) / 1e6,
              static_cast<double>(stats.median_ns) / iterations);

  CHECK(stats.reader_done == iterations);
  CHECK(stats.writer_done == iterations);
}

// ---------------------------------------------------------------------------
// Backend micro-benchmark: cost of arming/disarming two waiters on one fd.
// fix01 re-arms epoll on every register and on every deregister that leaves
// waiters behind; fix02 only re-arms when the union interest mask changes.
// ---------------------------------------------------------------------------

TEST_CASE("benchmark: backend register/deregister churn - 2 waiters on one fd")
{
  int sv[2];
  if (!make_socketpair(sv))
  {
    CHECK(false);
    return;
  }

  std::unique_ptr<dripcore::event_loop> loop(dripcore::create_event_loop());
  loop->setup_wake_notification();

  const int rounds = 50000;
  const int warmup_rounds = 5000;
  int reader_token = 0;
  int writer_token = 0;

  auto one_round = [&]()
  {
    loop->register_event(sv[0], POLLIN, &reader_token);
    loop->register_event(sv[0], POLLOUT, &writer_token);
    backend_deregister(*loop, sv[0], POLLOUT, &writer_token);
    backend_deregister(*loop, sv[0], POLLIN, &reader_token);
  };

  for (int r = 0; r < warmup_rounds; ++r)
  {
    one_round();
  }

  const auto start = clock_type::now();
  for (int r = 0; r < rounds; ++r)
  {
    one_round();
  }
  const long long elapsed = ns_since(start);

  std::printf("\n=== [backend] register+deregister churn, 2 waiters on one fd, "
              "%d rounds ===\n", rounds);
  std::printf("  total: %.3f ms  (%.1f ns/round)\n",
              static_cast<double>(elapsed) / 1e6,
              static_cast<double>(elapsed) / rounds);

  close_socketpair(sv);
}

// ---------------------------------------------------------------------------
// Backend micro-benchmark: dispatch cost for the two-waiter case. One reader
// and one writer on the same fd both fire from a single epoll_wait, so this
// measures the per-event bookkeeping plus the two callbacks.
// ---------------------------------------------------------------------------

TEST_CASE("benchmark: backend dispatch - reader and writer on one fd")
{
  const int rounds = 20000;
  const int warmup_rounds = 2000;

  std::printf("\n=== [backend] dispatch: reader + writer on one fd, "
              "%d rounds ===\n", rounds);

  int sv[2];
  if (!make_socketpair(sv))
  {
    CHECK(false);
    return;
  }

  std::unique_ptr<dripcore::event_loop> loop(dripcore::create_event_loop());
  loop->setup_wake_notification();

  int reader_token = 0;
  int writer_token = 0;
  const char out = 'x';
  char in[8];
  long long callbacks = 0;
  int completed = 0;

  auto one_round = [&]() -> bool
  {
    loop->register_event(sv[0], POLLIN, &reader_token);
    loop->register_event(sv[0], POLLOUT, &writer_token);
    if (::write(sv[1], &out, 1) != 1)
    {
      return false;
    }
    loop->poll(100, [&callbacks](void*) { ++callbacks; });
    return ::read(sv[0], in, sizeof(in)) > 0;
  };

  for (int r = 0; r < warmup_rounds; ++r)
  {
    if (!one_round())
    {
      break;
    }
  }
  callbacks = 0;

  const auto start = clock_type::now();
  for (int r = 0; r < rounds; ++r)
  {
    if (!one_round())
    {
      break;
    }
    ++completed;
  }
  const long long elapsed = ns_since(start);

  const double divisor = completed > 0 ? static_cast<double>(completed) : 1.0;
  std::printf("  %9.3f ms total, %8.1f ns/round, %5.2f callbacks/round\n",
              static_cast<double>(elapsed) / 1e6,
              static_cast<double>(elapsed) / divisor,
              static_cast<double>(callbacks) / divisor);

  // Both waiters must be delivered from the single fd event.
  CHECK(callbacks == static_cast<long long>(completed) * 2);

  close_socketpair(sv);
}
