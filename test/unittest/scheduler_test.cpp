#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"
#include "dripcore/cancelled.h"

#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstddef>
#include <chrono>

TEST_CASE("scheduler: basic task execution")
{
  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<bool> &task_ran) : task_ran_(task_ran) {}
    dripcore::awaitable<> body() override
    {
      task_ran_ = true;
      co_await yield();
      co_return;
    }

  private:
    std::atomic<bool> &task_ran_;
  };

  scheduler.spawn<test_task>(task_ran);
  scheduler.run();

  CHECK(task_ran == true);
}

class PipeFixture
{
protected:
  int pipefd[2];

  PipeFixture()
  {
    REQUIRE(pipe(pipefd) == 0);
    fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
  }

  ~PipeFixture()
  {
    if (pipefd[0] >= 0) {
      close(pipefd[0]);
    }
    if (pipefd[1] >= 0) {
      close(pipefd[1]);
    }
  }
};

TEST_CASE_FIXTURE(PipeFixture, "scheduler: pipe communication")
{
  dripcore::scheduler executor;

  std::atomic<bool> writer_done = false;
  std::atomic<bool> reader_done = false;

  // Writer task - writes to pipe
  class writer_task : public dripcore::task
  {
  public:
    writer_task(int fd, std::atomic<bool>& done) : write_fd_(fd), done_(done) {}

    dripcore::awaitable<> body() override
    {
      const char *msg = "Hello from writer!";

      // Wait until pipe is writable
      co_await writable(write_fd_);

      ssize_t written = write(write_fd_, msg, strlen(msg));
      CHECK(written > 0);

      done_ = true;
      co_return;
    }

  private:
    int write_fd_;
    std::atomic<bool>& done_;
  };

  // Reader task - reads from pipe
  class reader_task : public dripcore::task
  {
  public:
    reader_task(int fd, std::atomic<bool>& done)
        : read_fd_(fd), done_(done) {}

    dripcore::awaitable<> body() override
    {
      char buffer[256];

      // Wait until pipe is readable
      co_await readable(read_fd_);

      ssize_t n = read(read_fd_, buffer, sizeof(buffer) - 1);
      if (n > 0)
      {
        buffer[n] = '\0';
        CHECK_EQ(buffer, std::string("Hello from writer!"));
      }

      done_ = true;
      co_return;
    }

  private:
    int read_fd_;
    std::atomic<bool>& done_;
  };

  // Spawn both tasks
  executor.spawn<writer_task>(pipefd[1], writer_done);
  executor.spawn<reader_task>(pipefd[0], reader_done);

  // Run the executor
  executor.run();

  // Verify both tasks completed
  CHECK(writer_done == true);
  CHECK(reader_done == true);
}

TEST_CASE_FIXTURE(PipeFixture, "scheduler: I/O tasks are not starved by CPU-bound tasks" * doctest::timeout(1))
{
  // This test ensures that I/O tasks get a chance to run even when
  // CPU-bound tasks keep yielding

  dripcore::scheduler executor;

  std::atomic<bool> io_task_completed = false;
  std::atomic<int> cpu_task_yields = 0;
  const int max_yields = 100;

  // CPU-bound task that yields many times
  class cpu_task : public dripcore::task
  {
  public:
    cpu_task(std::atomic<int>& counter, int max) : counter_(counter), max_(max) {}

    dripcore::awaitable<> body() override
    {
      while (counter_ < max_)
      {
        counter_++;
        co_await yield();
      }
      co_return;
    }

  private:
    std::atomic<int>& counter_;
    int max_;
  };

  // I/O task that waits for pipe to be readable
  class io_task : public dripcore::task
  {
  public:
    io_task(int read_fd, int write_fd, std::atomic<bool>& done)
        : read_fd_(read_fd), write_fd_(write_fd), done_(done) {}

    dripcore::awaitable<> body() override
    {
      // First write to the pipe so there's data to read
      const char *msg = "test";
      [[maybe_unused]] auto w = write(write_fd_, msg, strlen(msg));

      // Now wait for it to be readable
      co_await readable(read_fd_);

      char buf[10];
      [[maybe_unused]] auto r = read(read_fd_, buf, sizeof(buf));

      done_ = true;
      co_return;
    }

  private:
    int read_fd_;
    int write_fd_;
    std::atomic<bool>& done_;
  };

  // Spawn both tasks - CPU task first, then I/O task
  executor.spawn<cpu_task>(cpu_task_yields, max_yields);
  executor.spawn<io_task>(pipefd[0], pipefd[1], io_task_completed);

  executor.run();

  // Both tasks should complete
  CHECK(cpu_task_yields == max_yields);
  CHECK(io_task_completed == true);
}

TEST_CASE("scheduler: task exceptions are captured, not terminated")
{
  // This test verifies that when a task throws an exception, the process
  // does not terminate. The exception is captured in the task promise.
  // See doc/shutdown_and_cancellation_design.md §4.

  dripcore::scheduler scheduler;
  std::atomic<bool> task_started{false};
  std::atomic<bool> task_completed{false};

  class throwing_task : public dripcore::task
  {
  public:
    throwing_task(std::atomic<bool>& started, std::atomic<bool>& completed)
      : started_(started), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      started_ = true;
      co_await yield();
      throw std::runtime_error("test exception");
      completed_ = true; // should not reach here
    }

  private:
    std::atomic<bool>& started_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<throwing_task>(task_started, task_completed);
  scheduler.run(); // should not terminate

  CHECK(task_started == true);
  CHECK(task_completed == false); // exception prevented normal completion
}

TEST_CASE("scheduler: dripcore::cancelled exception is captured")
{
  // Verify that dripcore::cancelled (the cancellation exception) is captured
  // like any other exception, not terminated. This is the foundation for the
  // cancellation protocol in doc/shutdown_and_cancellation_design.md.

  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class cancelled_task : public dripcore::task
  {
  public:
    cancelled_task(std::atomic<bool>& ran) : ran_(ran) {}

    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      throw dripcore::cancelled{};
    }

  private:
    std::atomic<bool>& ran_;
  };

  scheduler.spawn<cancelled_task>(task_ran);
  scheduler.run(); // should not terminate

  CHECK(task_ran == true);
}

TEST_CASE("scheduler: exception thrown before first co_await is captured")
{
  // A task that throws before any body-level suspension is still fully handled
  // by unhandled_exception(), provided it is a real coroutine (contains a co_*
  // keyword). No executor-side try/catch is required: because initial_suspend()
  // is suspend_always, the body runs inside the coroutine's implicit try block
  // on the first resume.

  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class early_throw_task : public dripcore::task
  {
  public:
    early_throw_task(std::atomic<bool>& ran) : ran_(ran) {}

    dripcore::awaitable<> body() override
    {
      ran_ = true;
      throw std::runtime_error("thrown before first co_await");
      co_return; // unreachable; present only to make body() a coroutine
    }

  private:
    std::atomic<bool>& ran_;
  };

  scheduler.spawn<early_throw_task>(task_ran);
  scheduler.run(); // must not terminate

  CHECK(task_ran == true);
}

TEST_CASE_FIXTURE(PipeFixture,
  "scheduler: pending I/O wait is cancelled via cancel_pending"
  * doctest::timeout(2))
{
  // A task suspended on readable(fd) for a pipe that never becomes readable is
  // woken by cancel_pending(): its knote is removed (EV_DELETE) and the
  // coroutine is resumed so io_awaiter::await_resume throws dripcore::cancelled.
  // Without this, the task would block forever and run() would never return.

  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};
  std::atomic<bool> read_completed{false};
  std::atomic<std::size_t> cancelled_count{0};

  class reader_task : public dripcore::task
  {
  public:
    reader_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : fd_(fd), cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await readable(fd_);   // pipe never becomes readable
        completed_ = true;        // must not be reached
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    int fd_;
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  class canceller_task : public dripcore::task
  {
  public:
    canceller_task(std::atomic<std::size_t>& count) : count_(count) {}

    dripcore::awaitable<> body() override
    {
      // Let the reader park on its io wait first, then cancel.
      co_await sleep_for(std::chrono::milliseconds(10));
      count_ = dripcore::executor::current()->cancel_pending();
      co_return;
    }

  private:
    std::atomic<std::size_t>& count_;
  };

  scheduler.spawn<reader_task>(pipefd[0], was_cancelled, read_completed);
  scheduler.spawn<canceller_task>(cancelled_count);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(read_completed == false);
  CHECK(cancelled_count >= 1);
}

TEST_CASE("scheduler: sleeping task is cancelled via cancel_pending"
  * doctest::timeout(2))
{
  // A task blocked in a long sleep_for() is woken by cancel_pending(): the
  // timer entry is dropped and the coroutine resumes so sleep_awaiter's
  // await_resume throws dripcore::cancelled.

  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};
  std::atomic<bool> slept{false};

  class sleeper_task : public dripcore::task
  {
  public:
    sleeper_task(std::atomic<bool>& cancelled, std::atomic<bool>& slept)
      : cancelled_(cancelled), slept_(slept) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(10)); // long; should be cancelled
        slept_ = true;                                // must not be reached
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& slept_;
  };

  class canceller_task : public dripcore::task
  {
  public:
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(10));
      dripcore::executor::current()->cancel_pending();
      co_return;
    }
  };

  scheduler.spawn<sleeper_task>(was_cancelled, slept);
  scheduler.spawn<canceller_task>();
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(slept == false);
}

// Fixture for task control cancellation tests
class TaskControlFixture : public PipeFixture
{
protected:
  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};
  std::atomic<bool> operation_completed{false};

  // Helper to create a canceller task that cancels another task after a delay
  template <typename TaskType>
  class canceller_task : public dripcore::task
  {
  public:
    canceller_task(int fd, std::atomic<bool> &cancelled, std::atomic<bool> &completed)
        : fd_(fd), cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      auto target = spawn<TaskType>(fd_, cancelled_, completed_);
      co_await sleep_for(std::chrono::milliseconds(10));
      target.cancel();
      co_return;
    }

  private:
    int fd_;
    std::atomic<bool> &cancelled_;
    std::atomic<bool> &completed_;
  };
};

// Test cancellation of a task waiting on a timer (sleep)
TEST_CASE_FIXTURE(TaskControlFixture,
  "scheduler: task control cancel - timer"
  * doctest::timeout(2))
{
  class sleeper_task : public dripcore::task
  {
  public:
    sleeper_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(10)); // long; should be cancelled
        completed_ = true; // must not be reached
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<canceller_task<sleeper_task>>(-1, was_cancelled, operation_completed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(operation_completed == false);
}

// Test cancellation of a task waiting on I/O
TEST_CASE_FIXTURE(TaskControlFixture,
  "scheduler: task control cancel - io"
  * doctest::timeout(2))
{
  std::atomic<bool> was_cancelled{false};
  std::atomic<bool> read_completed{false};

  class reader_task : public dripcore::task
  {
  public:
    reader_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : fd_(fd), cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await readable(fd_); // pipe never becomes readable
        completed_ = true; // must not be reached
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    int fd_;
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<canceller_task<reader_task>>(pipefd[0], was_cancelled, read_completed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(read_completed == false);
}

// Test cancellation of a task yielding
TEST_CASE_FIXTURE(TaskControlFixture,
  "scheduler: task control cancel - yield"
  * doctest::timeout(2))
{
  class yielder_task : public dripcore::task
  {
  public:
    yielder_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        for (unsigned i = 0; i < 10000000; i++)
        {
          co_await yield();
        }
        completed_ = true; // must not be reached
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<canceller_task<yielder_task>>(-1, was_cancelled, operation_completed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(operation_completed == false);
}

TEST_CASE("task_control: done() and cancel() are safe after the task completes"
  * doctest::timeout(2))
{
  // A task_control may outlive the task it refers to. Once the task completes,
  // the executor reaps it and destroys the coroutine frame, so the control
  // block must not dereference the (now dangling) coroutine handle.
  // Regression test: this is a use-after-free if done()/cancel() inspect
  // handle_ instead of a completion flag stored in the control block.

  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class simple_task : public dripcore::task
  {
  public:
    simple_task(std::atomic<bool>& ran) : ran_(ran) {}

    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      co_return;
    }

  private:
    std::atomic<bool>& ran_;
  };

  dripcore::task_control tc = scheduler.spawn<simple_task>(task_ran);
  CHECK(tc.done() == false);

  scheduler.run();   // task completes and its frame is destroyed

  CHECK(task_ran == true);
  CHECK(tc.done() == true);   // must not touch the destroyed frame
  tc.cancel();                // must be a safe no-op
  CHECK(tc.done() == true);
}

TEST_CASE("scheduler: graceful shutdown cancels all tasks"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> sleeper_cancelled{false};

  class long_sleeper : public dripcore::task
  {
  public:
    long_sleeper(std::atomic<bool>& cancelled) : cancelled_(cancelled) {}
    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(30));
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }
  private:
    std::atomic<bool>& cancelled_;
  };

  class shutdown_initiator : public dripcore::task
  {
  public:
    shutdown_initiator(dripcore::scheduler& sched) : sched_(sched) {}
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(10));
      sched_.shutdown(std::chrono::seconds(5));
      co_return;
    }
  private:
    dripcore::scheduler& sched_;
  };

  scheduler.spawn<long_sleeper>(sleeper_cancelled);
  scheduler.spawn<shutdown_initiator>(scheduler);
  scheduler.run();

  CHECK(sleeper_cancelled == true);
}

TEST_CASE("scheduler: forced shutdown destroys tasks after deadline"
  * doctest::timeout(5))
{
  struct never_resume_awaiter
  {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) {}
    void await_resume() {}
  };

  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};

  class stubborn_task : public dripcore::task
  {
  public:
    stubborn_task(std::atomic<bool>& cancelled) : cancelled_(cancelled) {}
    dripcore::awaitable<> body() override
    {
      bool need_stubborn = false;
      try {
        co_await sleep_for(std::chrono::seconds(30));
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
        need_stubborn = true;
      }
      if (need_stubborn) {
        // Re-suspend on a non-dripcore awaiter that the executor cannot
        // cancel. The deadline will expire and the frame will be
        // force-destroyed.
        co_await never_resume_awaiter{};
      }
      co_return;
    }
  private:
    std::atomic<bool>& cancelled_;
  };

  class shutdown_initiator : public dripcore::task
  {
  public:
    shutdown_initiator(dripcore::scheduler& sched) : sched_(sched) {}
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(10));
      sched_.shutdown(std::chrono::milliseconds(50));
      co_return;
    }
  private:
    dripcore::scheduler& sched_;
  };

  scheduler.spawn<stubborn_task>(was_cancelled);
  scheduler.spawn<shutdown_initiator>(scheduler);
  scheduler.run();

  CHECK(was_cancelled == true);
}

TEST_CASE("scheduler: shutdown before run cancels tasks at first suspension"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};

  class sleeper_task : public dripcore::task
  {
  public:
    sleeper_task(std::atomic<bool>& cancelled) : cancelled_(cancelled) {}
    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(30));
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }
  private:
    std::atomic<bool>& cancelled_;
  };

  scheduler.spawn<sleeper_task>(was_cancelled);
  scheduler.shutdown(std::chrono::seconds(5));
  scheduler.run();

  CHECK(was_cancelled == true);
}

// ---------------------------------------------------------------------------
// task_control::stop() tests
//
// stop() cancels the target task and co_awaits its completion.  The caller
// resumes only after the target's final_suspend() has published finished_
// and scheduled the stop continuation via ready_task().
// ---------------------------------------------------------------------------

// Fixture for task_control stop() tests
class TaskStopFixture : public PipeFixture
{
protected:
  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};
  std::atomic<bool> operation_completed{false};
  std::atomic<bool> stopper_resumed{false};

  // Helper: spawns a target task, waits, then co_awaits stop()
  template <typename TargetTask>
  class stopper_task : public dripcore::task
  {
  public:
    stopper_task(int fd, std::atomic<bool>& cancelled,
                 std::atomic<bool>& completed, std::atomic<bool>& resumed)
      : fd_(fd), cancelled_(cancelled), completed_(completed), resumed_(resumed) {}

    dripcore::awaitable<> body() override
    {
      auto target = spawn<TargetTask>(fd_, cancelled_, completed_);
      co_await sleep_for(std::chrono::milliseconds(10));
      co_await target.stop();
      resumed_ = true;
      co_return;
    }

  private:
    int fd_;
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
    std::atomic<bool>& resumed_;
  };
};

TEST_CASE_FIXTURE(TaskStopFixture,
  "task_control: stop() cancels and waits - timer"
  * doctest::timeout(2))
{
  class sleeper_task : public dripcore::task
  {
  public:
    sleeper_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(10));
        completed_ = true;
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<stopper_task<sleeper_task>>(
    -1, was_cancelled, operation_completed, stopper_resumed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(operation_completed == false);
  CHECK(stopper_resumed == true);
}

TEST_CASE_FIXTURE(TaskStopFixture,
  "task_control: stop() cancels and waits - io"
  * doctest::timeout(2))
{
  class reader_task : public dripcore::task
  {
  public:
    reader_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : fd_(fd), cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        co_await readable(fd_);
        completed_ = true;
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    int fd_;
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<stopper_task<reader_task>>(
    pipefd[0], was_cancelled, operation_completed, stopper_resumed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(operation_completed == false);
  CHECK(stopper_resumed == true);
}

TEST_CASE_FIXTURE(TaskStopFixture,
  "task_control: stop() cancels and waits - yield"
  * doctest::timeout(2))
{
  class yielder_task : public dripcore::task
  {
  public:
    yielder_task(int fd, std::atomic<bool>& cancelled, std::atomic<bool>& completed)
      : cancelled_(cancelled), completed_(completed) {}

    dripcore::awaitable<> body() override
    {
      try {
        for (unsigned i = 0; i < 10000000; i++) {
          co_await yield();
        }
        completed_ = true;
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& cancelled_;
    std::atomic<bool>& completed_;
  };

  scheduler.spawn<stopper_task<yielder_task>>(
    -1, was_cancelled, operation_completed, stopper_resumed);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(operation_completed == false);
  CHECK(stopper_resumed == true);
}

TEST_CASE("task_control: stop() on already-done task returns immediately"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};
  std::atomic<bool> stopper_resumed{false};

  class quick_task : public dripcore::task
  {
  public:
    quick_task(std::atomic<bool>& ran) : ran_(ran) {}

    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      co_return;
    }

  private:
    std::atomic<bool>& ran_;
  };

  class stopper_task : public dripcore::task
  {
  public:
    stopper_task(std::atomic<bool>& ran, std::atomic<bool>& resumed)
      : ran_(ran), resumed_(resumed) {}

    dripcore::awaitable<> body() override
    {
      auto target = spawn<quick_task>(ran_);
      co_await sleep_for(std::chrono::milliseconds(50));
      CHECK(target.done() == true);
      co_await target.stop();
      resumed_ = true;
      co_return;
    }

  private:
    std::atomic<bool>& ran_;
    std::atomic<bool>& resumed_;
  };

  scheduler.spawn<stopper_task>(task_ran, stopper_resumed);
  scheduler.run();

  CHECK(task_ran == true);
  CHECK(stopper_resumed == true);
}

// ---------------------------------------------------------------------------
// task_control::exception() and task_control::cancelled() tests
//
// exception propagation from the task promise to the task_control_block
// lets a task_control holder observe how a task terminated after it
// completes. See doc/shutdown_and_cancellation_design.md §4.
// ---------------------------------------------------------------------------

TEST_CASE("task_control: exception() returns failure exception after task completes"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;

  class failing_task : public dripcore::task
  {
  public:
    failing_task(std::atomic<bool>& ran) : ran_(ran) {}
    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      throw std::runtime_error("task failed");
    }
  private:
    std::atomic<bool>& ran_;
  };

  std::atomic<bool> task_ran{false};
  auto tc = scheduler.spawn<failing_task>(task_ran);
  CHECK(tc.done() == false);

  scheduler.run();

  CHECK(task_ran == true);
  CHECK(tc.done() == true);
  CHECK(tc.cancelled() == false);
  REQUIRE(tc.exception() != nullptr);
  try {
    std::rethrow_exception(tc.exception());
    CHECK(false); // should not reach
  } catch (const std::runtime_error& e) {
    CHECK(std::string(e.what()) == "task failed");
  }
}

TEST_CASE("task_control: cancelled() is true and exception() is null after cancel"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> was_cancelled{false};

  class sleeper_task : public dripcore::task
  {
  public:
    sleeper_task(std::atomic<bool>& cancelled) : cancelled_(cancelled) {}
    dripcore::awaitable<> body() override
    {
      try {
        co_await sleep_for(std::chrono::seconds(30));
      } catch (const dripcore::cancelled&) {
        cancelled_ = true;
      }
      co_return;
    }
  private:
    std::atomic<bool>& cancelled_;
  };

  class canceller_task : public dripcore::task
  {
  public:
    canceller_task(dripcore::task_control& target)
      : target_(target) {}
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(10));
      target_.cancel();
      co_return;
    }
  private:
    dripcore::task_control& target_;
  };

  auto tc = scheduler.spawn<sleeper_task>(was_cancelled);
  scheduler.spawn<canceller_task>(tc);
  scheduler.run();

  CHECK(was_cancelled == true);
  CHECK(tc.done() == true);
  CHECK(tc.cancelled() == true);
  CHECK(tc.exception() == nullptr);
}

TEST_CASE("task_control: self-thrown cancelled sets cancelled() and leaves exception() null"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class self_cancel_task : public dripcore::task
  {
  public:
    self_cancel_task(std::atomic<bool>& ran) : ran_(ran) {}
    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      throw dripcore::cancelled{};
    }
  private:
    std::atomic<bool>& ran_;
  };

  auto tc = scheduler.spawn<self_cancel_task>(task_ran);
  scheduler.run();

  CHECK(task_ran == true);
  CHECK(tc.done() == true);
  CHECK(tc.cancelled() == true);
  CHECK(tc.exception() == nullptr);
}

TEST_CASE("task_control: normal completion has no exception and is not cancelled"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class simple_task : public dripcore::task
  {
  public:
    simple_task(std::atomic<bool>& ran) : ran_(ran) {}
    dripcore::awaitable<> body() override
    {
      ran_ = true;
      co_await yield();
      co_return;
    }
  private:
    std::atomic<bool>& ran_;
  };

  auto tc = scheduler.spawn<simple_task>(task_ran);
  scheduler.run();

  CHECK(task_ran == true);
  CHECK(tc.done() == true);
  CHECK(tc.cancelled() == false);
  CHECK(tc.exception() == nullptr);
}

TEST_CASE("task_control: stop() on failing task propagates exception"
  * doctest::timeout(2))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> stopper_resumed{false};

  class failing_task : public dripcore::task
  {
  public:
    failing_task(std::atomic<bool>& started) : started_(started) {}
    dripcore::awaitable<> body() override
    {
      started_ = true;
      co_await yield();
      throw std::logic_error("boom");
    }
  private:
    std::atomic<bool>& started_;
  };

  class stopper_task : public dripcore::task
  {
  public:
    stopper_task(std::atomic<bool>& resumed, dripcore::task_control& target)
      : resumed_(resumed), target_(target) {}
    dripcore::awaitable<> body() override
    {
      auto child = spawn<failing_task>(started_);
      co_await sleep_for(std::chrono::milliseconds(50));
      CHECK(child.done() == true);
      CHECK(child.cancelled() == false);
      REQUIRE(child.exception() != nullptr);
      try {
        std::rethrow_exception(child.exception());
        CHECK(false);
      } catch (const std::logic_error& e) {
        CHECK(std::string(e.what()) == "boom");
      }
      co_await child.stop();
      resumed_ = true;
      co_return;
    }
  private:
    std::atomic<bool>& resumed_;
    dripcore::task_control& target_;
    std::atomic<bool> started_{false};
  };

  dripcore::task_control tc;
  scheduler.spawn<stopper_task>(stopper_resumed, tc);
  scheduler.run();

  CHECK(stopper_resumed == true);
}