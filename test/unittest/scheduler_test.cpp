#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <cstdio>

TEST_CASE("scheduler: basic task execution")
{
  dripcore::scheduler scheduler;
  std::atomic<bool> task_ran{false};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<bool> &task_ran) : task_ran_(task_ran) {}
    dripcore::task_handle execute()
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

    dripcore::task_handle execute()
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

    dripcore::task_handle execute()
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

    dripcore::task_handle execute()
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

    dripcore::task_handle execute()
    {
      // First write to the pipe so there's data to read
      const char *msg = "test";
      write(write_fd_, msg, strlen(msg));

      // Now wait for it to be readable
      co_await readable(read_fd_);

      char buf[10];
      read(read_fd_, buf, sizeof(buf));

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