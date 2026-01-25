#include "doctest/doctest.h"
#include "dripcore/scheduler.h"

#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <chrono>
#include <cstdio>

// Simple benchmark helper
class simple_bench {
public:
  simple_bench(const char* name) : name_(name) {}
  
  template <typename F>
  void run(int iterations, F func) {
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
      func();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto total = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    
    std::printf("[BENCHMARK] %s: %lld ns per op (%.3f ms total)\n", 
                name_, total / iterations, total / 1000000.0);
  }

private:
  const char* name_;
};

TEST_CASE("dripcore scheduler basic test")
{
  dripcore::scheduler scheduler;

  std::atomic<bool> task_ran{false};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<bool>& task_ran) : task_ran_(task_ran) {}
  public:
    dripcore::task_handle execute() {
      task_ran_ = true;
      co_await yield();
      //co_return;
    }
  private:
    std::atomic<bool>& task_ran_;
  };

  scheduler.spawn<test_task>(task_ran);
  scheduler.run();

  CHECK(task_ran == true);
}

#if 0
TEST_CASE("dripcore scheduler sleep test")
{
  dripcore::scheduler scheduler;

  auto start = std::chrono::steady_clock::now();
  bool task_finished = false;

  // We spawn a task that captures task_finished by reference
  scheduler.spawn([&]() -> dripcore::task {
    // 50ms is enough to outrun any jitter from the loop itself
    co_await dripcore::sleep(scheduler, std::chrono::milliseconds(50));
    task_finished = true;
    co_return;
  });

  // This will now block the thread and sleep in the kernel
  scheduler.run();

  auto end = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // Debug output to see the actual delay in the test logs
  std::cout << "Sleep test elapsed: " << elapsed.count() << "ms" << std::endl;

  // Verify the task actually finished
  CHECK(task_finished == true);

  // Verify that it actually waited at least the requested time
  // (We allow a tiny margin for scheduler overhead, but it should be close to 50)
  CHECK(elapsed.count() >= 50);
}

TEST_CASE("dripcore scheduler yield test")
{
  dripcore::scheduler scheduler;
  int counter = 0;

  scheduler.spawn([&]() -> dripcore::task {
    counter++;
    co_await dripcore::yield();
    counter++;
    co_return;
  });

  scheduler.spawn([&]() -> dripcore::task {
    counter++;
    co_await dripcore::yield();
    counter++;
    co_return;
  });

  scheduler.run();

  CHECK(counter == 4);
}

TEST_CASE("dripcore scheduler many timers test")
{
  dripcore::scheduler scheduler;
  int count = 0;
  const int num_timers = 100;

  for (int i = 0; i < num_timers; ++i) {
    scheduler.spawn([&, i]() -> dripcore::task {
      co_await dripcore::sleep(scheduler, std::chrono::milliseconds(i % 10 + 1));
      count++;
      co_return;
    });
  }

  scheduler.run();
  CHECK(count == num_timers);
}
#endif

TEST_CASE("dripcore scheduler pipe communication test")
{
  dripcore::scheduler scheduler;

  // Create a pipe
  int pipefd[2];
  REQUIRE(pipe(pipefd) == 0);

  // Make pipe non-blocking
  fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
  fcntl(pipefd[1], F_SETFL, O_NONBLOCK);

  std::string received_message;
  bool writer_done = false;
  bool reader_done = false;

  // Writer task - writes to pipe
  class writer_task : public dripcore::task
  {
  public:
    writer_task(int fd, bool& done) : write_fd_(fd), done_(done) {}

    dripcore::task_handle execute() {
      const char* msg = "Hello from writer!";
      
      // Wait until pipe is writable
      co_await writable(write_fd_);
      
      ssize_t written = write(write_fd_, msg, strlen(msg));
      CHECK(written > 0);
      
      close(write_fd_);
      done_ = true;
      co_return;
    }

  private:
    int write_fd_;
    bool& done_;
  };

  // Reader task - reads from pipe
  class reader_task : public dripcore::task
  {
  public:
    reader_task(int fd, std::string& result, bool& done) 
      : read_fd_(fd), result_(result), done_(done) {}

    dripcore::task_handle execute() {
      char buffer[256];
      
      // Wait until pipe is readable
      co_await readable(read_fd_);
      
      ssize_t n = read(read_fd_, buffer, sizeof(buffer) - 1);
      if (n > 0) {
        buffer[n] = '\0';
        result_ = buffer;
      }
      
      close(read_fd_);
      done_ = true;
      co_return;
    }

  private:
    int read_fd_;
    std::string& result_;
    bool& done_;
  };

  // Spawn both tasks
  scheduler.spawn<writer_task>(pipefd[1], writer_done);
  scheduler.spawn<reader_task>(pipefd[0], received_message, reader_done);

  // Run the scheduler
  scheduler.run();

  // Verify both tasks completed
  CHECK(writer_done == true);
  CHECK(reader_done == true);
  CHECK(received_message == "Hello from writer!");
}

TEST_CASE("benchmark: scheduler timeout flaw - yield-only tasks")
{
  // This benchmark demonstrates the flaw in the current scheduler:
  // The scheduler always waits for the poll timeout (10ms) even when
  // there are no I/O events to wait for, only ready tasks.
  //
  // Expected behavior: Tasks that only yield should complete quickly
  // Actual behavior: Each scheduler loop iteration incurs ~10ms overhead

  simple_bench bench("spawn_and_yield_tasks");
  
  bench.run(1, []() {
    dripcore::scheduler scheduler;
    
    int completed_count = 0;
    const int num_tasks = 100;
    
    class yield_task : public dripcore::task
    {
    public:
      yield_task(int& counter) : counter_(counter) {}
      
      dripcore::task_handle execute() {
        // Just yield a few times and finish
        co_await yield();
        co_await yield();
        co_await yield();
        counter_++;
        co_return;
      }
      
    private:
      int& counter_;
    };
    
    // Spawn many tasks that only yield (no I/O)
    for (int i = 0; i < num_tasks; ++i) {
      scheduler.spawn<yield_task>(completed_count);
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    scheduler.run();
    auto end = std::chrono::high_resolution_clock::now();
    
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::printf("  Completed %d tasks in %lld ms\n", completed_count, elapsed_ms);
    std::printf("  Average: %.2f ms per task\n", (double)elapsed_ms / num_tasks);
    
    // Verify all tasks completed
    CHECK(completed_count == num_tasks);
    
    // The flaw: With 100 tasks yielding 3 times each, we have ~300 scheduler
    // iterations. Each iteration waits up to 10ms in poll, even though there
    // are ready tasks to run. This can add hundreds of milliseconds of overhead.
    //
    // Ideal: < 10ms total (no I/O waiting needed)
    // Current: Likely 30-100ms+ due to polling timeouts
    
    if (elapsed_ms > 50) {
      std::printf("  WARNING: Scheduler took %lld ms - demonstrating timeout flaw!\n", elapsed_ms);
      std::printf("  The scheduler waits for poll timeout even when no I/O is pending.\n");
    }
  });
}

TEST_CASE("benchmark: single task spawn latency")
{
  // This benchmark measures the latency of spawning a single task.
  // The flaw: Even a single task that just yields and finishes will
  // take at least 10ms due to the poll timeout in process_events().
  
  std::printf("\n[LATENCY TEST] Single task spawn -> yield -> complete\n");
  std::printf("Expected: < 1ms (no I/O, just immediate scheduling)\n");
  std::printf("Actual with 10ms poll timeout:\n\n");
  
  for (int run = 0; run < 5; ++run) {
    dripcore::scheduler scheduler;
    bool task_done = false;
    
    class simple_task : public dripcore::task
    {
    public:
      simple_task(bool& done) : done_(done) {}
      
      dripcore::task_handle execute() {
        co_await yield();
        done_ = true;
        co_return;
      }
      
    private:
      bool& done_;
    };
    
    auto start = std::chrono::high_resolution_clock::now();
    scheduler.spawn<simple_task>(task_done);
    scheduler.run();
    auto end = std::chrono::high_resolution_clock::now();
    
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    
    std::printf("  Run %d: %lld μs (%.3f ms)\n", run + 1, elapsed_us, elapsed_us / 1000.0);
    CHECK(task_done == true);
  }
  
  std::printf("\nFLAW EXPLANATION:\n");
  std::printf("The scheduler.run() loop calls process_events() which waits\n");
  std::printf("for the poll timeout (10ms) even when there are no I/O events\n");
  std::printf("registered. This adds unnecessary latency to pure CPU-bound tasks.\n");
}

TEST_CASE("I/O tasks are not starved by CPU-bound tasks")
{
  // This test ensures that I/O tasks get a chance to run even when
  // CPU-bound tasks keep yielding
  
  dripcore::scheduler scheduler;
  
  int pipefd[2];
  REQUIRE(pipe(pipefd) == 0);
  fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
  fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
  
  bool io_task_completed = false;
  int cpu_task_yields = 0;
  const int max_yields = 100;
  
  // CPU-bound task that yields many times
  class cpu_task : public dripcore::task
  {
  public:
    cpu_task(int& counter, int max) : counter_(counter), max_(max) {}
    
    dripcore::task_handle execute() {
      while (counter_ < max_) {
        counter_++;
        co_await yield();
      }
      co_return;
    }
    
  private:
    int& counter_;
    int max_;
  };
  
  // I/O task that waits for pipe to be readable
  class io_task : public dripcore::task
  {
  public:
    io_task(int read_fd, int write_fd, bool& done) 
      : read_fd_(read_fd), write_fd_(write_fd), done_(done) {}
    
    dripcore::task_handle execute() {
      // First write to the pipe so there's data to read
      const char* msg = "test";
      write(write_fd_, msg, strlen(msg));
      close(write_fd_);
      
      // Now wait for it to be readable
      co_await readable(read_fd_);
      
      char buf[10];
      read(read_fd_, buf, sizeof(buf));
      close(read_fd_);
      
      done_ = true;
      co_return;
    }
    
  private:
    int read_fd_;
    int write_fd_;
    bool& done_;
  };
  
  // Spawn both tasks - CPU task first, then I/O task
  scheduler.spawn<cpu_task>(cpu_task_yields, max_yields);
  scheduler.spawn<io_task>(pipefd[0], pipefd[1], io_task_completed);
  
  scheduler.run();
  
  // Both tasks should complete
  CHECK(cpu_task_yields == max_yields);
  CHECK(io_task_completed == true);
  
  std::printf("\nI/O fairness test passed:\n");
  std::printf("  CPU task yielded %d times\n", cpu_task_yields);
  std::printf("  I/O task still completed successfully\n");
}
