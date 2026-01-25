#include "doctest/doctest.h"
#include "dripcore/scheduler.h"

#include <iostream>

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