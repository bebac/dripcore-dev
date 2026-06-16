#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <chrono>
#include <atomic>
#include <algorithm>
#include <string>
#include <vector>

TEST_CASE("sleep_for basic test")
{
  dripcore::scheduler scheduler;

  std::atomic<bool> task_ran{false};

  class sleep_task : public dripcore::task
  {
  public:
    sleep_task(std::atomic<bool>& flag) : flag_(flag) {}
    dripcore::coroutine execute() {
      co_await sleep_for(std::chrono::milliseconds(50));
      flag_ = true;
      co_return;
    }
  private:
    std::atomic<bool>& flag_;
  };

  auto start = std::chrono::steady_clock::now();
  scheduler.spawn<sleep_task>(task_ran);
  scheduler.run();
  auto end = std::chrono::steady_clock::now();

  CHECK(task_ran == true);

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  CHECK(elapsed.count() >= 50);
  CHECK(elapsed.count() < 200); // Shouldn't take too much longer
}

#if 0 // No free functions yet
TEST_CASE("sleep_for with free function")
{
  dripcore::scheduler scheduler;

  bool task_ran = false;

  class free_sleep_task : public dripcore::task
  {
  public:
    free_sleep_task(bool& flag) : flag_(flag) {}
    dripcore::coroutine execute() {
      co_await dripcore::sleep_for(std::chrono::milliseconds(50));
      flag_ = true;
      co_return;
    }
  private:
    bool& flag_;
  };

  scheduler.spawn<free_sleep_task>(task_ran);
  scheduler.run();

  CHECK(task_ran == true);
}
#endif

TEST_CASE("multiple sleep tasks")
{
  dripcore::scheduler scheduler;

  int counter = 0;

  class counting_task : public dripcore::task
  {
  public:
    counting_task(int& counter, int delay_ms)
      : counter_(counter), delay_ms_(delay_ms) {}
    dripcore::coroutine execute() {
      co_await sleep_for(std::chrono::milliseconds(delay_ms_));
      counter_++;
      co_return;
    }
  private:
    int& counter_;
    int delay_ms_;
  };

  scheduler.spawn<counting_task>(counter, 100);
  scheduler.spawn<counting_task>(counter, 50);
  scheduler.spawn<counting_task>(counter, 200);

  auto start = std::chrono::steady_clock::now();
  scheduler.run();
  auto end = std::chrono::steady_clock::now();

  CHECK(counter == 3);

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  // Should take at least as long as the longest sleep (200ms)
  CHECK(elapsed.count() >= 200);
  // But not too much longer
  CHECK(elapsed.count() < 400);
}

TEST_CASE("sleep_for with yield interleaving")
{
  dripcore::scheduler scheduler;

  std::vector<std::string> order;

  class interleaved_task : public dripcore::task
  {
  public:
    interleaved_task(std::vector<std::string>& order, const std::string& name, int sleep_ms)
      : order_(order), name_(name), sleep_ms_(sleep_ms) {}
    dripcore::coroutine execute() {
      order_.push_back(name_ + "-start");
      co_await sleep_for(std::chrono::milliseconds(sleep_ms_));
      order_.push_back(name_ + "-end");
      co_return;
    }
  private:
    std::vector<std::string>& order_;
    std::string name_;
    int sleep_ms_;
  };

  scheduler.spawn<interleaved_task>(order, "A", 100);
  scheduler.spawn<interleaved_task>(order, "B", 50);
  scheduler.spawn<interleaved_task>(order, "C", 0);

  scheduler.run();

  // C should finish first (0ms sleep)
  bool valid_start = (order[0] == "A-start" || order[0] == "B-start" || order[0] == "C-start");
  CHECK(valid_start);
  // All should have started
  CHECK(std::find(order.begin(), order.end(), "A-start") != order.end());
  CHECK(std::find(order.begin(), order.end(), "B-start") != order.end());
  CHECK(std::find(order.begin(), order.end(), "C-start") != order.end());
  // All should have ended
  CHECK(std::find(order.begin(), order.end(), "A-end") != order.end());
  CHECK(std::find(order.begin(), order.end(), "B-end") != order.end());
  CHECK(std::find(order.begin(), order.end(), "C-end") != order.end());
}
