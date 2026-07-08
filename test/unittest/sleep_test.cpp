#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <chrono>
#include <atomic>
#include <algorithm>
#include <string>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

TEST_CASE("sleep: basic sleep_for test" * doctest::timeout(1))
{
  dripcore::scheduler scheduler;

  std::atomic<bool> task_ran{false};

  class sleep_task : public dripcore::task
  {
  public:
    sleep_task(std::atomic<bool>& flag) : flag_(flag) {}
    dripcore::task_handle execute()
    {
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

TEST_CASE("sleep: multiple sleep tasks" * doctest::timeout(1))
{
  dripcore::scheduler scheduler;

  std::atomic<int> counter = 0;

  class counting_task : public dripcore::task
  {
  public:
    counting_task(std::atomic<int>& counter, int delay_ms)
        : counter_(counter), delay_ms_(delay_ms) {}
    dripcore::task_handle execute()
    {
      co_await sleep_for(std::chrono::milliseconds(delay_ms_));
      counter_++;
      co_return;
    }

  private:
    std::atomic<int>& counter_;
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

TEST_CASE("sleep: tasks complete in order of sleep duration")
{
  dripcore::scheduler scheduler;

  struct shared_state {
    std::mutex mtx;
    std::vector<std::string> order;
  };
  auto state = std::make_shared<shared_state>();

  class sleep_task : public dripcore::task
  {
  public:
    sleep_task(std::shared_ptr<shared_state> state,
               const std::string &name, int sleep_ms)
        : state_(state), name_(name), sleep_ms_(sleep_ms) {}
    dripcore::task_handle execute()
    {
      {
        std::lock_guard<std::mutex> lock(state_->mtx);
        state_->order.push_back(name_ + "-start");
      }
      co_await sleep_for(std::chrono::milliseconds(sleep_ms_));
      {
        std::lock_guard<std::mutex> lock(state_->mtx);
        state_->order.push_back(name_ + "-end");
      }
      co_return;
    }

  private:
    std::shared_ptr<shared_state> state_;
    std::string name_;
    int sleep_ms_;
  };

  scheduler.spawn<sleep_task>(state, "A", 100);
  scheduler.spawn<sleep_task>(state, "B", 50);
  scheduler.spawn<sleep_task>(state, "C", 0);

  scheduler.run();

  auto& order = state->order;

  // All tasks should have started
  CHECK(std::find(order.begin(), order.end(), "A-start") != order.end());
  CHECK(std::find(order.begin(), order.end(), "B-start") != order.end());
  CHECK(std::find(order.begin(), order.end(), "C-start") != order.end());

  // All tasks should have ended
  CHECK(std::find(order.begin(), order.end(), "A-end") != order.end());
  CHECK(std::find(order.begin(), order.end(), "B-end") != order.end());
  CHECK(std::find(order.begin(), order.end(), "C-end") != order.end());

  // C (0ms) should finish before B (50ms) and A (100ms)
  auto c_end = std::find(order.begin(), order.end(), "C-end");
  auto b_end = std::find(order.begin(), order.end(), "B-end");
  auto a_end = std::find(order.begin(), order.end(), "A-end");
  CHECK(c_end < b_end);  // C finishes before B
  CHECK(c_end < a_end);  // C finishes before A
}
