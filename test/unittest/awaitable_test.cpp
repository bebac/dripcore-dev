#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"
#include "dripcore/awaitable.h"

#include <atomic>
#include <string>
#include <memory>

// ============================================================================
// Basic Tests
// ============================================================================

TEST_CASE("awaitable: basic void composition")
{
  dripcore::scheduler sched;
  std::atomic<bool> completed{false};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<bool>& completed) : completed_(completed) {}

    dripcore::task_handle execute()
    {
      auto helper_coroutine = []() -> dripcore::awaitable<> {
        co_return;
      };

      co_await helper_coroutine();
      completed_ = true;
      co_return;
    }

  private:
    std::atomic<bool>& completed_;
  };

  sched.spawn<test_task>(completed);
  sched.run();

  CHECK(completed == true);
}

TEST_CASE("awaitable: basic value returning")
{
  dripcore::scheduler sched;
  std::atomic<int> result{0};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<int>& result) : result_(result) {}

    dripcore::task_handle execute()
    {
      auto return_value_coroutine = []() -> dripcore::awaitable<int> {
        co_return 42;
      };

      int val = co_await return_value_coroutine();
      result_ = val;
      co_return;
    }

  private:
    std::atomic<int>& result_;
  };

  sched.spawn<test_task>(result);
  sched.run();

  CHECK(result == 42);
}

// ============================================================================
// Nested Awaiting Tests
// ============================================================================

TEST_CASE("awaitable: nested awaiting")
{
  dripcore::scheduler sched;
  std::atomic<int> result{0};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<int>& result) : result_(result) {}

    dripcore::task_handle execute()
    {
      struct nested_helper {
        dripcore::awaitable<int> innermost() {
          co_return 100;
        }
        dripcore::awaitable<int> middle() {
          int val = co_await innermost();
          co_return val * 2;
        }
        dripcore::awaitable<int> outermost() {
          int val = co_await middle();
          co_return val + 50;
        }
      };

      nested_helper h;
      result_ = co_await h.outermost();
      co_return;
    }

  private:
    std::atomic<int>& result_;
  };

  sched.spawn<test_task>(result);
  sched.run();

  CHECK(result == 250);
}

// ============================================================================
// Exception Handling Tests
// ============================================================================

TEST_CASE("awaitable: exception propagation")
{
  dripcore::scheduler sched;
  std::atomic<bool> exception_caught{false};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<bool>& caught) : caught_(caught) {}

    dripcore::task_handle execute()
    {
      auto throwing_coroutine = []() -> dripcore::awaitable<> {
        throw std::runtime_error("test error");
        co_return;
      };

      try
      {
        co_await throwing_coroutine();
      }
      catch (const std::exception& e)
      {
        caught_ = true;
      }
      co_return;
    }

  private:
    std::atomic<bool>& caught_;
  };

  sched.spawn<test_task>(exception_caught);
  sched.run();

  CHECK(exception_caught == true);
}

// ============================================================================
// Complex Types Tests
// ============================================================================

TEST_CASE("awaitable: std::string return type")
{
  dripcore::scheduler sched;
  std::string result;
  std::mutex result_mutex;

  class test_task : public dripcore::task
  {
  public:
    test_task(std::string& result, std::mutex& mtx) : result_(result), result_mutex_(mtx) {}

    dripcore::task_handle execute()
    {
      auto return_string = []() -> dripcore::awaitable<std::string> {
        co_return std::string("hello world");
      };

      std::string str = co_await return_string();
      std::lock_guard<std::mutex> lock(result_mutex_);
      result_ = str;
      co_return;
    }

  private:
    std::string& result_;
    std::mutex& result_mutex_;
  };

  sched.spawn<test_task>(result, result_mutex);
  sched.run();

  {
    std::lock_guard<std::mutex> lock(result_mutex);
    CHECK(result == "hello world");
  }
}

TEST_CASE("awaitable: move-only type")
{
  dripcore::scheduler sched;
  std::atomic<int> result{0};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<int>& result) : result_(result) {}

    dripcore::task_handle execute()
    {
      auto return_unique_ptr = []() -> dripcore::awaitable<std::unique_ptr<int>> {
        co_return std::make_unique<int>(99);
      };

      auto ptr = co_await return_unique_ptr();
      result_ = *ptr;
      co_return;
    }

  private:
    std::atomic<int>& result_;
  };

  sched.spawn<test_task>(result);
  sched.run();

  CHECK(result == 99);
}

// ============================================================================
// Backward Compatibility Tests
// ============================================================================

TEST_CASE("awaitable: backward compatibility with existing tasks")
{
  dripcore::scheduler sched;
  std::atomic<bool> completed{false};

  class old_task : public dripcore::task
  {
  public:
    old_task(std::atomic<bool>& completed) : completed_(completed) {}

    dripcore::task_handle execute()
    {
      completed_ = true;
      co_await yield();
      co_return;
    }

  private:
    std::atomic<bool>& completed_;
  };

  sched.spawn<old_task>(completed);
  sched.run();

  CHECK(completed == true);
}

TEST_CASE("awaitable: mixing old and new style")
{
  dripcore::scheduler sched;
  std::atomic<bool> old_completed{false};
  std::atomic<bool> new_completed{false};

  class old_style_task : public dripcore::task
  {
  public:
    old_style_task(std::atomic<bool>& completed) : completed_(completed) {}

    dripcore::task_handle execute()
    {
      co_await yield();
      completed_ = true;
      co_return;
    }

  private:
    std::atomic<bool>& completed_;
  };

  class new_style_task : public dripcore::task
  {
  public:
    new_style_task(std::atomic<bool>& completed) : completed_(completed) {}

    dripcore::task_handle execute()
    {
      co_await []() -> dripcore::awaitable<> {
        co_return;
      }();
      completed_ = true;
      co_return;
    }

  private:
    std::atomic<bool>& completed_;
  };

  sched.spawn<old_style_task>(old_completed);
  sched.spawn<new_style_task>(new_completed);
  sched.run();

  CHECK(old_completed == true);
  CHECK(new_completed == true);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_CASE("awaitable: integration with yield")
{
  dripcore::scheduler sched;
  std::atomic<int> counter{0};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<int>& counter) : counter_(counter) {}

    dripcore::task_handle execute()
    {
      auto async_operation = [this]() -> dripcore::awaitable<> {
        co_await yield();
        co_return;
      };

      counter_.fetch_add(1);
      co_await async_operation();
      counter_.fetch_add(1);
      co_await yield();
      counter_.fetch_add(1);
      co_return;
    }

  private:
    std::atomic<int>& counter_;
  };

  sched.spawn<test_task>(counter);
  sched.run();

  CHECK(counter == 3);
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_CASE("awaitable: many tasks with awaitables" * doctest::timeout(5))
{
  dripcore::scheduler sched;
  const int num_tasks = 100;
  std::atomic<int> completion_count{0};

  class test_task : public dripcore::task
  {
  public:
    test_task(std::atomic<int>& count) : count_(count) {}

    dripcore::task_handle execute()
    {
      co_await []() -> dripcore::awaitable<> {
        co_return;
      }();
      count_.fetch_add(1);
      co_return;
    }

  private:
    std::atomic<int>& count_;
  };

  for (int i = 0; i < num_tasks; ++i)
  {
    sched.spawn<test_task>(completion_count);
  }

  sched.run();
  CHECK(completion_count == num_tasks);
}
