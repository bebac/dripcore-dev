#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"
#include "dripcore/blocking_pool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <latch>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace
{
  // Gate that holds blocking-pool workers inside their job until the test
  // releases them; makes "worker in flight" deterministic. open() blocks
  // until every blocked waiter has woken and left wait(), so the gate can
  // be destroyed (or the test case can end) safely afterwards — a worker
  // still inside cv_.wait() when the gate dies is undefined behavior.
  class gate
  {
  public:
    void wait()
    {
      std::unique_lock<std::mutex> lock(mtx_);
      ++waiters_;
      cv_.wait(lock, [this] { return open_; });
      --waiters_;
      drained_cv_.notify_all();
    }

    void open()
    {
      {
        std::lock_guard<std::mutex> lock(mtx_);
        open_ = true;
      }
      cv_.notify_all();

      std::unique_lock<std::mutex> lock(mtx_);
      drained_cv_.wait(lock, [this] { return waiters_ == 0; });
    }

  private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable drained_cv_;
    bool open_ = false;
    int waiters_ = 0;
  };
} // namespace

TEST_CASE("blocking: run_blocking returns values" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  std::atomic<int> int_result{-1};
  std::atomic<bool> string_ok{false};

  class value_task : public dripcore::task
  {
  public:
    value_task(std::atomic<int>& int_result, std::atomic<bool>& string_ok)
      : int_result_(int_result), string_ok_(string_ok)
    {
    }

    dripcore::awaitable<> body() override
    {
      int_result_ = co_await run_blocking([] { return 42; });
      std::string s = co_await run_blocking([] { return std::string("hello"); });
      string_ok_ = (s == "hello");
      co_return;
    }

  private:
    std::atomic<int>& int_result_;
    std::atomic<bool>& string_ok_;
  };

  scheduler.spawn<value_task>(int_result, string_ok);
  scheduler.run();

  CHECK(int_result.load() == 42);
  CHECK(string_ok.load());
}

TEST_CASE("blocking: void jobs and exception propagation" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> void_ran{false};
  std::atomic<bool> caught{false};
  std::string message;

  class void_task : public dripcore::task
  {
  public:
    void_task(std::atomic<bool>& ran) : ran_(ran) {}
    dripcore::awaitable<> body() override
    {
      co_await run_blocking([this] { ran_ = true; });
      co_return;
    }
  private:
    std::atomic<bool>& ran_;
  };

  class throwing_task : public dripcore::task
  {
  public:
    throwing_task(std::atomic<bool>& caught, std::string& message)
      : caught_(caught), message_(message)
    {
    }
    dripcore::awaitable<> body() override
    {
      try
      {
        co_await run_blocking([]() -> int { throw std::runtime_error("boom"); });
      }
      catch (const std::runtime_error& e)
      {
        message_ = e.what();
        caught_ = true;
      }
      co_return;
    }
  private:
    std::atomic<bool>& caught_;
    std::string& message_;
  };

  scheduler.spawn<void_task>(void_ran);
  scheduler.spawn<throwing_task>(caught, message);
  scheduler.run();

  CHECK(void_ran.load());
  CHECK(caught.load());
  CHECK(message == "boom");
}

TEST_CASE("blocking: cancel while suspended resumes promptly" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  auto g = std::make_shared<gate>();
  auto worker_finished = std::make_shared<std::atomic<bool>>(false);
  std::atomic<bool> completed_after{false};

  class blocked_task : public dripcore::task
  {
  public:
    blocked_task(std::shared_ptr<gate> g, std::shared_ptr<std::atomic<bool>> worker_finished,
                 std::atomic<bool>& completed_after)
      : g_(std::move(g)), worker_finished_(std::move(worker_finished)), completed_after_(completed_after)
    {
    }
    dripcore::awaitable<> body() override
    {
      // The job is self-contained (captures by value): after cancellation
      // it outlives this task object, which is destroyed while the
      // worker is still blocked on the gate.
      co_await run_blocking([g = g_, wf = worker_finished_] {
        g->wait();
        wf->store(true); // runs to completion; result discarded
      });
      completed_after_ = true; // must not run
      co_return;
    }
  private:
    std::shared_ptr<gate> g_;
    std::shared_ptr<std::atomic<bool>> worker_finished_;
    std::atomic<bool>& completed_after_;
  };

  class cancel_driver : public dripcore::task
  {
  public:
    cancel_driver(dripcore::task_control ctrl, std::shared_ptr<gate> g,
                  std::shared_ptr<std::atomic<bool>> worker_finished)
      : ctrl_(ctrl), g_(std::move(g)), worker_finished_(std::move(worker_finished))
    {
    }
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(50));
      ctrl_.cancel();
      g_->open();
      // Wait for the abandoned worker to run to completion so no worker
      // outlives run(); its result must be discarded, not delivered.
      for (int i = 0; i < 500 && !worker_finished_->load(); ++i)
      {
        co_await sleep_for(std::chrono::milliseconds(1));
      }
      co_return;
    }
  private:
    dripcore::task_control ctrl_;
    std::shared_ptr<gate> g_;
    std::shared_ptr<std::atomic<bool>> worker_finished_;
  };

  auto ctrl = scheduler.spawn<blocked_task>(g, worker_finished, completed_after);
  scheduler.spawn<cancel_driver>(ctrl, g, worker_finished);
  scheduler.run();

  CHECK(ctrl.done());
  CHECK(ctrl.cancelled());
  CHECK_FALSE(completed_after.load());
  CHECK(worker_finished->load()); // worker ran to completion, result discarded
}

TEST_CASE("blocking: cancel before suspension never posts the job" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  std::atomic<bool> job_ran{false};
  std::atomic<bool> after_await{false};

  class target_task : public dripcore::task
  {
  public:
    target_task(std::atomic<bool>& job_ran, std::atomic<bool>& after_await)
      : job_ran_(job_ran), after_await_(after_await)
    {
    }
    dripcore::awaitable<> body() override
    {
      co_await yield();
      co_await run_blocking([this] { job_ran_ = true; });
      after_await_ = true; // must not run
      co_return;
    }
  private:
    std::atomic<bool>& job_ran_;
    std::atomic<bool>& after_await_;
  };

  class cancel_driver : public dripcore::task
  {
  public:
    cancel_driver(dripcore::task_control ctrl) : ctrl_(ctrl) {}
    dripcore::awaitable<> body() override
    {
      ctrl_.cancel();
      co_return;
    }
  private:
    dripcore::task_control ctrl_;
  };

  auto ctrl = scheduler.spawn<target_task>(job_ran, after_await);
  scheduler.spawn<cancel_driver>(ctrl);
  scheduler.run();

  CHECK(ctrl.done());
  CHECK(ctrl.cancelled());
  CHECK_FALSE(job_ran.load());
  CHECK_FALSE(after_await.load());
}

TEST_CASE("blocking: deadline expiry cancels the await" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  auto g = std::make_shared<gate>();
  auto worker_finished = std::make_shared<std::atomic<bool>>(false);
  std::atomic<bool> threw_cancelled{false};

  class timeout_task : public dripcore::task
  {
  public:
    timeout_task(std::shared_ptr<gate> g, std::shared_ptr<std::atomic<bool>> worker_finished,
                 std::atomic<bool>& threw)
      : g_(std::move(g)), worker_finished_(std::move(worker_finished)), threw_(threw)
    {
    }
    dripcore::awaitable<> body() override
    {
      try
      {
        // Self-contained job: it outlives this task, which is destroyed
        // at the deadline while the worker is still blocked on the gate.
        co_await run_blocking(
          [g = g_, wf = worker_finished_] {
            g->wait();
            wf->store(true);
          },
          std::chrono::milliseconds(50));
      }
      catch (const dripcore::cancelled&)
      {
        threw_ = true;
      }
      co_return;
    }
  private:
    std::shared_ptr<gate> g_;
    std::shared_ptr<std::atomic<bool>> worker_finished_;
    std::atomic<bool>& threw_;
  };

  // Releases the abandoned worker after the deadline has fired and waits
  // for it to run to completion, so no worker outlives run().
  class release_driver : public dripcore::task
  {
  public:
    release_driver(std::shared_ptr<gate> g, std::shared_ptr<std::atomic<bool>> worker_finished)
      : g_(std::move(g)), worker_finished_(std::move(worker_finished))
    {
    }
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(200));
      g_->open();
      for (int i = 0; i < 500 && !worker_finished_->load(); ++i)
      {
        co_await sleep_for(std::chrono::milliseconds(1));
      }
      co_return;
    }
  private:
    std::shared_ptr<gate> g_;
    std::shared_ptr<std::atomic<bool>> worker_finished_;
  };

  auto start = std::chrono::steady_clock::now();
  scheduler.spawn<timeout_task>(g, worker_finished, threw_cancelled);
  scheduler.spawn<release_driver>(g, worker_finished);
  scheduler.run();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - start);

  CHECK(threw_cancelled.load());
  CHECK(worker_finished->load()); // worker ran to completion, result discarded
  CHECK(elapsed.count() < 2000);
}

TEST_CASE("blocking: deadline does not fire when the job completes first" * doctest::timeout(5))
{
  dripcore::scheduler scheduler;
  std::atomic<int> result{-1};

  class fast_task : public dripcore::task
  {
  public:
    fast_task(std::atomic<int>& result) : result_(result) {}
    dripcore::awaitable<> body() override
    {
      result_ = co_await run_blocking([] { return 7; }, std::chrono::seconds(10));
      co_return;
    }
  private:
    std::atomic<int>& result_;
  };

  auto start = std::chrono::steady_clock::now();
  scheduler.spawn<fast_task>(result);
  scheduler.run();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - start);

  CHECK(result.load() == 7);
  CHECK(elapsed.count() < 2000);
}

TEST_CASE("blocking: concurrent run_blocking jobs" * doctest::timeout(10))
{
  dripcore::scheduler scheduler;
  std::atomic<int> sum{0};

  class square_task : public dripcore::task
  {
  public:
    square_task(int i, std::atomic<int>& sum) : i_(i), sum_(sum) {}
    dripcore::awaitable<> body() override
    {
      int v = co_await run_blocking([this] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return i_ * i_;
      });
      sum_ += v;
      co_return;
    }
  private:
    int i_;
    std::atomic<int>& sum_;
  };

  for (int i = 0; i < 4; ++i)
  {
    scheduler.spawn<square_task>(i, sum);
  }
  scheduler.run();

  CHECK(sum.load() == 0 + 1 + 4 + 9);
}

TEST_CASE("blocking: shutdown is not delayed by blocked workers" * doctest::timeout(10))
{
  dripcore::scheduler scheduler;
  // Shared ownership: the abandoned workers hold the gate past run() and
  // past the end of the test case; open() drains them before the last
  // reference goes away.
  auto g = std::make_shared<gate>();
  std::atomic<int> fast_result{-1};

  // Completes long before its deadline; exercises deadline-entry cleanup
  // on the completion path.
  class fast_with_deadline_task : public dripcore::task
  {
  public:
    fast_with_deadline_task(std::atomic<int>& result) : result_(result) {}
    dripcore::awaitable<> body() override
    {
      result_ = co_await run_blocking([] { return 3; }, std::chrono::seconds(10));
      co_return;
    }
  private:
    std::atomic<int>& result_;
  };

  class blocked_task : public dripcore::task
  {
  public:
    blocked_task(std::shared_ptr<gate> g) : g_(std::move(g)) {}
    dripcore::awaitable<> body() override
    {
      // Capture the gate by value: this task object is destroyed while
      // the abandoned worker is still blocked on the gate.
      co_await run_blocking([g = g_] { g->wait(); });
      co_return;
    }
  private:
    std::shared_ptr<gate> g_;
  };

  // Blocked past shutdown with a live deadline entry; exercises the
  // cancel_pending path where the expiry hook runs after the blocking
  // walk already cancelled the awaiter (claim must be a no-op).
  class blocked_with_deadline_task : public dripcore::task
  {
  public:
    blocked_with_deadline_task(std::shared_ptr<gate> g) : g_(std::move(g)) {}
    dripcore::awaitable<> body() override
    {
      co_await run_blocking([g = g_] { g->wait(); }, std::chrono::seconds(10));
      co_return;
    }
  private:
    std::shared_ptr<gate> g_;
  };

  class shutdown_driver : public dripcore::task
  {
  public:
    shutdown_driver(dripcore::scheduler& sched) : sched_(sched) {}
    dripcore::awaitable<> body() override
    {
      co_await sleep_for(std::chrono::milliseconds(50));
      sched_.shutdown(std::chrono::milliseconds(500));
      co_return;
    }
  private:
    dripcore::scheduler& sched_;
  };

  scheduler.spawn<fast_with_deadline_task>(fast_result);
  auto blocked_ctrl = scheduler.spawn<blocked_task>(g);
  auto blocked_deadline_ctrl = scheduler.spawn<blocked_with_deadline_task>(g);
  scheduler.spawn<shutdown_driver>(scheduler);

  auto start = std::chrono::steady_clock::now();
  scheduler.run();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - start);

  CHECK(fast_result.load() == 3);
  CHECK(blocked_ctrl.done());
  CHECK(blocked_ctrl.cancelled());
  CHECK(blocked_deadline_ctrl.done());
  CHECK(blocked_deadline_ctrl.cancelled());
  // The drain must finish on the cancel path, not by waiting out the
  // blocked workers.
  CHECK(elapsed.count() < 2000);

  g->open(); // drain the abandoned workers so the gate can die safely
}

TEST_CASE("blocking: cancel racing completion never double-resumes" * doctest::timeout(30))
{
  // The cancel lands while the worker is mid-job, so the single-resume
  // claim is genuinely raced every iteration. Either outcome (result
  // delivered, or cancelled) is correct; a double resume would crash or
  // be caught by the sanitizers.
  for (int i = 0; i < 25; ++i)
  {
    dripcore::scheduler scheduler;
    std::atomic<bool> completed{false};

    class racer_task : public dripcore::task
    {
    public:
      racer_task(std::atomic<bool>& completed) : completed_(completed) {}
      dripcore::awaitable<> body() override
      {
        int v = co_await run_blocking([] {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          return 1;
        });
        completed_ = (v == 1);
        co_return;
      }
    private:
      std::atomic<bool>& completed_;
    };

    class cancel_task : public dripcore::task
    {
    public:
      cancel_task(dripcore::task_control ctrl) : ctrl_(ctrl) {}
      dripcore::awaitable<> body() override
      {
        ctrl_.cancel();
        co_return;
      }
    private:
      dripcore::task_control ctrl_;
    };

    auto ctrl = scheduler.spawn<racer_task>(completed);
    scheduler.spawn<cancel_task>(ctrl);
    scheduler.run();

    REQUIRE(ctrl.done());
  }
}

TEST_CASE("blocking_pool: runs posted jobs and accepts move-only callables" * doctest::timeout(5))
{
  dripcore::blocking_pool pool(2);
  std::atomic<int> count{0};
  std::latch done(4);

  for (int i = 0; i < 4; ++i)
  {
    pool.post([&] {
      ++count;
      done.count_down();
    });
  }
  done.wait();
  CHECK(count.load() == 4);

  // Move-only job (std::function would reject this).
  std::latch done2(1);
  std::atomic<int> moved_value{0};
  pool.post([p = std::make_unique<int>(7), &done2, &moved_value]() mutable {
    moved_value = *p;
    done2.count_down();
  });
  done2.wait();
  CHECK(moved_value.load() == 7);
}
