#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"
#include "dripcore/channel.h"

#include <vector>
#include <string>
#include <memory>

TEST_CASE("channel: producer task sends, consumer task receives" * doctest::timeout(1))
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(16);
  auto count = 10u;

  // Producer: sends 0..<count> then closes
  class producer : public dripcore::task
  {
  public:
    producer(std::shared_ptr<dripcore::channel<int>> ch, int count)
        : ch_(ch), count_(count) {}

    dripcore::coroutine execute()
    {
      for (int i = 0; i < count_; ++i)
      {
        ch_->try_send(i);
        co_await yield();
      }
      ch_->close();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    int count_;
  };

  // Consumer: receives until channel closes
  class consumer : public dripcore::task
  {
  public:
    consumer(std::shared_ptr<dripcore::channel<int>> ch, int count)
        : ch_(ch), count_(count), expected_(0) {}

    dripcore::coroutine execute()
    {
      while (true)
      {
        auto val = co_await ch_->recv();
        if (!val) {
          break;
        }
        CHECK(*val == expected_++);
      }
      CHECK(count_ == expected_);
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    int count_;
    int expected_;
  };

  sched.spawn<consumer>(ch, count);
  sched.spawn<producer>(ch, count);
  sched.run();
}

TEST_CASE("channel: recv returns nullopt after close with empty buffer" * doctest::timeout(1))
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(4);
  std::atomic<bool> got_nullopt = false;

  // Close immediately, consumer should get nullopt
  class closer : public dripcore::task
  {
  public:
    closer(std::shared_ptr<dripcore::channel<int>> ch) : ch_(ch) {}

    dripcore::coroutine execute()
    {
      ch_->close();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
  };

  class reader : public dripcore::task
  {
  public:
    reader(std::shared_ptr<dripcore::channel<int>> ch, std::atomic<bool>& got_nullopt)
        : ch_(ch), got_nullopt_(got_nullopt) {}

    dripcore::coroutine execute()
    {
      auto val = co_await ch_->recv();
      got_nullopt_ = !val.has_value();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::atomic<bool>& got_nullopt_;
  };

  sched.spawn<closer>(ch);
  sched.spawn<reader>(ch, got_nullopt);
  sched.run();

  CHECK(got_nullopt == true);
}

TEST_CASE("channel: drain remaining items after close" * doctest::timeout(1))
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(8);
  std::vector<int> received;

  // Send items, then close — consumer should still get them all
  class sender : public dripcore::task
  {
  public:
    sender(std::shared_ptr<dripcore::channel<int>> ch) : ch_(ch) {}

    dripcore::coroutine execute()
    {
      ch_->try_send(10);
      ch_->try_send(20);
      ch_->try_send(30);
      ch_->close();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
  };

  class reader : public dripcore::task
  {
  public:
    reader(std::shared_ptr<dripcore::channel<int>> ch, std::vector<int> expected)
        : ch_(ch), expected_(expected) {}

    dripcore::coroutine execute()
    {
      for ( auto v : expected_ )
      {
        auto val = co_await ch_->recv();
        if (!val) {
          REQUIRE(false);
        }
        received_.push_back(*val);
      }
      CHECK(received_ == expected_);
      // Channel should be closed next.
      auto val = co_await ch_->recv();
      CHECK(!val);
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::vector<int> expected_;
    std::vector<int> received_;
  };

  sched.spawn<sender>(ch);
  sched.spawn<reader>(ch, std::vector{10, 20, 30});
  sched.run();
}

TEST_CASE("channel: try_send returns false when full")
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(2);
  std::atomic<bool> send1_ok = false;
  std::atomic<bool> send2_ok = false;
  std::atomic<bool> send3_ok = false;

  class test_task : public dripcore::task
  {
  public:
    test_task(std::shared_ptr<dripcore::channel<int>> ch,
              std::atomic<bool>& s1, std::atomic<bool>& s2, std::atomic<bool>& s3)
        : ch_(ch), s1_(s1), s2_(s2), s3_(s3) {}

    dripcore::coroutine execute()
    {
      s1_ = ch_->try_send(1); // Should succeed (0/2)
      s2_ = ch_->try_send(2); // Should succeed (1/2)
      s3_ = ch_->try_send(3); // Should fail   (2/2 = full)
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::atomic<bool>& s1_;
    std::atomic<bool>& s2_;
    std::atomic<bool>& s3_;
  };

  sched.spawn<test_task>(ch, send1_ok, send2_ok, send3_ok);
  sched.run();

  CHECK(send1_ok == true);
  CHECK(send2_ok == true);
  CHECK(send3_ok == false);
  CHECK(ch->size() == 2);
}

TEST_CASE("channel: try_send returns false after close")
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(4);
  std::atomic<bool> send_after_close = true;

  class test_task : public dripcore::task
  {
  public:
    test_task(std::shared_ptr<dripcore::channel<int>> ch, std::atomic<bool>& result)
        : ch_(ch), result_(result) {}

    dripcore::coroutine execute()
    {
      ch_->close();
      result_ = ch_->try_send(42);
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::atomic<bool>& result_;
  };

  sched.spawn<test_task>(ch, send_after_close);
  sched.run();

  CHECK(send_after_close == false);
}

TEST_CASE("channel: consumer suspends until producer sends")
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<int>>(4);
  std::atomic<int> received_value = -1;
  std::atomic<int> send_order = 0;
  std::atomic<int> recv_order = 0;

  // Consumer starts first, should suspend waiting for data
  class consumer : public dripcore::task
  {
  public:
    consumer(std::shared_ptr<dripcore::channel<int>> ch, std::atomic<int>& value, std::atomic<int>& order)
        : ch_(ch), value_(value), order_(order) {}

    dripcore::coroutine execute()
    {
      auto val = co_await ch_->recv(); // Should suspend here
      if (val)
      {
        value_ = *val;
        order_ = 2; // Should be second (after producer sends)
      }
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::atomic<int>& value_;
    std::atomic<int>& order_;
  };

  class producer : public dripcore::task
  {
  public:
    producer(std::shared_ptr<dripcore::channel<int>> ch, std::atomic<int> &order)
        : ch_(ch), order_(order) {}

    dripcore::coroutine execute()
    {
      co_await yield();  // Let consumer run and suspend on recv
      order_ = 1;        // Producer acts first
      ch_->try_send(42); // This should wake the consumer
      ch_->close();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<int>> ch_;
    std::atomic<int> &order_;
  };

  sched.spawn<consumer>(ch, received_value, recv_order);
  sched.spawn<producer>(ch, send_order);
  sched.run();

  CHECK(received_value == 42);
  CHECK(send_order == 1);
  CHECK(recv_order == 2);
}

TEST_CASE("channel: works with move-only types")
{
  dripcore::scheduler sched;

  auto ch = std::make_shared<dripcore::channel<std::unique_ptr<int>>>(4);
  std::atomic<int> received_value = -1;

  class sender : public dripcore::task
  {
  public:
    sender(std::shared_ptr<dripcore::channel<std::unique_ptr<int>>> ch) : ch_(ch) {}

    dripcore::coroutine execute()
    {
      ch_->try_send(std::make_unique<int>(99));
      ch_->close();
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<std::unique_ptr<int>>> ch_;
  };

  class receiver : public dripcore::task
  {
  public:
    receiver(std::shared_ptr<dripcore::channel<std::unique_ptr<int>>> ch, std::atomic<int>& out)
        : ch_(ch), out_(out) {}

    dripcore::coroutine execute()
    {
      co_await yield();
      auto val = co_await ch_->recv();
      if (val && *val)
      {
        out_ = **val;
      }
      co_return;
    }

  private:
    std::shared_ptr<dripcore::channel<std::unique_ptr<int>>> ch_;
    std::atomic<int>& out_;
  };

  sched.spawn<sender>(ch);
  sched.spawn<receiver>(ch, received_value);
  sched.run();

  CHECK(received_value == 99);
}

TEST_CASE("channel: concurrent send from multiple producers, single consumer")
{
    dripcore::scheduler sched;

    auto ch = std::make_shared<dripcore::channel<int>>(100);
    const int producer_count = 10;
    const int items_per_producer = 10;
    const int expected_total = producer_count * items_per_producer;

    std::atomic<int> produced_count{0};
    std::atomic<int> received_count{0};

    class producer : public dripcore::task {
    public:
        producer(std::shared_ptr<dripcore::channel<int>> ch,
                 int id, int items, std::atomic<int>& produced)
            : ch_(ch), id_(id), items_(items), produced_(produced) {}

        dripcore::coroutine execute() {
            for (int i = 0; i < items_; ++i) {
                while (!ch_->try_send(id_ * 100 + i)) {
                    co_await yield();
                }
                produced_.fetch_add(1);
            }
            co_return;
        }

    private:
        std::shared_ptr<dripcore::channel<int>> ch_;
        int id_;
        int items_;
        std::atomic<int>& produced_;
    };

    class consumer : public dripcore::task {
    public:
        consumer(std::shared_ptr<dripcore::channel<int>> ch,
                 std::atomic<int>& received, int expected)
            : ch_(ch), received_(received), expected_(expected) {}

        dripcore::coroutine execute() {
            int count = 0;
            while (count < expected_) {
                auto val = co_await ch_->recv();
                if (!val) break;
                received_.fetch_add(1);
                count++;
            }
            co_return;
        }

    private:
        std::shared_ptr<dripcore::channel<int>> ch_;
        std::atomic<int>& received_;
        int expected_;
    };

    // Spawn multiple producers
    for (int i = 0; i < producer_count; ++i) {
        sched.spawn<producer>(ch, i, items_per_producer, produced_count);
    }

    // Spawn SINGLE consumer
    sched.spawn<consumer>(ch, received_count, expected_total);

    sched.run();
    ch->close();
    sched.run(); // Process any remaining items

    CHECK(produced_count == expected_total);
    CHECK(received_count == expected_total);
}
