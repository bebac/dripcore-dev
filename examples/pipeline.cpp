// ---------------------------------------------------------------------------
// Pipeline example — a three-stage text processing pipeline using
// dripcore channels.
//
//   stdin → producer_task → channel<string> → upper_task → channel<string> → printer_task
//
// The producer reads lines from stdin and sends them through a bounded
// channel to the upper stage, which uppercases each line and sends it
// through another bounded channel to the printer. The bounded channels
// provide natural backpressure — if the printer is slow, the upper
// stage blocks on send, which in turn blocks the producer.
//
// All three stages run concurrently on a single thread, cooperatively
// scheduled by dripcore. When stdin reaches EOF, the producer closes its
// output channel. The upper stage detects the close (recv returns
// nullopt) and closes its output channel. The printer detects the close
// and exits. The scheduler drains all tasks and exits.
//
// Build: cmake --build build --target pipeline
// Run:   echo -e "hello\nworld" | ./build/examples/pipeline
//        cat README.md | ./build/examples/pipeline
// ---------------------------------------------------------------------------

#include <dripcore/task.h>
#include <dripcore/channel.h>
#include <dripcore/scheduler.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

// ---------------------------------------------------------------------------
// producer_task — reads lines from stdin, sends them through a channel.
// ---------------------------------------------------------------------------
class producer_task : public dripcore::task
{
public:
  producer_task(std::shared_ptr<dripcore::channel<std::string>> out)
    : out_(std::move(out)) {}

  dripcore::awaitable<> body() override
  {
    std::string line;
    while (std::getline(std::cin, line)) {
      co_await out_->send(this, std::move(line));
    }
    // EOF — close the channel so the next stage knows we're done
    out_->close();
    co_return;
  }

private:
  std::shared_ptr<dripcore::channel<std::string>> out_;
};

// ---------------------------------------------------------------------------
// upper_task — reads lines from the input channel, uppercases each line,
// and sends the result through the output channel.
// ---------------------------------------------------------------------------
class upper_task : public dripcore::task
{
public:
  upper_task(
    std::shared_ptr<dripcore::channel<std::string>> in,
    std::shared_ptr<dripcore::channel<std::string>> out
  ) : in_(std::move(in)), out_(std::move(out)) {}

  dripcore::awaitable<> body() override
  {
    while (true) {
      auto line = co_await in_->recv(this);
      if (!line) {
        // Input channel closed — propagate the close downstream
        out_->close();
        break;
      }

      // Uppercase the line
      std::transform(line->begin(), line->end(), line->begin(),
        [](unsigned char c) { return std::toupper(c); });

      co_await out_->send(this, std::move(*line));
    }
    co_return;
  }

private:
  std::shared_ptr<dripcore::channel<std::string>> in_;
  std::shared_ptr<dripcore::channel<std::string>> out_;
};

// ---------------------------------------------------------------------------
// printer_task — reads lines from the input channel and prints them to
// stdout with a line number.
// ---------------------------------------------------------------------------
class printer_task : public dripcore::task
{
public:
  printer_task(std::shared_ptr<dripcore::channel<std::string>> in)
    : in_(std::move(in)) {}

  dripcore::awaitable<> body() override
  {
    size_t line_num = 1;
    while (true) {
      auto line = co_await in_->recv(this);
      if (!line) {
        // Channel closed — all upstream stages are done
        break;
      }

      std::cout << line_num++ << ": " << *line << '\n';
    }
    co_return;
  }

private:
  std::shared_ptr<dripcore::channel<std::string>> in_;
};

// ---------------------------------------------------------------------------
// main — wire up the pipeline and run it.
// ---------------------------------------------------------------------------
int main()
{
  // Bounded channels — capacity 4. This provides backpressure: if the
  // downstream stage is slow, the upstream stage blocks on send().
  auto ch1 = std::make_shared<dripcore::channel<std::string>>(4);
  auto ch2 = std::make_shared<dripcore::channel<std::string>>(4);

  dripcore::scheduler sched;

  // Spawn all three stages. They start suspended and will be scheduled
  // when the scheduler runs.
  sched.spawn<producer_task>(ch1);
  sched.spawn<upper_task>(ch1, ch2);
  sched.spawn<printer_task>(ch2);

  // Run until all tasks complete. The scheduler cooperatively schedules
  // the three stages on a single thread. When the producer closes ch1,
  // the upper stage drains and closes ch2, the printer drains and exits,
  // and the scheduler returns.
  sched.run();

  return 0;
}