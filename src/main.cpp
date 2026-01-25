#if 0
#include "dripcore/loop.h"
#include "dripcore/task.h"
#include "dripcore/task_channel.h"

#include <iostream>
#include <vector>

class count_task : public dripcore::task
{
public:
  using channel  = dripcore::task_channel<std::string, 1>;
private:
  using ichannel = dripcore::task_ichannel<std::string>;
public:
  count_task(const channel input)
    : input_(input, this)
  {
  }
public:
  void main() override
  {
    auto m = input_.recv();
    std::cout << m << std::endl;
  }
private:
  dripcore::task_ichannel<std::string> input_;
};

int main(int argc, char* argv[])
{
  dripcore::loop loop;

  count_task::channel messages;

  auto t = loop.spawn<count_task>(messages);

  if ( auto t_ref = t.lock() )
  {
    messages.send("hello", t_ref.get());
  }

  loop.run();

  return 0;
}
#endif

#if 0
#include "dripcore/task.h"
#include "dripcore/channel.h"
#include "dripcore/scheduler.h"

#include <iostream>

// A task function using C++20 coroutine syntax
dripcore::task counter_task(const std::string& name, int limit)
{
  for (int i = 1; i <= limit; ++i) {
    std::cout << "Task " << name << ": " << i << std::endl;

    // Suspend execution and let the scheduler run something else.
    // This is the "Fiber.yield" equivalent from Crystal.
    co_await dripcore::yield();
  }

  std::cout << "Task " << name << " finished!" << std::endl;
}

int main(int argc, char* argv[])
{
  dripcore::scheduler sched;

  std::cout << "Starting scheduler with two tasks..." << std::endl;

  // Spawn two tasks. They will start in the 'init' state and
  // be resumed by the scheduler's run_queue processing.
  sched.spawn(counter_task("A", 3));
  sched.spawn(counter_task("B", 3));

  // The scheduler will run until all tasks are 'done()'.
  sched.run();

  std::cout << "All tasks complete." << std::endl;

  return 0;
}
#endif

#include "dripcore/task.h"
#include "dripcore/scheduler.h"

#include <coroutine>
#include <iostream>
#include <memory>
#include <list>
#include <system_error>

class greeter : public dripcore::task
{
public:
  greeter(std::string name) : name_(name) {}

  // The coroutine is a member function
  dripcore::task_handle execute() {
    std::cout << "hello " << name_ << std::endl;
    co_await yield();
    std::cout << "goodbye " << name_ << std::endl;
    co_return;
  }

private:
  std::string name_;
};

int main(int argc, char* argv[])
{
  dripcore::scheduler scheduler;

  scheduler.spawn<greeter>("Alice");
  scheduler.run();

  return 0;
}
