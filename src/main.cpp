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
#include "dripcore/executor.h"
#include "dripcore/socket.h"

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
  dripcore::coroutine execute() {
    std::cout << "hello " << name_ << std::endl;
    co_await yield();
    std::cout << "goodbye " << name_ << std::endl;
    co_return;
  }

private:
  std::string name_;
};

class echo_connection : public dripcore::task {
public:
  echo_connection(dripcore::socket socket) : socket_(std::move(socket)) {}

  dripcore::coroutine execute() {
    char buf[4096];

    while (true) {
      co_await readable(socket_);

      ssize_t n = socket_.recv(buf, sizeof(buf));
      if (n <= 0) break;  // EOF or error

      // Echo back
      co_await writable(socket_);
      write(socket_.platform_handle(), buf, n);
    }

    // Socket closes automatically
  }

private:
  dripcore::socket socket_;
};

class echo_server : public dripcore::task {
public:
  echo_server(int port) : port_(port) {}

  dripcore::coroutine execute() {
    dripcore::socket server = dripcore::ipv4::tcp::socket();
    dripcore::socket_address addr("0.0.0.0", 4321);
    server.bind(addr);
    server.listen(5);
    server.nonblocking(true);

    while (true) {
      co_await readable(server);

      dripcore::socket_address client_address;

      auto address_len = socklen_t{client_address.len()};
      auto client = server.accept(client_address.ptr(), &address_len);

      client.nonblocking(true);

      // Spawn connection handler using task::spawn()
      spawn<echo_connection>(std::move(client));

      std::cout << "client connected " << client_address << std::endl;
    }
  }
private:
  int port_;
};

int main(int argc, char* argv[])
{
  dripcore::executor executor;

  //executor.spawn<greeter>("Alice");
  executor.spawn<echo_server>(4321);
  executor.run();

  return 0;
}
