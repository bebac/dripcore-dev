# Doctest Usage Guide for DripCore

This guide provides instructions for writing effective doctest tests for the DripCore coroutine-based task scheduler library.

## 🚨 CRITICAL: Thread Safety Requirement

**DripCore will become multithreaded with multiple executors and work-stealing.**

Even though currently single-threaded, **ALL tests MUST be written with thread safety in mind**:

- ✅ **Use `std::atomic<T>` for shared simple types** (bool, int, flags, counters)
- ✅ **Use `std::mutex` + `std::lock_guard` for complex shared data** (vectors, maps, strings)
- ✅ **Never assume tasks run on the same thread as test context**
- ❌ **Never use raw variables for shared state** - causes data races

**This is mandatory for all DripCore tests.**

## Table of Contents

- [Quick Reference](#quick-reference)
- [Test Organization](#test-organization)
- [Assertion System](#assertion-system)
- [Testing Asynchronous Code](#testing-asynchronous-code)
- [Test Fixtures](#test-fixtures)
- [Test Suites and Decorators](#test-suites-and-decorators)
- [Subcases](#subcases-for-hierarchical-testing)
- [Exception Testing](#exception-testing)
- [Special Comparisons](#special-comparisons)
- [Best Practices for DripCore](#best-practices-for-dripcore)
- [Command Line Usage](#command-line-usage)
- [Complete Example](#complete-test-file-example)

---

## Quick Reference

```cpp
// Test case definition
TEST_CASE("component: description") { /* ... */ }

// Assertions
REQUIRE(condition);    // Fails immediately
CHECK(condition);      // Fails but continues
WARN(condition);       // Logs warning only

// Common comparisons
CHECK_EQ(a, b); CHECK_NE(a, b); CHECK_LT(a, b); CHECK_GT(a, b);
CHECK_LE(a, b); CHECK_GE(a, b);

// Exception testing
CHECK_THROWS(expr);
CHECK_THROWS_AS(expr, std::runtime_error);
CHECK_NOTHROW(expr);

// Subcases
SUBCASE("case name") { /* ... */ }

// Fixtures
TEST_CASE_FIXTURE(FixtureClass, "test name") { /* ... */ }

// Suites
TEST_SUITE("suite name") { /* ... */ }
```

---

## Test Organization

### File Structure

```
/test
├── doctest_main.cpp      # Main entry point
├── dripcore_test.cpp     # Core scheduler tests
├── executor_test.cpp      # Executor tests
├── event_loop_test.cpp    # Platform-specific tests
├── io_test.cpp            # I/O tests
├── benchmark_test.cpp     # Performance tests
└── integration_test.cpp   # End-to-end tests
```

### Test Naming

Use descriptive, hierarchical names:
```cpp
TEST_CASE("scheduler: task spawning with multiple executors")
TEST_CASE("executor: ready queue processing order")
TEST_CASE("io: socket readable awaiter behavior")
```

---

## Assertion System

### Severity Levels

| Level | Behavior | Use Case |
|-------|----------|----------|
| `REQUIRE` | Stops test immediately | Critical preconditions |
| `CHECK` | Continues, reports all failures | Multiple assertions |
| `WARN` | Logs warning only | Non-critical checks |

### Expression Decomposition

Doctest shows actual values on failure:
```cpp
int a = 4, b = 2;
CHECK(a == b);
// Output: CHECK( a == b ) is NOT correct! values: CHECK( 4 == 2 )
```

### Binary vs Expression Asserts

**Expression asserts** (recommended):
```cpp
CHECK(a == b);
CHECK(a < b && b > c);
```

**Binary asserts** (faster compilation):
```cpp
CHECK_EQ(a, b);
CHECK_LT(a, b);
```

---

## Testing Asynchronous Code

**All examples use proper synchronization for future multithreading.**

### Basic Task Pattern

```cpp
TEST_CASE("scheduler: basic task execution") {
    dripcore::scheduler scheduler;
    std::atomic<bool> task_ran{false};
    
    class test_task : public dripcore::task {
    public:
        test_task(std::atomic<bool>& flag) : flag_(flag) {}
        dripcore::coroutine execute() {
            flag_ = true;  // Atomic write
            co_await yield();
            co_return;
        }
    private:
        std::atomic<bool>& flag_;
    };
    
    scheduler.spawn<test_task>(task_ran);
    scheduler.run();
    CHECK(task_ran == true);  // Atomic read
}
```

### Shared Complex State

```cpp
TEST_CASE("scheduler: shared vector access") {
    dripcore::scheduler scheduler;
    std::vector<int> execution_order;
    std::mutex order_mutex;
    
    class ordered_task : public dripcore::task {
    public:
        ordered_task(std::vector<int>& order, std::mutex& mtx, int id) 
            : order_(order), order_mutex_(mtx), id_(id) {}
        
        dripcore::coroutine execute() {
            std::lock_guard<std::mutex> lock(order_mutex_);
            order_.push_back(id_);
            co_await yield();
            co_return;
        }
    private:
        std::vector<int>& order_;
        std::mutex& order_mutex_;
        int id_;
    };
    
    scheduler.spawn<ordered_task>(execution_order, order_mutex, 1);
    scheduler.spawn<ordered_task>(execution_order, order_mutex, 2);
    scheduler.run();
    
    {
        std::lock_guard<std::mutex> lock(order_mutex);
        CHECK(execution_order.size() == 2);
    }
}
```

### I/O Operations

```cpp
TEST_CASE("io: pipe communication") {
    dripcore::scheduler scheduler;
    
    int pipefd[2];
    REQUIRE(pipe(pipefd) == 0);
    fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
    
    std::string received_message;
    std::mutex message_mutex;
    std::atomic<bool> writer_done{false};
    std::atomic<bool> reader_done{false};
    
    // Writer task
    class writer_task : public dripcore::task {
    public:
        writer_task(int fd, std::atomic<bool>& done) : fd_(fd), done_(done) {}
        dripcore::coroutine execute() {
            const char* msg = "Hello!";
            co_await writable(fd_);
            write(fd_, msg, strlen(msg));
            close(fd_);
            done_ = true;
            co_return;
        }
    private:
        int fd_;
        std::atomic<bool>& done_;
    };
    
    // Reader task
    class reader_task : public dripcore::task {
    public:
        reader_task(int fd, std::string& result, std::mutex& msg_mtx, std::atomic<bool>& done)
            : fd_(fd), result_(result), message_mutex_(msg_mtx), done_(done) {}
        dripcore::coroutine execute() {
            char buffer[256];
            co_await readable(fd_);
            ssize_t n = read(fd_, buffer, sizeof(buffer) - 1);
            if (n > 0) {
                buffer[n] = '\0';
                std::lock_guard<std::mutex> lock(message_mutex_);
                result_ = buffer;
            }
            close(fd_);
            done_ = true;
            co_return;
        }
    private:
        int fd_;
        std::string& result_;
        std::mutex& message_mutex_;
        std::atomic<bool>& done_;
    };
    
    scheduler.spawn<writer_task>(pipefd[1], writer_done);
    scheduler.spawn<reader_task>(pipefd[0], received_message, message_mutex, reader_done);
    scheduler.run();
    
    CHECK(writer_done == true);
    CHECK(reader_done == true);
    {
        std::lock_guard<std::mutex> lock(message_mutex);
        CHECK(received_message == "Hello!");
    }
}
```

---

## Test Fixtures

Use fixtures for shared setup/teardown:

```cpp
class SchedulerFixture {
protected:
    dripcore::scheduler scheduler;
    
    void spawn_flag_task(std::atomic<bool>& flag) {
        scheduler.spawn([&]() -> dripcore::task {
            flag = true;
            co_return;
        });
    }
};

TEST_CASE_FIXTURE(SchedulerFixture, "fixture: basic execution") {
    std::atomic<bool> task_ran{false};
    spawn_flag_task(task_ran);
    scheduler.run();
    CHECK(task_ran == true);
}

// Fixture with setup/teardown
class PipeFixture {
protected:
    int pipefd[2];
    PipeFixture() {
        REQUIRE(pipe(pipefd) == 0);
        fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
        fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
    }
    ~PipeFixture() {
        if (pipefd[0] >= 0) close(pipefd[0]);
        if (pipefd[1] >= 0) close(pipefd[1]);
    }
};

TEST_CASE_FIXTURE(PipeFixture, "io: fixture with pipe") {
    dripcore::scheduler scheduler;
    std::atomic<bool> read_complete{false};
    
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::readable(pipefd[0]);
        char buffer[10];
        read(pipefd[0], buffer, sizeof(buffer));
        read_complete = true;
        co_return;
    });
    
    write(pipefd[1], "test", 4);
    scheduler.run();
    CHECK(read_complete == true);
}
```

---

## Test Suites and Decorators

### Test Suites

Group related test cases:

```cpp
TEST_SUITE("scheduler") {
    TEST_CASE("basic task execution") { /* ... */ }
    TEST_CASE("task ordering") { /* ... */ }
}

TEST_SUITE("io") {
    TEST_CASE("pipe communication") { /* ... */ }
    TEST_CASE("socket operations") { /* ... */ }
}

// Run specific suite: ./doctest --test-suite=scheduler
```

### Decorators

```cpp
// Skip a test
TEST_CASE("platform: linux specific" * doctest::skip(true)) { /* ... */ }

// Set timeout
TEST_CASE("stress: many tasks" * doctest::timeout(5.0)) { /* ... */ }

// Mark expected failures
TEST_CASE("known issue" * doctest::may_fail(true)) { /* ... */ }
TEST_CASE("should crash" * doctest::should_fail(true)) { /* ... */ }

// Add description
TEST_CASE("scheduler: task spawning" * 
          doctest::description("Tests basic task spawning")) { /* ... */ }

// Combine decorators
TEST_CASE("experimental" * 
          doctest::may_fail(true) * 
          doctest::timeout(2.0)) { /* ... */ }
```

---

## Subcases for Hierarchical Testing

Use subcases to organize complex test scenarios:

```cpp
TEST_CASE("scheduler: task lifecycle") {
    dripcore::scheduler scheduler;
    
    SUBCASE("single task") {
        std::atomic<bool> completed{false};
        scheduler.spawn([&]() -> dripcore::task {
            completed = true;
            co_return;
        });
        scheduler.run();
        CHECK(completed == true);
    }
    
    SUBCASE("multiple tasks") {
        std::atomic<int> completed_count{0};
        for (int i = 0; i < 10; ++i) {
            scheduler.spawn([&]() -> dripcore::task {
                completed_count++;
                co_return;
            });
        }
        scheduler.run();
        CHECK(completed_count == 10);
    }
    
    SUBCASE("nested yielding") {
        std::atomic<int> yield_count{0};
        scheduler.spawn([&]() -> dripcore::task {
            for (int i = 0; i < 5; ++i) {
                yield_count++;
                co_await dripcore::yield();
            }
            co_return;
        });
        scheduler.run();
        CHECK(yield_count == 5);
    }
}
```

Each subcase path is executed independently. Shared setup code runs for each path.

---

## Exception Testing

### Basic Exception Testing

```cpp
TEST_CASE("executor: exception handling") {
    dripcore::scheduler scheduler;
    
    SUBCASE("task throws exception") {
        std::atomic<bool> exception_caught{false};
        
        scheduler.spawn([&]() -> dripcore::task {
            try {
                throw std::runtime_error("test error");
            } catch (...) {
                exception_caught = true;
            }
            co_return;
        });
        
        scheduler.run();
        CHECK(exception_caught == true);
    }
    
    SUBCASE("exception in coroutine") {
        CHECK_THROWS([&]() {
            scheduler.spawn([&]() -> dripcore::task {
                throw std::logic_error("coroutine error");
                co_return;
            });
            scheduler.run();
        }());
    }
}
```

### Exception Type and Message Testing

```cpp
TEST_CASE("io: error handling") {
    CHECK_THROWS_AS(dripcore::readable(-1), std::runtime_error);
    CHECK_THROWS_WITH(dripcore::writable(-1), "invalid file descriptor");
    CHECK_THROWS_WITH_AS(connect_to_invalid_address(), "connection refused", std::system_error);
}
```

---

## Special Comparisons

### Floating Point Comparisons

```cpp
TEST_CASE("benchmark: timing accuracy") {
    dripcore::scheduler scheduler;
    auto start = std::chrono::high_resolution_clock::now();
    
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::sleep(scheduler, std::chrono::milliseconds(100));
        co_return;
    });
    scheduler.run();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    // Allow 10% tolerance
    CHECK(elapsed_ms == doctest::Approx(100).epsilon(0.1));
}
```

### String Containment

```cpp
TEST_CASE("logging: error messages") {
    std::string error_msg = get_last_error_message();
    CHECK(error_msg == doctest::Contains("timeout"));
    CHECK(error_msg == doctest::Contains("task") && 
          error_msg == doctest::Contains("failed"));
}

TEST_CASE("io: exception messages") {
    CHECK_THROWS_WITH(invalid_operation(), doctest::Contains("invalid"));
}
```

---

## Best Practices for DripCore

### 🔍 Thread Safety Checklist

**Before writing any test, ask yourself:**

1. **Is this variable shared between tasks?** → If yes, MUST be atomic or mutex-protected
2. **Is this variable shared between tasks and test context?** → If yes, MUST be atomic or mutex-protected
3. **Is this data structure accessed by multiple tasks?** → If yes, MUST be mutex-protected
4. **Am I using raw variables for shared state?** → If yes, STOP - use atomic or mutex

**Quick Reference:**
- `bool flag` → `std::atomic<bool> flag`
- `int counter` → `std::atomic<int> counter`
- `std::string message` → `std::string message + std::mutex message_mutex`
- `std::vector<T> data` → `std::vector<T> data + std::mutex data_mutex`

### Thread Safety Principles

1. **Never assume single-threaded execution** - Tests must work with multiple executors
2. **All shared state must be properly synchronized**
3. **Use thread-safe primitives for all shared data**
4. **Avoid data races at all costs**

### Test Isolation

Each test case should be independent:

```cpp
// Good - each test creates its own scheduler
TEST_CASE("scheduler: test 1") {
    dripcore::scheduler scheduler;
    // ...
}

// Bad - shared state between tests
static dripcore::scheduler shared_scheduler; // Don't do this!
```

### Synchronization Patterns

#### ✅ Correct: Using Atomic Variables

```cpp
TEST_CASE("concurrency: atomic counter") {
    dripcore::scheduler scheduler;
    std::atomic<int> counter{0};
    
    for (int i = 0; i < 100; ++i) {
        scheduler.spawn([&]() -> dripcore::task {
            counter++;  // Atomic increment
            co_return;
        });
    }
    
    scheduler.run();
    CHECK(counter == 100);
}
```

#### ✅ Correct: Using Mutexes for Complex Operations

```cpp
TEST_CASE("concurrency: mutex-protected vector") {
    dripcore::scheduler scheduler;
    std::vector<int> shared_data;
    std::mutex data_mutex;
    
    for (int i = 0; i < 10; ++i) {
        scheduler.spawn([&, i]() -> dripcore::task {
            std::lock_guard<std::mutex> lock(data_mutex);
            shared_data.push_back(i * 2);
            co_return;
        });
    }
    
    scheduler.run();
    
    {
        std::lock_guard<std::mutex> lock(data_mutex);
        CHECK(shared_data.size() == 10);
    }
}
```

#### ❌ WRONG: Unprotected Shared State

```cpp
// DANGEROUS - This will cause data races!
TEST_CASE("concurrency: UNSAFE") {
    dripcore::scheduler scheduler;
    int counter = 0;  // NOT thread-safe!
    
    for (int i = 0; i < 100; ++i) {
        scheduler.spawn([&]() -> dripcore::task {
            counter++;  // DATA RACE! Undefined behavior!
            co_return;
        });
    }
    
    scheduler.run();
}
```

### Communication Between Tasks and Test Context

```cpp
TEST_CASE("task communication: atomic flags") {
    dripcore::scheduler scheduler;
    
    std::atomic<bool> task1_completed{false};
    std::atomic<bool> task2_completed{false};
    std::atomic<int> task1_result{0};
    std::atomic<int> task2_result{0};
    
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::yield();
        task1_result = 42;
        task1_completed = true;
        co_return;
    });
    
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::yield();
        task2_result = 84;
        task2_completed = true;
        co_return;
    });
    
    scheduler.run();
    
    CHECK(task1_completed == true);
    CHECK(task2_completed == true);
    CHECK(task1_result == 42);
    CHECK(task2_result == 84);
}
```

### Clean Up Resources

```cpp
TEST_CASE("io: socket cleanup") {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(sockfd >= 0);
    
    auto cleanup = [&]() { if (sockfd >= 0) close(sockfd); };
    
    // Test code...
    cleanup();
}
```

### Use REQUIRE for Critical Setup

```cpp
TEST_CASE("io: pipe operations") {
    int pipefd[2];
    REQUIRE(pipe(pipefd) == 0); // Critical setup
}
```

### Test Edge Cases

```cpp
TEST_CASE("scheduler: edge cases") {
    dripcore::scheduler scheduler;
    
    SUBCASE("zero tasks") {
        scheduler.run();
        CHECK(true);
    }
    
    SUBCASE("task that never yields") {
        std::atomic<bool> completed{false};
        scheduler.spawn([&]() -> dripcore::task {
            completed = true;
            co_return;
        });
        scheduler.run();
        CHECK(completed == true);
    }
    
    SUBCASE("task with many yields") {
        std::atomic<int> yield_count{0};
        scheduler.spawn([&]() -> dripcore::task {
            for (int i = 0; i < 1000; ++i) {
                yield_count++;
                co_await dripcore::yield();
            }
            co_return;
        });
        scheduler.run();
        CHECK(yield_count == 1000);
    }
}
```

### Future Multithreading Considerations

#### Testing with Multiple Executors (Future)

```cpp
TEST_CASE("multithreading: work stealing") {
    std::atomic<int> total_tasks_completed{0};
    const int num_tasks = 1000;
    
    dripcore::scheduler scheduler;
    
    for (int i = 0; i < num_tasks; ++i) {
        scheduler.spawn([&]() -> dripcore::task {
            total_tasks_completed++;
            co_await dripcore::yield();
            co_return;
        });
    }
    
    scheduler.run();
    CHECK(total_tasks_completed == num_tasks);
}
```

#### Cross-Executor Communication

```cpp
TEST_CASE("multithreading: cross-executor communication") {
    dripcore::scheduler scheduler;
    
    std::queue<std::string> message_queue;
    std::mutex queue_mutex;
    std::atomic<int> messages_processed{0};
    const int total_messages = 100;
    
    // Producer tasks
    for (int i = 0; i < 10; ++i) {
        scheduler.spawn([&, i]() -> dripcore::task {
            for (int j = 0; j < 10; ++j) {
                std::string msg = "msg_" + std::to_string(i * 10 + j);
                {
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    message_queue.push(msg);
                }
                co_await dripcore::yield();
            }
            co_return;
        });
    }
    
    // Consumer task
    scheduler.spawn([&]() -> dripcore::task {
        while (messages_processed < total_messages) {
            std::string msg;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                if (!message_queue.empty()) {
                    msg = message_queue.front();
                    message_queue.pop();
                    messages_processed++;
                } else {
                    co_await dripcore::yield();
                    continue;
                }
            }
            CHECK(!msg.empty());
            co_await dripcore::yield();
        }
        co_return;
    });
    
    scheduler.run();
    CHECK(messages_processed == total_messages);
}
```

---

## Performance Testing

**Even benchmark tests must use proper synchronization.**

### Simple Benchmarks

```cpp
TEST_CASE("benchmark: task spawn performance") {
    dripcore::scheduler scheduler;
    const int iterations = 1000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < iterations; ++i) {
        std::atomic<bool> done{false};
        scheduler.spawn([&]() -> dripcore::task {
            done = true;
            co_return;
        });
        scheduler.run();
        CHECK(done == true);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::printf("Task spawn: %lld ms for %d iterations (%.2f μs per spawn)\n",
                (long long)total_ms, iterations, (total_ms * 1000.0) / iterations);
    
    CHECK(total_ms > 0);
    CHECK(total_ms < 10000);
}
```

### Comparative Benchmarks

```cpp
TEST_CASE("benchmark: yield vs sleep") {
    dripcore::scheduler scheduler;
    
    SUBCASE("pure yield") {
        std::atomic<int> yield_count{0};
        auto start = std::chrono::high_resolution_clock::now();
        
        scheduler.spawn([&]() -> dripcore::task {
            for (int i = 0; i < 1000; ++i) {
                yield_count++;
                co_await dripcore::yield();
            }
            co_return;
        });
        scheduler.run();
        
        auto end = std::chrono::high_resolution_clock::now();
        auto yield_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        
        CHECK(yield_count == 1000);
        std::printf("Pure yield: %lld μs\n", (long long)yield_time);
    }
    
    SUBCASE("short sleep") {
        std::atomic<int> sleep_count{0};
        auto start = std::chrono::high_resolution_clock::now();
        
        scheduler.spawn([&]() -> dripcore::task {
            for (int i = 0; i < 10; ++i) {
                sleep_count++;
                co_await dripcore::sleep(scheduler, std::chrono::microseconds(100));
            }
            co_return;
        });
        scheduler.run();
        
        auto end = std::chrono::high_resolution_clock::now();
        auto sleep_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        
        CHECK(sleep_count == 10);
        std::printf("Short sleep: %lld μs\n", (long long)sleep_time);
    }
}
```

---

## Command Line Usage

### Running Tests

```bash
# Run all tests
./build/doctest

# Run specific test case
./build/doctest --test-case="scheduler: basic task execution"

# Run tests matching pattern
./build/doctest --test-case="*scheduler*"

# Run specific test suite
./build/doctest --test-suite=scheduler

# Run specific subcase
./build/doctest --test-case="scheduler: task lifecycle" --subcase="multiple tasks"
```

### Filtering and Output

```bash
# Show only failed tests
./build/doctest --no-prefixes --failure-only

# Show test duration
./build/doctest --duration

# Output in JUnit format (for CI)
./build/doctest --junit

# Output in XML format
./build/doctest --xml

# Verbose output
./build/doctest --verbose

# Quiet mode (only failures)
./build/doctest --quiet
```

### Advanced Options

```bash
# Run tests in random order
./build/doctest --rand-seed=time

# Run only N tests
./build/doctest --count=10

# Skip tests marked with skip decorator
./build/doctest --no-skip

# Force color output
./build/doctest --force-colors

# No color output
./build/doctest --no-colors
```

---

## Complete Test File Example

```cpp
// test/executor_test.cpp
#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <atomic>
#include <vector>

class ExecutorFixture {
protected:
    dripcore::scheduler scheduler;
    
    void spawn_flag_task(std::atomic<bool>& flag) {
        scheduler.spawn([&]() -> dripcore::task {
            flag = true;
            co_return;
        });
    }
};

TEST_SUITE("executor") {
    TEST_CASE_FIXTURE(ExecutorFixture, "basic execution") {
        std::atomic<bool> task_ran{false};
        spawn_flag_task(task_ran);
        scheduler.run();
        CHECK(task_ran == true);
    }
    
    TEST_CASE_FIXTURE(ExecutorFixture, "multiple tasks") {
        std::vector<std::atomic<bool>> flags(10);
        for (auto& flag : flags) {
            spawn_flag_task(flag);
        }
        scheduler.run();
        
        for (const auto& flag : flags) {
            CHECK(flag == true);
        }
    }
    
    TEST_CASE("task lifecycle with subcases") {
        dripcore::scheduler scheduler;
        
        SUBCASE("immediate completion") {
            std::atomic<bool> completed{false};
            scheduler.spawn([&]() -> dripcore::task {
                completed = true;
                co_return;
            });
            scheduler.run();
            CHECK(completed == true);
        }
        
        SUBCASE("with yielding") {
            std::atomic<int> yield_count{0};
            scheduler.spawn([&]() -> dripcore::task {
                for (int i = 0; i < 5; ++i) {
                    yield_count++;
                    co_await dripcore::yield();
                }
                co_return;
            });
            scheduler.run();
            CHECK(yield_count == 5);
        }
    }
}
```

---

## Additional Resources

- [Doctest GitHub Repository](https://github.com/doctest/doctest)
- [Doctest Documentation](https://github.com/doctest/doctest/tree/master/doc/markdown)

---

*This guide is tailored for the DripCore project and focuses on patterns most useful for testing coroutine-based task schedulers and I/O operations.*