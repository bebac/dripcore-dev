# Doctest Usage Guide for DripCore

This guide provides comprehensive instructions for writing effective doctest tests for the DripCore coroutine-based task scheduler library. It covers doctest features most relevant to testing asynchronous, cooperative multitasking systems.

## Table of Contents

- [Quick Reference](#quick-reference)
- [Test Organization](#test-organization)
- [Assertion System](#assertion-system)
- [Testing Asynchronous Code](#testing-asynchronous-code)
- [Test Fixtures](#test-fixtures)
- [Test Suites and Decorators](#test-suites-and-decorators)
- [Subcases for Hierarchical Testing](#subcases-for-hierarchical-testing)
- [BDD-Style Testing](#bdd-style-testing)
- [Exception Testing](#exception-testing)
- [Floating Point Comparisons](#floating-point-comparisons)
- [String Containment](#string-containment)
- [Best Practices for DripCore](#best-practices-for-dripcore)
- [Performance Testing](#performance-testing)
- [Command Line Usage](#command-line-usage)

---

## Quick Reference

### Essential Macros

```cpp
// Test case definition
TEST_CASE("test name") { /* ... */ }

// Assertions (3 severity levels)
REQUIRE(condition);    // Fails immediately, stops test
CHECK(condition);      // Fails but continues test
WARN(condition);       // Logs warning, doesn't fail test

// Common comparisons
CHECK_EQ(a, b);        // a == b
CHECK_NE(a, b);        // a != b
CHECK_LT(a, b);        // a < b
CHECK_GT(a, b);        // a > b
CHECK_LE(a, b);        // a <= b
CHECK_GE(a, b);        // a >= b

// Exception testing
CHECK_THROWS(expr);                    // Throws any exception
CHECK_THROWS_AS(expr, std::runtime_error); // Throws specific type
CHECK_NOTHROW(expr);                   // Doesn't throw

// Subcases
SUBCASE("subcase name") { /* ... */ }

// Test fixtures
TEST_CASE_FIXTURE(FixtureClass, "test name") { /* ... */ }

// Test suites
TEST_SUITE("suite name") { /* ... */ }
```

---

## Test Organization

### File Structure

Follow the existing dripcore pattern:

```
/test
├── doctest_main.cpp      # Main entry point with DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
├── dripcore_test.cpp     # Core scheduler and task tests
├── executor_test.cpp      # Executor-specific tests
├── event_loop_test.cpp    # Platform-specific event loop tests
├── io_test.cpp            # I/O-related tests (sockets, pipes)
├── benchmark_test.cpp     # Performance benchmarks
└── integration_test.cpp   # Integration/end-to-end tests
```

### Test Naming Convention

Use descriptive, hierarchical names that reflect the component and scenario:

```cpp
// Good
TEST_CASE("scheduler: task spawning with multiple executors")
TEST_CASE("executor: ready queue processing order")
TEST_CASE("io: socket readable awaiter behavior")

// Avoid
TEST_CASE("test1")
TEST_CASE("scheduler test")
```

---

## Assertion System

### Severity Levels

| Level | Behavior | Use Case |
|-------|----------|----------|
| `REQUIRE` | Stops test immediately on failure | Critical preconditions, setup validation |
| `CHECK` | Continues test, reports all failures | Multiple independent assertions |
| `WARN` | Logs warning, test continues | Non-critical checks, informational |

### Expression Decomposition

Doctest automatically decomposes expressions to show actual values on failure:

```cpp
int a = 4, b = 2;
CHECK(a == b);
// Output on failure:
// CHECK( a == b ) is NOT correct!
//   values: CHECK( 4 == 2 )
```

### Binary vs Expression Asserts

**Expression asserts** (recommended for most cases):
```cpp
CHECK(a == b);
CHECK(a < b && b > c);
REQUIRE(vec.size() > 0);
```

**Binary asserts** (faster compilation, no decomposition):
```cpp
CHECK_EQ(a, b);
CHECK_LT(a, b);
CHECK_GT(a, b);
```

Use binary asserts when:
- Compilation speed is critical
- You don't need value decomposition
- Testing simple comparisons

### Message Asserts

Add context to specific assertions:

```cpp
CHECK_MESSAGE(task_completed, 
              "Task should complete within timeout", 
              timeout_ms, "ms timeout was used");

INFO("Current task count: ", task_count);
CHECK(task_count > 0);
```

---

## Testing Asynchronous Code

### Basic Pattern for DripCore Tasks

```cpp
TEST_CASE("scheduler: basic task execution") {
    dripcore::scheduler scheduler;
    std::atomic<bool> task_ran{false};
    
    class test_task : public dripcore::task {
    public:
        test_task(std::atomic<bool>& flag) : flag_(flag) {}
        
        dripcore::coroutine execute() {
            flag_ = true;
            co_await yield();
            co_return;
        }
    private:
        std::atomic<bool>& flag_;
    };
    
    scheduler.spawn<test_task>(task_ran);
    scheduler.run();
    
    CHECK(task_ran == true);
}
```

### Testing Task Ordering

```cpp
TEST_CASE("scheduler: task execution order") {
    dripcore::scheduler scheduler;
    std::vector<int> execution_order;
    
    class ordered_task : public dripcore::task {
    public:
        ordered_task(std::vector<int>& order, int id) 
            : order_(order), id_(id) {}
        
        dripcore::coroutine execute() {
            order_.push_back(id_);
            co_await yield();
            co_return;
        }
    private:
        std::vector<int>& order_;
        int id_;
    };
    
    // Spawn tasks in order 1, 2, 3
    scheduler.spawn<ordered_task>(execution_order, 1);
    scheduler.spawn<ordered_task>(execution_order, 2);
    scheduler.spawn<ordered_task>(execution_order, 3);
    
    scheduler.run();
    
    // Verify execution order (may vary due to cooperative scheduling)
    CHECK(execution_order.size() == 3);
    CHECK(std::find(execution_order.begin(), execution_order.end(), 1) != execution_order.end());
    CHECK(std::find(execution_order.begin(), execution_order.end(), 2) != execution_order.end());
    CHECK(std::find(execution_order.begin(), execution_order.end(), 3) != execution_order.end());
}
```

### Testing I/O Operations

```cpp
TEST_CASE("io: pipe communication between tasks") {
    dripcore::scheduler scheduler;
    
    int pipefd[2];
    REQUIRE(pipe(pipefd) == 0);
    fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
    
    std::string received_message;
    bool writer_done = false;
    bool reader_done = false;
    
    // Writer task
    class writer_task : public dripcore::task {
    public:
        writer_task(int fd, bool& done) : fd_(fd), done_(done) {}
        
        dripcore::coroutine execute() {
            const char* msg = "Hello from writer!";
            co_await writable(fd_);
            
            ssize_t written = write(fd_, msg, strlen(msg));
            CHECK(written > 0);
            
            close(fd_);
            done_ = true;
            co_return;
        }
    private:
        int fd_;
        bool& done_;
    };
    
    // Reader task
    class reader_task : public dripcore::task {
    public:
        reader_task(int fd, std::string& result, bool& done) 
            : fd_(fd), result_(result), done_(done) {}
        
        dripcore::coroutine execute() {
            char buffer[256];
            co_await readable(fd_);
            
            ssize_t n = read(fd_, buffer, sizeof(buffer) - 1);
            if (n > 0) {
                buffer[n] = '\0';
                result_ = buffer;
            }
            
            close(fd_);
            done_ = true;
            co_return;
        }
    private:
        int fd_;
        std::string& result_;
        bool& done_;
    };
    
    scheduler.spawn<writer_task>(pipefd[1], writer_done);
    scheduler.spawn<reader_task>(pipefd[0], received_message, reader_done);
    
    scheduler.run();
    
    CHECK(writer_done == true);
    CHECK(reader_done == true);
    CHECK(received_message == "Hello from writer!");
}
```

---

## Test Fixtures

Use fixtures for shared setup/teardown across multiple test cases.

### Basic Fixture

```cpp
class SchedulerFixture {
protected:
    dripcore::scheduler scheduler;
    
    // Helper method to create a simple task
    template<typename F>
    void spawn_simple_task(F&& func) {
        scheduler.spawn([func = std::forward<F>(func)]() -> dripcore::task {
            func();
            co_return;
        });
    }
};

TEST_CASE_FIXTURE(SchedulerFixture, "fixture: basic task execution") {
    std::atomic<bool> task_ran{false};
    
    spawn_simple_task([&]() { task_ran = true; });
    scheduler.run();
    
    CHECK(task_ran == true);
}
```

### Fixture with Setup/Teardown

```cpp
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
    // pipefd[0] and pipefd[1] are already set up
    char buffer[10];
    write(pipefd[1], "test", 4);
    
    dripcore::scheduler scheduler;
    bool read_complete = false;
    
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::readable(pipefd[0]);
        read(pipefd[0], buffer, sizeof(buffer));
        read_complete = true;
        co_return;
    });
    
    scheduler.run();
    CHECK(read_complete == true);
}
```

---

## Test Suites and Decorators

### Test Suites

Group related test cases for better organization and filtering:

```cpp
TEST_SUITE("scheduler") {
    TEST_CASE("basic task execution") { /* ... */ }
    TEST_CASE("task ordering") { /* ... */ }
    TEST_CASE("error handling") { /* ... */ }
}

TEST_SUITE("io") {
    TEST_CASE("pipe communication") { /* ... */ }
    TEST_CASE("socket operations") { /* ... */ }
}

// Run specific suite: ./doctest --test-suite=scheduler
```

### Decorators

Add metadata and behavior modifiers to test cases:

```cpp
// Skip a test (useful for known issues or platform-specific tests)
TEST_CASE("platform: linux specific" * doctest::skip(true)) {
    // This test only works on Linux
}

// Set timeout for slow tests
TEST_CASE("stress: many tasks" * doctest::timeout(5.0)) {
    // Test with 5 second timeout
}

// Mark expected failures
TEST_CASE("known issue: executor timeout flaw" * doctest::may_fail(true)) {
    // This test might fail due to known issue
}

// Test that should fail (for verifying bug reproduction)
TEST_CASE("bug reproduction: should crash" * doctest::should_fail(true)) {
    // If this passes, the bug is fixed!
}

// Add descriptive text
TEST_CASE("scheduler: task spawning" * 
          doctest::description("Tests basic task spawning functionality")) {
    // ...
}

// Combine multiple decorators
TEST_CASE("experimental: new feature" * 
          doctest::may_fail(true) * 
          doctest::timeout(2.0) *
          doctest::description("Tests experimental feature - may be unstable")) {
    // ...
}
```

### Suite-Level Decorators

```cpp
TEST_SUITE("stress tests" * doctest::timeout(10.0)) {
    TEST_CASE("many tasks") { /* inherits 10s timeout */ }
    TEST_CASE("heavy io") { /* inherits 10s timeout */ }
    
    TEST_CASE("quick check" * doctest::timeout(1.0)) { 
        /* overrides suite timeout */ 
    }
}
```

---

## Subcases for Hierarchical Testing

Use subcases to organize complex test scenarios with shared setup:

```cpp
TEST_CASE("scheduler: task lifecycle") {
    dripcore::scheduler scheduler;
    
    SUBCASE("single task") {
        bool completed = false;
        scheduler.spawn([&]() -> dripcore::task {
            completed = true;
            co_return;
        });
        scheduler.run();
        CHECK(completed == true);
    }
    
    SUBCASE("multiple tasks") {
        int completed_count = 0;
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
        int yield_count = 0;
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

### Subcase Execution Model

Each subcase path is executed independently. Shared setup code runs for each path:

```cpp
TEST_CASE("io: socket operations") {
    // This setup runs for EACH subcase
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(sockfd >= 0);
    
    SUBCASE("connect") {
        // Test connection
        CHECK(connect(sockfd, /* ... */) == 0);
    }
    
    SUBCASE("bind") {
        // Test binding
        CHECK(bind(sockfd, /* ... */) == 0);
    }
    
    // Cleanup runs for each subcase
    close(sockfd);
}
```

---

## BDD-Style Testing

Doctest supports Behavior-Driven Development syntax for more readable tests:

```cpp
SCENARIO("scheduler handles task execution") {
    dripcore::scheduler scheduler;
    
    GIVEN("a task that performs work") {
        bool work_done = false;
        
        WHEN("the task is spawned and executed") {
            scheduler.spawn([&]() -> dripcore::task {
                work_done = true;
                co_return;
            });
            scheduler.run();
            
            THEN("the work should be completed") {
                CHECK(work_done == true);
            }
        }
    }
    
    GIVEN("multiple tasks with dependencies") {
        int execution_order = 0;
        
        WHEN("tasks are spawned in sequence") {
            scheduler.spawn([&]() -> dripcore::task {
                execution_order = 1;
                co_await dripcore::yield();
                execution_order = 2;
                co_return;
            });
            
            scheduler.spawn([&]() -> dripcore::task {
                execution_order = 3;
                co_return;
            });
            
            scheduler.run();
            
            THEN("all tasks should execute") {
                CHECK(execution_order >= 1);
                CHECK(execution_order <= 3);
            }
        }
    }
}
```

BDD macros map to regular doctest macros:
- `SCENARIO` → `TEST_CASE` with "Scenario: " prefix
- `GIVEN`/`WHEN`/`THEN` → `SUBCASE` with appropriate prefixes
- `AND_GIVEN`/`AND_WHEN`/`AND_THEN` → Additional `SUBCASE` for chaining

---

## Exception Testing

### Basic Exception Testing

```cpp
TEST_CASE("executor: exception handling") {
    dripcore::scheduler scheduler;
    
    SUBCASE("task throws exception") {
        bool exception_caught = false;
        
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
TEST_CASE("io: socket error handling") {
    SUBCASE("invalid socket operations") {
        CHECK_THROWS_AS(
            dripcore::readable(-1), 
            std::runtime_error
        );
        
        CHECK_THROWS_WITH(
            dripcore::writable(-1), 
            "invalid file descriptor"
        );
    }
    
    SUBCASE("connection errors") {
        CHECK_THROWS_WITH_AS(
            connect_to_invalid_address(), 
            "connection refused", 
            std::system_error
        );
    }
}
```

---

## Floating Point Comparisons

Use `doctest::Approx` for floating-point comparisons with tolerance:

```cpp
TEST_CASE("benchmark: timing accuracy") {
    dripcore::scheduler scheduler;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Run some operation
    scheduler.spawn([&]() -> dripcore::task {
        co_await dripcore::sleep(scheduler, std::chrono::milliseconds(100));
        co_return;
    });
    scheduler.run();
    
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    // Allow 10% tolerance for timing
    CHECK(elapsed_ms == doctest::Approx(100).epsilon(0.1));
    
    // Or with absolute tolerance
    CHECK(elapsed_ms == doctest::Approx(100).epsilon(0.05)); // 5% tolerance
}
```

---

## String Containment

Use `doctest::Contains` for partial string matching:

```cpp
TEST_CASE("logging: error messages") {
    std::string error_msg = get_last_error_message();
    
    CHECK(error_msg == doctest::Contains("timeout"));
    CHECK(error_msg == doctest::Contains("task") && 
          error_msg == doctest::Contains("failed"));
}

TEST_CASE("io: exception messages") {
    CHECK_THROWS_WITH(
        invalid_operation(), 
        doctest::Contains("invalid")
    );
}
```

---

## Best Practices for DripCore

### 1. Test Isolation

Each test case should be independent:

```cpp
// Good - each test creates its own scheduler
TEST_CASE("scheduler: test 1") {
    dripcore::scheduler scheduler;
    // ...
}

TEST_CASE("scheduler: test 2") {
    dripcore::scheduler scheduler; // Fresh scheduler
    // ...
}

// Bad - shared state between tests
static dripcore::scheduler shared_scheduler; // Don't do this!
```

### 2. Use Atomic Variables for Shared State

```cpp
TEST_CASE("concurrency: multiple tasks accessing shared state") {
    dripcore::scheduler scheduler;
    std::atomic<int> counter{0};
    
    for (int i = 0; i < 100; ++i) {
        scheduler.spawn([&]() -> dripcore::task {
            counter++;
            co_return;
        });
    }
    
    scheduler.run();
    CHECK(counter == 100);
}
```

### 3. Clean Up Resources

```cpp
TEST_CASE("io: socket cleanup") {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(sockfd >= 0);
    
    // Use RAII or ensure cleanup
    auto cleanup = [&]() { if (sockfd >= 0) close(sockfd); };
    
    // Test code...
    
    cleanup(); // Ensure cleanup even if test fails
}
```

### 4. Use REQUIRE for Critical Setup

```cpp
TEST_CASE("io: pipe operations") {
    int pipefd[2];
    REQUIRE(pipe(pipefd) == 0); // Critical setup - fail immediately if this fails
    
    // Rest of test...
}
```

### 5. Test Edge Cases

```cpp
TEST_CASE("scheduler: edge cases") {
    dripcore::scheduler scheduler;
    
    SUBCASE("zero tasks") {
        scheduler.run(); // Should handle gracefully
        CHECK(true); // If we get here, it's good
    }
    
    SUBCASE("task that never yields") {
        bool completed = false;
        scheduler.spawn([&]() -> dripcore::task {
            completed = true;
            // No yield - task completes immediately
            co_return;
        });
        scheduler.run();
        CHECK(completed == true);
    }
    
    SUBCASE("task with infinite yield loop") {
        int yield_count = 0;
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

### 6. Test Error Conditions

```cpp
TEST_CASE("executor: error conditions") {
    dripcore::scheduler scheduler;
    
    SUBCASE("invalid file descriptor") {
        CHECK_THROWS_AS(
            dripcore::readable(-1), 
            std::runtime_error
        );
    }
    
    SUBCASE("closed file descriptor") {
        int fd = open("/dev/null", O_RDONLY);
        close(fd);
        
        CHECK_THROWS_AS(
            dripcore::readable(fd), 
            std::runtime_error
        );
    }
}
```

---

## Performance Testing

### Simple Benchmarks

```cpp
TEST_CASE("benchmark: task spawn performance") {
    dripcore::scheduler scheduler;
    
    const int iterations = 1000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < iterations; ++i) {
        bool done = false;
        scheduler.spawn([&]() -> dripcore::task {
            done = true;
            co_return;
        });
        scheduler.run();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::printf("Task spawn benchmark: %lld ms for %d iterations (%.2f μs per spawn)\n",
                (long long)total_ms, iterations, (total_ms * 1000.0) / iterations);
    
    // Basic sanity check
    CHECK(total_ms > 0);
    CHECK(total_ms < 10000); // Should complete in reasonable time
}
```

### Comparative Benchmarks

```cpp
TEST_CASE("benchmark: yield vs sleep performance") {
    dripcore::scheduler scheduler;
    
    SUBCASE("pure yield") {
        int yield_count = 0;
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
        int sleep_count = 0;
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
# Run tests in random order (good for detecting dependencies)
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

## Example: Complete Test File

Here's a complete example showing best practices:

```cpp
// test/executor_test.cpp
#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <atomic>
#include <vector>

// Test fixture for executor tests
class ExecutorFixture {
protected:
    dripcore::scheduler scheduler;
    
    // Helper to create a task that sets a flag
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
            bool completed = false;
            scheduler.spawn([&]() -> dripcore::task {
                completed = true;
                co_return;
            });
            scheduler.run();
            CHECK(completed == true);
        }
        
        SUBCASE("with yielding") {
            int yield_count = 0;
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
- [DripCore Testing Strategy](testing_strategy.md)

---

*This guide is specifically tailored for the DripCore project and focuses on patterns and features most useful for testing coroutine-based task schedulers and I/O operations.*