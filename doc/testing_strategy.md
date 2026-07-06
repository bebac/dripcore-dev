# DripCore Testing Strategy and Migration Plan

## Executive Summary

This document outlines a comprehensive testing strategy for the DripCore library, addressing the current test organization, coverage gaps, benchmarking needs, and a migration plan for implementing the proposed continuation support and multithreaded scheduler features.

**Current State:**
- Basic unit tests exist using doctest framework
- Some benchmarks are embedded in test files
- Test coverage is incomplete, especially for edge cases
- No formal benchmark suite exists
- Tests are not well-organized for the upcoming architectural changes

**Target State:**
- Well-organized test hierarchy mirroring library structure
- Comprehensive unit, integration, and stress tests
- Formal benchmark suite with performance regression detection
- Test coverage for all proposed features (continuations, multithreading)
- CI/CD integration for automated testing and benchmarking

---

## 1. Current Test Analysis

### 1.1 Existing Test Files

| File | Purpose | Status | Issues |
|------|---------|--------|--------|
| `test/doctest_main.cpp` | Doctest runner | ✅ Complete | None |
| `test/dripcore_test.cpp` | Core scheduler/executor tests | ⚠️ Partial | Contains commented-out tests, embedded benchmarks |
| `test/channel_test.cpp` | Channel functionality tests | ✅ Complete | Well-structured, good coverage |
| `test/sleep_test.cpp` | Sleep/timer functionality tests | ✅ Complete | Some tests commented out |

### 1.2 Current Test Coverage

#### Well-Covered Areas:
- ✅ Basic task spawning and execution
- ✅ Yield functionality
- ✅ Pipe I/O communication
- ✅ Channel operations (send, receive, close, try_send)
- ✅ Sleep/timer functionality
- ✅ Basic I/O fairness

#### Missing Coverage:
- ❌ Error handling and edge cases
- ❌ Memory management and lifetime scenarios
- ❌ Concurrent task scenarios (preparing for multithreading)
- ❌ Event loop edge cases
- ❌ Socket operations beyond basic pipes
- ❌ Timer edge cases (cancellation, overflow)
- ❌ Performance characteristics under load

### 1.3 Current Test Issues

1. **Commented-Out Tests**: Several important tests are disabled (`#if 0` blocks)
   - Sleep test in `dripcore_test.cpp` (lines 61-91)
   - Yield test in `dripcore_test.cpp` (lines 93-115)
   - Many timers test in `dripcore_test.cpp` (lines 117-134)
   - Free function sleep test in `sleep_test.cpp` (lines 42-67)

2. **Embedded Benchmarks**: Performance tests mixed with unit tests
   - Makes it hard to separate functional testing from performance testing
   - No baseline tracking or regression detection

3. **Organization Issues**:
   - Tests are not grouped by component
   - No clear separation between unit, integration, and benchmark tests
   - Test files don't mirror library structure

4. **Missing Test Types**:
   - No stress tests
   - No concurrency tests
   - No error injection tests
   - No memory leak tests

---

## 2. Proposed Test Organization

### 2.1 Directory Structure

```
test/
├── CMakeLists.txt                    # Test build configuration
├── doctest_main.cpp                 # Doctest runner (existing)
│
├── unit/                            # Unit tests
│   ├── CMakeLists.txt
│   ├── scheduler/                   # Scheduler-specific tests
│   │   ├── scheduler_test.cpp
│   │   ├── executor_test.cpp
│   │   └── task_test.cpp
│   ├── io/                          # I/O-related tests
│   │   ├── pipe_test.cpp
│   │   ├── socket_test.cpp
│   │   └── io_awaiter_test.cpp
│   ├── timing/                      # Timer/sleep tests
│   │   ├── sleep_test.cpp           # Move existing
│   │   ├── timer_test.cpp
│   │   └── timeout_test.cpp
│   ├── concurrency/                 # Concurrency primitives
│   │   ├── channel_test.cpp         # Move existing
│   │   ├── mutex_test.cpp
│   │   └── atomic_test.cpp
│   └── platform/                    # Platform-specific tests
│       ├── kqueue_test.cpp
│       └── epoll_test.cpp
│
├── integration/                     # Integration tests
│   ├── CMakeLists.txt
│   ├── echo_server_test.cpp        # Test the example echo server
│   ├── http_client_test.cpp        # HTTP client scenarios
│   └── stress_test.cpp             # Stress scenarios
│
├── benchmark/                       # Performance benchmarks
│   ├── CMakeLists.txt
│   ├── scheduler_bench.cpp          # Scheduler performance
│   ├── io_bench.cpp                 # I/O performance
│   ├── context_switch_bench.cpp     # Context switching overhead
│   └── memory_bench.cpp             # Memory usage patterns
│
├── regression/                      # Regression tests
│   ├── CMakeLists.txt
│   └── performance_regression.cpp   # Performance regression detection
│
└── fixtures/                        # Test fixtures and helpers
    ├── test_utils.h
    ├── mock_event_loop.h
    └── benchmark_utils.h
```

### 2.2 Test File Naming Convention

- Unit tests: `<component>_test.cpp` (e.g., `scheduler_test.cpp`)
- Integration tests: `<scenario>_test.cpp` (e.g., `echo_server_test.cpp`)
- Benchmarks: `<focus>_bench.cpp` (e.g., `context_switch_bench.cpp`)
- Test utilities: `test_<utility>.h` (e.g., `test_utils.h`)

### 2.3 Test Component Mapping

| Library Component | Test Location | Current Status |
|-----------------|---------------|----------------|
| `scheduler.h/cpp` | `unit/scheduler/scheduler_test.cpp` | ⚠️ Needs expansion |
| `executor.h/cpp` | `unit/scheduler/executor_test.cpp` | ❌ Missing |
| `task.h` | `unit/scheduler/task_test.cpp` | ❌ Missing |
| `coroutine.h/cpp` | `unit/scheduler/coroutine_test.cpp` | ❌ Missing |
| `yield_awaiter.h/cpp` | `unit/scheduler/yield_awaiter_test.cpp` | ❌ Missing |
| `io_awaiter.h/cpp` | `unit/io/io_awaiter_test.cpp` | ❌ Missing |
| `sleep_awaiter.h/cpp` | `unit/timing/sleep_test.cpp` | ✅ Exists (move) |
| `channel.h` | `unit/concurrency/channel_test.cpp` | ✅ Exists (move) |
| `socket.h/cpp` | `unit/io/socket_test.cpp` | ❌ Missing |
| `socket_address.h` | `unit/io/socket_address_test.cpp` | ❌ Missing |
| `event_loop.h` | `unit/platform/event_loop_test.cpp` | ❌ Missing |
| `event_loop_kqueue.cpp` | `unit/platform/kqueue_test.cpp` | ❌ Missing |
| `event_loop_epoll.cpp` | `unit/platform/epoll_test.cpp` | ❌ Missing |

---

## 3. Test Coverage Requirements

### 3.1 Unit Test Coverage Matrix

#### Core Scheduler/Executor
- [ ] Scheduler construction with different executor counts
- [ ] Task spawning and execution order
- [ ] Task completion and cleanup
- [ ] Error propagation in tasks
- [ ] Executor thread-local access
- [ ] Multiple executor scenarios (for multithreading)
- [ ] Work stealing behavior
- [ ] Load balancing across executors

#### Coroutine Infrastructure
- [ ] Coroutine creation and destruction
- [ ] Promise type behavior
- [ ] Exception handling in coroutines
- [ ] Coroutine suspension and resumption
- [ ] Nested coroutine scenarios

#### Awaiters
- [ ] `yield_awaiter` behavior
- [ ] `io_awaiter` with different file descriptors
- [ ] `sleep_awaiter` accuracy and cancellation
- [ ] Awaiter composition
- [ ] Awaiter error handling

#### I/O Operations
- [ ] Pipe read/write operations
- [ ] Socket operations (TCP, UDP)
- [ ] Non-blocking vs blocking behavior
- [ ] Error conditions (EBADF, EAGAIN, etc.)
- [ ] Event registration and deregistration
- [ ] Multiple I/O events on same descriptor

#### Channels
- [ ] Bounded and unbounded channels
- [ ] Send/receive operations
- [ ] Try_send behavior when full
- [ ] Close and drain behavior
- [ ] Multiple producers/consumers
- [ ] Move-only types in channels
- [ ] Exception safety

#### Timers
- [ ] Timer registration and firing
- [ ] Timer cancellation
- [ ] Multiple timers with different durations
- [ ] Timer accuracy under load
- [ ] Timer wheel efficiency

### 3.2 Integration Test Scenarios

#### Basic Scenarios
- [ ] Echo server with multiple clients
- [ ] HTTP client making requests
- [ ] File I/O operations
- [ ] Mixed I/O and CPU tasks

#### Advanced Scenarios
- [ ] High concurrency scenarios (1000+ connections)
- [ ] Long-running tasks with periodic yields
- [ ] Resource exhaustion scenarios
- [ ] Error injection and recovery

#### Multithreading Scenarios (Phase 3+)
- [ ] Cross-executor task spawning
- [ ] Work stealing under load
- [ ] Thread affinity scenarios
- [ ] Shared resource access
- [ ] Synchronization primitives

### 3.3 Benchmark Requirements

#### Performance Metrics to Track
1. **Task Spawning**: Time to spawn and execute a simple task
2. **Context Switching**: Overhead of yielding between tasks
3. **I/O Throughput**: Operations per second for different I/O types
4. **Memory Usage**: Memory overhead per task/coroutine
5. **Scalability**: Performance with increasing task counts
6. **Concurrency**: Performance with multiple executors

#### Benchmark Types
1. **Microbenchmarks**: Isolated component performance
2. **Macrobenchmarks**: End-to-end scenario performance
3. **Regression Benchmarks**: Detect performance regressions
4. **Stress Benchmarks**: Performance under extreme load

---

## 4. Test Implementation Plan

### 4.1 Phase 1: Test Infrastructure (Week 1)

**Objective**: Establish the new test organization and infrastructure.

#### Tasks:
1. **Create Directory Structure**
   - Create `test/unit/`, `test/integration/`, `test/benchmark/`, `test/regression/`, `test/fixtures/`
   - Move existing tests to appropriate locations

2. **Update CMake Configuration**
   - Create separate CMake targets for unit tests, integration tests, benchmarks
   - Add test discovery and grouping
   - Configure different build flags for different test types

3. **Create Test Utilities**
   - `test_utils.h`: Common test helpers, assertions, fixtures
   - `benchmark_utils.h`: Benchmarking infrastructure, statistics collection
   - `mock_event_loop.h`: Mock implementations for unit testing

4. **Migrate Existing Tests**
   - Move `channel_test.cpp` to `unit/concurrency/channel_test.cpp`
   - Move `sleep_test.cpp` to `unit/timing/sleep_test.cpp`
   - Split `dripcore_test.cpp` into component-specific files
   - Enable commented-out tests with fixes

#### Deliverables:
- New test directory structure
- Updated CMake configuration
- Test utility headers
- Migrated existing tests

### 4.2 Phase 2: Core Coverage (Week 2-3)

**Objective**: Achieve comprehensive coverage of existing functionality.

#### Tasks:
1. **Executor Tests**
   - Test thread-local accessor
   - Test task lifecycle management
   - Test ready queue processing
   - Test event loop integration

2. **Scheduler Tests**
   - Test single executor scenarios
   - Test task spawning and ordering
   - Test error handling
   - Test cleanup behavior

3. **Awaiter Tests**
   - Test each awaiter type in isolation
   - Test awaiter composition
   - Test error conditions

4. **I/O Tests**
   - Test pipe operations
   - Test socket operations
   - Test event registration
   - Test error handling

5. **Timer Tests**
   - Test sleep accuracy
   - Test timer cancellation
   - Test multiple timers
   - Test timer wheel behavior

#### Deliverables:
- Complete unit test coverage for existing functionality
- All core components have dedicated test files
- Edge cases and error conditions covered

### 4.3 Phase 3: Continuation Support Tests (Week 4)

**Objective**: Add tests for the proposed continuation support feature.

#### Tasks:
1. **Awaitable Type Tests**
   - Test awaitable creation and destruction
   - Test awaitable state transitions
   - Test continuation registration
   - Test result propagation
   - Test exception handling

2. **Composition Tests**
   - Test awaitable composition
   - Test nested awaitables
   - Test complex dependency chains

3. **Integration Tests**
   - Test continuations with I/O operations
   - Test continuations with timers
   - Test continuations with channels

4. **Performance Tests**
   - Test continuation overhead
   - Test composition performance

#### Deliverables:
- Complete test coverage for continuation support
- Integration tests for continuation scenarios
- Performance benchmarks for continuation overhead

### 4.4 Phase 4: Multithreading Tests (Week 5-6)

**Objective**: Add tests for multithreaded scheduler functionality.

#### Tasks:
1. **Basic Multithreading Tests**
   - Test multiple executor scenarios
   - Test thread-local accessor with multiple threads
   - Test cross-executor communication

2. **Work Stealing Tests**
   - Test work distribution
   - Test load balancing
   - Test work stealing scenarios

3. **Concurrency Tests**
   - Test shared resource access
   - Test synchronization primitives
   - Test thread safety

4. **Stress Tests**
   - Test high concurrency scenarios
   - Test resource exhaustion
   - Test long-running scenarios

5. **Performance Tests**
   - Test scalability with multiple cores
   - Test work stealing overhead
   - Test cross-executor communication overhead

#### Deliverables:
- Complete test coverage for multithreading features
- Stress tests for high concurrency scenarios
- Performance benchmarks for multithreaded execution

### 4.5 Phase 5: Benchmark Suite (Week 7)

**Objective**: Create a comprehensive benchmark suite.

#### Tasks:
1. **Microbenchmark Implementation**
   - Context switching benchmark
   - Task spawning benchmark
   - I/O operation benchmarks
   - Memory usage benchmarks

2. **Macrobenchmark Implementation**
   - Echo server benchmark
   - HTTP client benchmark
   - File I/O benchmark

3. **Regression Detection**
   - Baseline performance tracking
   - Regression detection thresholds
   - Performance reporting

4. **CI/CD Integration**
   - Automated benchmark execution
   - Performance trend tracking
   - Alerting for regressions

#### Deliverables:
- Complete benchmark suite
- Baseline performance measurements
- Regression detection infrastructure
- CI/CD integration for benchmarks

---

## 5. Specific Test Cases Needed

### 5.1 For Continuation Support Proposal

#### Awaitable Type Tests
```cpp
// test/unit/scheduler/awaitable_test.cpp

TEST_CASE("awaitable: basic creation and completion") {
    // Test awaitable creation, completion, and result retrieval
}

TEST_CASE("awaitable: continuation registration") {
    // Test that continuations are properly registered and notified
}

TEST_CASE("awaitable: exception propagation") {
    // Test that exceptions are properly propagated through awaitables
}

TEST_CASE("awaitable: composition") {
    // Test composing multiple awaitables
}

TEST_CASE("awaitable: thread affinity") {
    // Test thread affinity enforcement for awaitables
}
```

#### Continuation Integration Tests
```cpp
// test/integration/continuation_integration_test.cpp

TEST_CASE("continuation: I/O composition") {
    // Test continuations with I/O operations
}

TEST_CASE("continuation: timer composition") {
    // Test continuations with timer operations
}

TEST_CASE("continuation: channel composition") {
    // Test continuations with channel operations
}

TEST_CASE("continuation: complex workflows") {
    // Test complex workflows using continuations
}
```

### 5.2 For Multithreaded Scheduler Proposal

#### Executor Tests
```cpp
// test/unit/scheduler/executor_test.cpp

TEST_CASE("executor: thread-local access") {
    // Test executor::current() with multiple threads
}

TEST_CASE("executor: task distribution") {
    // Test task distribution across multiple executors
}

TEST_CASE("executor: work stealing") {
    // Test work stealing behavior
}

TEST_CASE("executor: load balancing") {
    // Test load balancing across executors
}
```

#### Scheduler Tests
```cpp
// test/unit/scheduler/scheduler_test.cpp

TEST_CASE("scheduler: multiple executors") {
    // Test scheduler with multiple executors
}

TEST_CASE("scheduler: cross-executor spawning") {
    // Test spawning tasks on specific executors
}

TEST_CASE("scheduler: shutdown behavior") {
    // Test graceful shutdown with multiple executors
}

TEST_CASE("scheduler: error handling") {
    // Test error handling in multithreaded scenarios
}
```

#### Concurrency Tests
```cpp
// test/unit/concurrency/multithreading_test.cpp

TEST_CASE("multithreading: shared channel access") {
    // Test channel access from multiple executors
}

TEST_CASE("multithreading: shared resource access") {
    // Test shared resource access with proper synchronization
}

TEST_CASE("multithreading: task migration") {
    // Test task migration between executors
}

TEST_CASE("multithreading: thread safety") {
    // Test thread safety of all shared data structures
}
```

#### Stress Tests
```cpp
// test/integration/stress_test.cpp

TEST_CASE("stress: high concurrency") {
    // Test with 1000+ concurrent tasks
}

TEST_CASE("stress: resource exhaustion") {
    // Test behavior under resource exhaustion
}

TEST_CASE("stress: long running") {
    // Test long-running scenarios (minutes to hours)
}

TEST_CASE("stress: mixed workload") {
    // Test mixed CPU and I/O workloads
}
```

### 5.3 Benchmark Tests

#### Microbenchmarks
```cpp
// test/benchmark/context_switch_bench.cpp

void benchmark_context_switching() {
    // Measure context switching overhead
    // Compare with baseline
    // Detect regressions
}

void benchmark_task_spawning() {
    // Measure task spawning overhead
    // Test with different task sizes
    // Measure memory allocation patterns
}
```

#### Macrobenchmarks
```cpp
// test/benchmark/echo_server_bench.cpp

void benchmark_echo_server_throughput() {
    // Measure echo server throughput
    // Test with different client counts
    // Measure latency percentiles
}

void benchmark_http_client() {
    // Measure HTTP client performance
    // Test with different request sizes
    // Measure concurrent request handling
}
```

---

## 6. Test Quality Requirements

### 6.1 Test Characteristics

All tests should adhere to the following quality standards:

1. **Deterministic**: Tests should produce the same result on every run
2. **Isolated**: Tests should not depend on each other
3. **Fast**: Unit tests should complete in milliseconds
4. **Clear**: Test intent should be obvious from the test name and structure
5. **Comprehensive**: Tests should cover both happy paths and edge cases
6. **Maintainable**: Tests should be easy to understand and modify

### 6.2 Test Naming Convention

Use descriptive test names that clearly indicate what is being tested:

```cpp
// Good
TEST_CASE("channel: try_send returns false when full")
TEST_CASE("scheduler: tasks execute in FIFO order")
TEST_CASE("executor: thread-local access returns correct executor")

// Bad
TEST_CASE("channel test 1")
TEST_CASE("test channel")
TEST_CASE("scheduler")
```

### 6.3 Test Structure

Follow the Arrange-Act-Assert pattern:

```cpp
TEST_CASE("example: test description") {
    // Arrange
    dripcore::scheduler scheduler;
    bool task_ran = false;
    
    // Act
    scheduler.spawn<test_task>(task_ran);
    scheduler.run();
    
    // Assert
    CHECK(task_ran == true);
}
```

### 6.4 Error Handling in Tests

- Use `REQUIRE` for preconditions that must be true for the test to make sense
- Use `CHECK` for conditions that are being tested
- Test both success and failure paths
- Test edge cases and boundary conditions

---

## 7. Benchmark Requirements

### 7.1 Benchmark Characteristics

1. **Reproducible**: Benchmarks should produce consistent results
2. **Isolated**: Benchmarks should measure one thing at a time
3. **Statistical**: Benchmarks should run multiple iterations and report statistics
4. **Comparable**: Benchmarks should allow comparison with historical results
5. **Actionable**: Benchmark results should be actionable for optimization

### 7.2 Benchmark Metrics

For each benchmark, track the following metrics:

- **Mean**: Average value across all iterations
- **Minimum**: Best observed value
- **Maximum**: Worst observed value
- **Standard Deviation**: Variability of results
- **Percentiles**: 50th, 90th, 99th percentiles
- **Throughput**: Operations per second
- **Latency**: Time per operation

### 7.3 Benchmark Reporting

Benchmark results should be reported in a standardized format:

```
[BENCHMARK] context_switch: 
  Iterations: 100000
  Mean: 45.2 ns/op
  Min: 38 ns/op
  Max: 120 ns/op
  Std Dev: 8.2 ns
  50th percentile: 42 ns/op
  90th percentile: 55 ns/op
  99th percentile: 80 ns/op
  Throughput: 22.1 M ops/sec
```

### 7.4 Performance Regression Detection

Implement performance regression detection with the following approach:

1. **Baseline Establishment**: Run benchmarks multiple times to establish baseline
2. **Threshold Setting**: Set regression thresholds (e.g., 5% degradation)
3. **Automated Detection**: Automatically detect and report regressions
4. **Historical Tracking**: Track performance over time
5. **Alerting**: Alert developers to significant regressions

---

## 8. CI/CD Integration

### 8.1 Test Pipeline

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cmake -S . -B build
      - run: cmake --build build
      - run: ctest --test-dir build -R unit

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cmake -S . -B build
      - run: cmake --build build
      - run: ctest --test-dir build -R integration

  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cmake -S . -B build
      - run: cmake --build build
      - run: ./build/benchmarks --report
      - run: ./scripts/check_regression.py
```

### 8.2 Benchmark Pipeline

```yaml
# .github/workflows/benchmark.yml
name: Benchmark

on:
  schedule:
    - cron: '0 0 * * *'  # Daily
  workflow_dispatch:

jobs:
  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cmake -S . -B build
      - run: cmake --build build
      - run: ./build/benchmarks --json > results.json
      - run: ./scripts/upload_benchmark.py results.json
```

---

## 9. Migration Plan

### 9.1 Immediate Actions (This Week)

1. **Enable Commented-Out Tests**
   - Review and fix the commented-out tests in `dripcore_test.cpp` and `sleep_test.cpp`
   - Ensure they pass with current implementation
   - Add them to the appropriate test files

2. **Create Initial Test Structure**
   - Create the new directory structure
   - Move existing tests to appropriate locations
   - Update CMake configuration

3. **Add Basic Test Utilities**
   - Create `test_utils.h` with common helpers
   - Create `benchmark_utils.h` with basic benchmarking infrastructure

### 9.2 Short-Term Actions (Next 2 Weeks)

1. **Implement Core Unit Tests**
   - Add executor-specific tests
   - Add awaiter-specific tests
   - Add I/O-specific tests
   - Add timer-specific tests

2. **Create Initial Benchmarks**
   - Move embedded benchmarks to separate files
   - Add basic performance tracking
   - Create baseline measurements

3. **Set Up CI/CD**
   - Configure GitHub Actions for testing
   - Set up automated test execution
   - Configure test reporting

### 9.3 Medium-Term Actions (Next Month)

1. **Add Continuation Tests**
   - Implement tests for awaitable type
   - Add continuation composition tests
   - Add integration tests for continuations

2. **Add Multithreading Tests**
   - Implement basic multithreading tests
   - Add work stealing tests
   - Add concurrency tests

3. **Enhance Benchmarks**
   - Add microbenchmarks for all core components
   - Add macrobenchmarks for common scenarios
   - Implement regression detection

### 9.4 Long-Term Actions (Ongoing)

1. **Maintain Test Coverage**
   - Add tests for new features as they are implemented
   - Maintain existing test coverage
   - Regularly review and update tests

2. **Performance Optimization**
   - Use benchmarks to identify optimization opportunities
   - Track performance improvements
   - Prevent performance regressions

3. **Test Quality Improvement**
   - Regularly review test quality
   - Improve test organization and clarity
   - Add missing edge case coverage

---

## 10. Success Criteria

### 10.1 Phase 1 Success (Test Infrastructure)
- [ ] New test directory structure in place
- [ ] CMake configuration updated for new structure
- [ ] Test utilities created and functional
- [ ] Existing tests migrated to new structure
- [ ] All existing tests still pass

### 10.2 Phase 2 Success (Core Coverage)
- [ ] All core components have dedicated test files
- [ ] Unit test coverage > 90% for existing functionality
- [ ] All edge cases and error conditions covered
- [ ] All tests pass reliably

### 10.3 Phase 3 Success (Continuation Tests)
- [ ] Complete test coverage for continuation support
- [ ] Integration tests for continuation scenarios
- [ ] Performance benchmarks for continuation overhead
- [ ] All continuation-related tests pass

### 10.4 Phase 4 Success (Multithreading Tests)
- [ ] Complete test coverage for multithreading features
- [ ] Stress tests for high concurrency scenarios
- [ ] Performance benchmarks for multithreaded execution
- [ ] All multithreading tests pass

### 10.5 Phase 5 Success (Benchmark Suite)
- [ ] Complete benchmark suite implemented
- [ ] Baseline performance measurements established
- [ ] Regression detection working
- [ ] CI/CD integration for benchmarks complete

---

## 11. Tools and Libraries

### 11.1 Testing Frameworks
- **Doctest**: Primary unit testing framework (already in use)
- **Google Test**: Optional alternative for more complex scenarios
- **Catch2**: Optional alternative with more features

### 11.2 Benchmarking Libraries
- **Google Benchmark**: Industry-standard benchmarking library
- **Custom Implementation**: Lightweight benchmarking for simple cases
- **Celero**: Alternative benchmarking framework

### 11.3 Coverage Tools
- **gcov/lcov**: Code coverage analysis
- **llvm-cov**: LLVM-based coverage analysis
- **Coveralls/Codecov**: Coverage reporting services

### 11.4 CI/CD Tools
- **GitHub Actions**: Primary CI/CD platform
- **GitLab CI**: Alternative for GitLab repositories
- **Jenkins**: Alternative for self-hosted CI

---

## 12. Appendix: Example Test Files

### 12.1 Example Unit Test File

```cpp
// test/unit/scheduler/executor_test.cpp

#include "doctest/doctest.h"
#include "dripcore/executor.h"
#include "dripcore/task.h"
#include "test_utils.h"

namespace {

class simple_task : public dripcore::task {
public:
    explicit simple_task(int& counter) : counter_(counter) {}
    
    dripcore::coroutine execute() {
        counter_++;
        co_await yield();
        counter_++;
        co_return;
    }

private:
    int& counter_;
};

} // namespace

TEST_CASE("executor: basic task execution") {
    dripcore::executor executor;
    int counter = 0;
    
    executor.spawn<simple_task>(counter);
    executor.run();
    
    CHECK(counter == 2);
}

TEST_CASE("executor: multiple task execution") {
    dripcore::executor executor;
    int counter = 0;
    const int num_tasks = 10;
    
    for (int i = 0; i < num_tasks; ++i) {
        executor.spawn<simple_task>(counter);
    }
    executor.run();
    
    CHECK(counter == num_tasks * 2);
}

TEST_CASE("executor: thread-local access") {
    // This test should be expanded for multithreading
    auto* executor = dripcore::executor::current();
    CHECK(executor != nullptr);
}
```

### 12.2 Example Benchmark File

```cpp
// test/benchmark/context_switch_bench.cpp

#include "benchmark_utils.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

namespace {

class yield_task : public dripcore::task {
public:
    explicit yield_task(int& counter) : counter_(counter) {}
    
    dripcore::coroutine execute() {
        for (int i = 0; i < 100; ++i) {
            co_await yield();
        }
        counter_++;
        co_return;
    }

private:
    int& counter_;
};

} // namespace

void benchmark_context_switching() {
    dripcore::scheduler scheduler;
    int completed = 0;
    const int num_tasks = 1000;
    
    for (int i = 0; i < num_tasks; ++i) {
        scheduler.spawn<yield_task>(completed);
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    scheduler.run();
    auto end = std::chrono::high_resolution_clock::now();
    
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    auto per_op = elapsed.count() / (num_tasks * 100); // 100 yields per task
    
    benchmark::report("context_switch", num_tasks * 100, elapsed.count(), per_op);
}

int main() {
    benchmark::initialize();
    
    for (int i = 0; i < 10; ++i) {
        benchmark_context_switching();
    }
    
    benchmark::finalize();
    return 0;
}
```

### 12.3 Example Integration Test File

```cpp
// test/integration/echo_server_test.cpp

#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/socket.h"
#include "test_utils.h"

#include <thread>
#include <vector>

TEST_CASE("integration: echo server with multiple clients") {
    dripcore::scheduler scheduler;
    const int port = test_utils::find_free_port();
    const int num_clients = 10;
    const int messages_per_client = 5;
    
    // Start echo server
    auto server_task = scheduler.spawn<echo_server_task>(port);
    
    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Start client threads
    std::vector<std::thread> clients;
    std::vector<std::string> results(num_clients);
    
    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back([&, i]() {
            results[i] = test_utils::echo_client(port, messages_per_client);
        });
    }
    
    // Wait for clients to complete
    for (auto& client : clients) {
        client.join();
    }
    
    // Shutdown server
    scheduler.shutdown();
    
    // Verify all clients received correct responses
    for (const auto& result : results) {
        CHECK(result == test_utils::expected_echo_response(messages_per_client));
    }
}
```

---

## 13. References

- [DripCore Continuation Support Proposal](dripcore_continuations_proposal.md)
- [Multithreaded Scheduler Proposal](multithreaded_scheduler_proposal.md)
- [Doctest Documentation](https://github.com/doctest/doctest)
- [Google Benchmark Documentation](https://github.com/google/benchmark)
- [Modern C++ Testing Best Practices](https://www.boost.org/doc/libs/release/libs/test/)

---

*Document Version: 1.0*
*Last Updated: 2026-07-06*
*Author: Oz (with input from Benny Bach)*