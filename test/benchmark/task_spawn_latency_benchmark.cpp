#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <atomic>

TEST_CASE("benchmark: task spawn latency - yield vs no-yield comparison")
{
    const int runs = 5;

    // WITHOUT YIELD: Task completes immediately without suspending
    // Measures: spawn overhead + direct execution
    std::printf("\n=== Single task execution without yield (direct execution) ===\n");
    for (int run = 0; run < runs; ++run) {
        dripcore::scheduler executor;
        std::atomic<bool> task_done = false;

        class no_yield_task : public dripcore::task
        {
        public:
            no_yield_task(std::atomic<bool>& done) : done_(done) {}
            dripcore::awaitable<> body() override {
                done_ = true;
                co_return;
            }
        private:
            std::atomic<bool>& done_;
        };

        auto start = std::chrono::high_resolution_clock::now();
        executor.spawn<no_yield_task>(task_done);
        executor.run();
        auto end = std::chrono::high_resolution_clock::now();

        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::printf("  Run %d: %lld μs (%.3f ms)\n", run + 1, (long long)elapsed_us, elapsed_us / 1000.0);
        CHECK(task_done == true);
    }

    // WITH YIELD: Task suspends once, then resumes
    // Measures: spawn + suspend + executor loop iteration + resume
    std::printf("\n=== Single task execution with yield (suspend/resume cycle) ===\n");
    for (int run = 0; run < runs; ++run) {
        dripcore::scheduler executor;
        std::atomic<bool> task_done = false;

        class yield_task : public dripcore::task
        {
        public:
            yield_task(std::atomic<bool>& done) : done_(done) {}
            dripcore::awaitable<> body() override {
                co_await yield();
                done_ = true;
                co_return;
            }
        private:
            std::atomic<bool>& done_;
        };

        auto start = std::chrono::high_resolution_clock::now();
        executor.spawn<yield_task>(task_done);
        executor.run();
        auto end = std::chrono::high_resolution_clock::now();

        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::printf("  Run %d: %lld μs (%.3f ms)\n", run + 1, (long long)elapsed_us, elapsed_us / 1000.0);
        CHECK(task_done == true);
    }
}