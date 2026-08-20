#include "doctest/doctest.h"
#include "dripcore/scheduler.h"
#include "dripcore/task.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <algorithm>

// Measures the per-yield cost by amortizing over many iterations.
//
// Task A: loops N times doing nothing        -> spawn + N loop iterations
// Task B: loops N times, each co_await yield() -> spawn + N yield cycles
// Per-yield cost = (B - A) / N
//
// With N = 10_000, even a 1 us/yield cost shows up as ~10 ms total,
// well above scheduler setup noise.

TEST_CASE("benchmark: yield cost - amortized over many iterations")
{
    const int runs = 5;
    const int iterations = 10000;

    // --- Baseline: N loop iterations, no yield ---
    std::printf("\n=== %d iterations WITHOUT yield ===\n", iterations);
    long long baseline_ns[5];
    for (int run = 0; run < runs; ++run) {
        dripcore::scheduler executor;
        std::atomic<bool> task_done{false};

        class no_yield_task : public dripcore::task
        {
        public:
            no_yield_task(std::atomic<bool>& done, int n) : done_(done), n_(n) {}
            dripcore::awaitable<> body() override {
                for (int i = 0; i < n_; ++i) {
                    // busy loop - no suspend
                }
                done_ = true;
                co_return;
            }
        private:
            std::atomic<bool>& done_;
            int n_;
        };

        auto start = std::chrono::high_resolution_clock::now();
        executor.spawn<no_yield_task>(task_done, iterations);
        executor.run();
        auto end = std::chrono::high_resolution_clock::now();

        baseline_ns[run] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        std::printf("  Run %d: %lld ns (%.3f ms)\n", run + 1,
                     (long long)baseline_ns[run], baseline_ns[run] / 1e6);
        CHECK(task_done == true);
    }

    // --- With yield: N loop iterations, each with co_await yield() ---
    std::printf("\n=== %d iterations WITH yield ===\n", iterations);
    long long yield_ns[5];
    for (int run = 0; run < runs; ++run) {
        dripcore::scheduler executor;
        std::atomic<bool> task_done{false};

        class yield_task : public dripcore::task
        {
        public:
            yield_task(std::atomic<bool>& done, int n) : done_(done), n_(n) {}
            dripcore::awaitable<> body() override {
                for (int i = 0; i < n_; ++i) {
                    co_await yield();
                }
                done_ = true;
                co_return;
            }
        private:
            std::atomic<bool>& done_;
            int n_;
        };

        auto start = std::chrono::high_resolution_clock::now();
        executor.spawn<yield_task>(task_done, iterations);
        executor.run();
        auto end = std::chrono::high_resolution_clock::now();

        yield_ns[run] = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        std::printf("  Run %d: %lld ns (%.3f ms)\n", run + 1,
                     (long long)yield_ns[run], yield_ns[run] / 1e6);
        CHECK(task_done == true);
    }

    // --- Summary: per-yield cost ---
    // Use the medians to reduce outlier sensitivity.
    std::printf("\n=== Per-yield cost (median of %d runs, %d iterations each) ===\n",
                runs, iterations);

    auto median = [](long long* arr, int n) -> long long {
        std::sort(arr, arr + n);
        return arr[n / 2];
    };

    long long med_baseline = median(baseline_ns, runs);
    long long med_yield = median(yield_ns, runs);
    long long diff_ns = med_yield - med_baseline;
    double per_yield_ns = static_cast<double>(diff_ns) / iterations;

    std::printf("  Baseline median: %lld ns (%.3f ms)\n",
                med_baseline, med_baseline / 1e6);
    std::printf("  Yield median:    %lld ns (%.3f ms)\n",
                med_yield, med_yield / 1e6);
    std::printf("  Difference:      %lld ns (%.3f ms)\n",
                diff_ns, diff_ns / 1e6);
    std::printf("  Per-yield cost:   %.1f ns (%.3f us)\n",
                per_yield_ns, per_yield_ns / 1e3);
}
