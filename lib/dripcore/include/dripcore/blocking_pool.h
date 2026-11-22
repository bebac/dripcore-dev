#ifndef DRIPCORE_BLOCKING_POOL_H
#define DRIPCORE_BLOCKING_POOL_H

#include <memory>
#include <utility>

namespace dripcore
{
  // A small pool of OS threads that runs blocking callables off the
  // executor thread. Used by blocking_awaiter (task::run_blocking()); see
  // doc/blocking_offload_proposal.md.
  //
  // Lifetime contract: the executor owns the pool lazily and destroys it
  // in its own destructor. Destruction stops accepting work and DETACHES
  // workers instead of joining them: a worker stuck inside a blocking
  // syscall must never delay executor destruction. This is safe because
  // the shutdown protocol guarantees every blocking_awaiter shared state
  // has already been cancelled (or completed) by the time the executor
  // is destroyed, so a detached straggler merely discards its result and
  // exits. The internal state is shared_ptr-owned so detached workers
  // never touch a freed pool object.
  class blocking_pool
  {
  public:
    explicit blocking_pool(unsigned threads = 2);

    // Stop accepting work; detach workers (see the class comment).
    ~blocking_pool();

    blocking_pool(const blocking_pool&) = delete;
    blocking_pool& operator=(const blocking_pool&) = delete;

    // Post a blocking job. F must be invocable with no arguments and may
    // be move-only. Thread-safe.
    template <typename F>
    void post(F&& f)
    {
      post_erased(std::make_unique<job_impl<std::decay_t<F>>>(std::forward<F>(f)));
    }

  private:
    // Type-erased move-only job (std::function requires copyability).
    struct job_base
    {
      virtual ~job_base() = default;
      virtual void run() = 0;
    };

    template <typename F>
    struct job_impl : job_base
    {
      explicit job_impl(F f) : f_(std::move(f)) {}
      void run() override { f_(); }
      F f_;
    };

    struct impl;

    void post_erased(std::unique_ptr<job_base> job);

    std::shared_ptr<impl> impl_;
  };
}

#endif // DRIPCORE_BLOCKING_POOL_H
