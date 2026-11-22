#include <dripcore/blocking_pool.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace dripcore
{
  struct blocking_pool::impl
  {
    std::mutex mtx;
    std::condition_variable cv;
    std::deque<std::unique_ptr<job_base>> queue;
    bool stopping = false;
    std::vector<std::thread> threads;
  };

  blocking_pool::blocking_pool(unsigned threads)
    : impl_(std::make_shared<impl>())
  {
    for (unsigned i = 0; i < threads; ++i)
    {
      // Workers hold the shared_ptr state block, so a detached straggler
      // never touches a freed pool (see the class comment in the header).
      impl_->threads.emplace_back([state = impl_] {
        while (true)
        {
          std::unique_ptr<job_base> job;
          {
            std::unique_lock<std::mutex> lock(state->mtx);
            state->cv.wait(lock, [&state] { return state->stopping || !state->queue.empty(); });
            if (state->stopping)
            {
              // Discard queued jobs: the shutdown protocol guarantees
              // their awaiter states are already cancelled, so nobody
              // is waiting on them.
              return;
            }
            job = std::move(state->queue.front());
            state->queue.pop_front();
          }
          job->run();
        }
      });
    }
  }

  blocking_pool::~blocking_pool()
  {
    {
      std::lock_guard<std::mutex> lock(impl_->mtx);
      impl_->stopping = true;
    }
    impl_->cv.notify_all();

    // Detach rather than join: a worker stuck inside a blocking syscall
    // must not delay executor destruction.
    for (auto& t : impl_->threads)
    {
      t.detach();
    }
  }

  void blocking_pool::post_erased(std::unique_ptr<job_base> job)
  {
    {
      std::lock_guard<std::mutex> lock(impl_->mtx);
      if (impl_->stopping)
      {
        // Pool is shutting down; the job is discarded. Its awaiter
        // state is cancelled by the same shutdown path, so nobody is
        // waiting on the result.
        return;
      }
      impl_->queue.push_back(std::move(job));
    }
    impl_->cv.notify_one();
  }
}
