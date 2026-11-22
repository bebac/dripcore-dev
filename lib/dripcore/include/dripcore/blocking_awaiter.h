#ifndef DRIPCORE_BLOCKING_AWAITER_H
#define DRIPCORE_BLOCKING_AWAITER_H

#include <dripcore/blocking_pool.h>
#include <dripcore/cancelled.h>
#include <dripcore/executor.h>
#include <dripcore/task_control_block.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace dripcore
{
  // Single-resume claim: exactly one of {worker completion, cancellation,
  // deadline expiry} wins the transition out of 'pending'. A party that
  // loses the claim must not touch the awaiting coroutine's handle.
  // See doc/blocking_offload_proposal.md.
  enum class blocking_step
  {
    pending,
    completed,
    cancelled
  };

  // State shared between the awaiting coroutine (executor thread) and the
  // blocking-pool worker thread. Heap-allocated and refcounted so it
  // outlives whichever party touches it last.
  struct blocking_state_base
  {
    std::atomic<blocking_step> step{blocking_step::pending};
    std::coroutine_handle<> waiter;  // valid while step == pending
    executor* exec = nullptr;        // where to resume the waiter
    std::exception_ptr error;
  };

  template <typename T>
  struct blocking_state : blocking_state_base
  {
    std::optional<T> result;
  };

  template <>
  struct blocking_state<void> : blocking_state_base
  {
  };

  // Non-template base: the executor's pending-blocking registry and the
  // deadline expiry hook are typed on this.
  struct blocking_awaiter_base
  {
    blocking_awaiter_base* prev_ = nullptr;
    blocking_awaiter_base* next_ = nullptr;
    bool linked_ = false;

    virtual ~blocking_awaiter_base() = default;

    // Mark the awaiting task cancelled and, if the claim on the shared
    // state wins, resume its frame (it throws dripcore::cancelled from
    // await_resume). Called on the executor thread from
    // task_control::cancel(), executor::cancel_pending(), and the
    // deadline expiry hook. Does not unlink from the executor registry;
    // callers that walk the registry unlink afterwards.
    virtual void cancel() = 0;
  };

  // Awaiter returned by task::run_blocking(). Mirrors io_awaiter /
  // sleep_awaiter: it registers a type-erased cancel function with the
  // task's control block and links into an executor-owned registry so
  // both per-task cancellation and the shutdown protocol can reach it
  // while it is suspended.
  //
  // F must be invocable with no arguments and return by value; it may be
  // move-only. A blocking call is never interrupted: cancellation and
  // deadlines abandon the worker job, which runs to completion and
  // discards its result.
  template <typename F>
  class blocking_awaiter final : public blocking_awaiter_base
  {
  public:
    using result_type = std::invoke_result_t<F&>;

    static_assert(!std::is_reference_v<result_type>,
                  "run_blocking callables must return by value");

    blocking_awaiter(executor* exec, task_control_block* cb, F f)
      : exec_ptr_(exec), control_block_(cb), f_(std::move(f)),
        state_(std::make_shared<blocking_state<result_type>>())
    {
    }

    blocking_awaiter(executor* exec, task_control_block* cb, F f,
                     std::chrono::steady_clock::duration timeout)
      : blocking_awaiter(exec, cb, std::move(f))
    {
      deadline_ = std::chrono::steady_clock::now() + timeout;
      has_deadline_ = true;
    }

    // Do not suspend if the task was already cancelled.
    bool await_ready() const noexcept
    {
      return control_block_ && control_block_->cancelled_.load(std::memory_order_acquire);
    }

    void await_suspend(std::coroutine_handle<> h)
    {
      state_->waiter = h;
      state_->exec = exec_ptr_;

      if (control_block_)
      {
        control_block_->handle_ = h;
        control_block_->register_awaiter(&cancel_trampoline, this);
      }

      exec_ptr_->register_blocking(this);

      if (has_deadline_)
      {
        exec_ptr_->register_deadline(deadline_, &expire_trampoline, this);
      }

      // Post the job only if the awaitable has not already been
      // cancelled. A cancel racing this check is benign: the worker's
      // claim then sees 'cancelled' and discards the result.
      if (state_->step.load(std::memory_order_acquire) == blocking_step::pending)
      {
        exec_ptr_->get_blocking_pool().post(make_job());
      }
    }

    result_type await_resume()
    {
      if (control_block_)
      {
        control_block_->clear_awaiter();
      }
      if (exec_ptr_)
      {
        exec_ptr_->deregister_blocking(this);
        exec_ptr_->deregister_deadline(this);
      }

      // step is the single source of truth for the outcome. If the
      // worker won the claim, deliver the result even if a racing
      // cancel() also set control_block_->cancelled_ — the task then
      // throws dripcore::cancelled at its next suspension point.
      if (state_->step.load(std::memory_order_acquire) != blocking_step::completed)
      {
        throw cancelled{};
      }
      if (state_->error)
      {
        std::rethrow_exception(state_->error);
      }
      if constexpr (!std::is_void_v<result_type>)
      {
        return std::move(*state_->result);
      }
    }

    ~blocking_awaiter()
    {
      // The co_await finished, or the frame is being force-destroyed.
      // Claim 'cancelled' so a racing worker never resumes a dead
      // handle. Never resumes.
      blocking_step expected = blocking_step::pending;
      state_->step.compare_exchange_strong(expected, blocking_step::cancelled,
                                           std::memory_order_acq_rel);
      if (exec_ptr_)
      {
        exec_ptr_->deregister_blocking(this);
        exec_ptr_->deregister_deadline(this);
      }
      if (control_block_)
      {
        control_block_->clear_awaiter();
      }
    }

    void cancel() override
    {
      if (control_block_)
      {
        control_block_->cancelled_.store(true, std::memory_order_release);
      }
      if (claim())
      {
        // The worker lost (or never made) its claim; the frame is still
        // suspended on this awaiter. Resume it on the executor thread.
        if (exec_ptr_ && control_block_)
        {
          exec_ptr_->ready_task(control_block_);
        }
      }
    }

  private:
    static void cancel_trampoline(void* ptr)
    {
      static_cast<blocking_awaiter*>(ptr)->cancel();
    }

    // Deadline expiry has the same semantics as cancellation: claim the
    // state; if the claim wins, the frame resumes and throws
    // dripcore::cancelled. The worker keeps running and discards.
    static void expire_trampoline(void* ptr)
    {
      static_cast<blocking_awaiter*>(ptr)->cancel();
    }

    // Transition pending -> cancelled; true if this caller won.
    bool claim()
    {
      blocking_step expected = blocking_step::pending;
      return state_->step.compare_exchange_strong(expected, blocking_step::cancelled,
                                                   std::memory_order_acq_rel);
    }

    auto make_job()
    {
      return [st = state_, f = std::move(f_)]() mutable
      {
        try
        {
          if constexpr (std::is_void_v<result_type>)
          {
            f();
          }
          else
          {
            st->result.emplace(f());
          }
        }
        catch (...)
        {
          st->error = std::current_exception();
        }
        // Publish the result, then claim. The release half of the
        // compare_exchange makes the result visible to the executor
        // thread that acquires 'completed'; resume_external()'s mutex
        // adds a further full barrier on the resume path.
        blocking_step expected = blocking_step::pending;
        if (st->step.compare_exchange_strong(expected, blocking_step::completed,
                                              std::memory_order_acq_rel))
        {
          st->exec->resume_external(st->waiter);
        }
      };
    }

    executor* exec_ptr_;
    task_control_block* control_block_;
    F f_;
    std::shared_ptr<blocking_state<result_type>> state_;
    std::chrono::steady_clock::time_point deadline_{};
    bool has_deadline_ = false;
  };
}

#endif // DRIPCORE_BLOCKING_AWAITER_H
