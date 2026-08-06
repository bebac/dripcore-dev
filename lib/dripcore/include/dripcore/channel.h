#ifndef DRIPCORE_CHANNEL_H
#define DRIPCORE_CHANNEL_H

#include <dripcore/executor.h>
#include <dripcore/task.h>
#include <dripcore/task_control_block.h>
#include <dripcore/cancelled.h>

#include <coroutine>
#include <deque>
#include <optional>
#include <cstddef>

namespace dripcore
{
  template <typename T>
  class channel
  {
  public:
    explicit channel(size_t capacity)
      : capacity_(capacity)
    {}

    // Non-copyable, non-movable (shared between producer and consumer)
    channel(const channel&) = delete;
    channel& operator=(const channel&) = delete;

    // --- Producer side (non-blocking, safe to call from callbacks) ---

    bool try_send(T value)
    {
      if (closed_) {
        return false;
      }

      if (buffer_.size() >= capacity_) {
        return false;
      }

      buffer_.push_back(std::move(value));

      // Wake the consumer if it's suspended on recv
      if (waiting_receiver_) {
        auto h = waiting_receiver_;
        waiting_receiver_ = nullptr;
        executor::current()->ready_task(h);
      }

      return true;
    }

    // --- Consumer side (coroutine-friendly) ---

    auto recv(dripcore::task* task)
    {
      return recv_awaiter{this, task->control_block_->executor_, task->control_block_.get()};
    }

    // --- Lifecycle ---

    void close()
    {
      closed_ = true;

      // Wake the consumer so it sees the close
      if (waiting_receiver_) {
        auto h = waiting_receiver_;
        waiting_receiver_ = nullptr;
        executor::current()->ready_task(h);
      }
    }

    bool is_closed() const { return closed_; }
    size_t size() const { return buffer_.size(); }
    bool empty() const { return buffer_.empty(); }

  private:
    struct recv_awaiter
    {
      channel* ch_;
      executor* exec_ptr_;
      task_control_block* control_block_;
      std::coroutine_handle<> handle_ = nullptr;

      bool await_ready()
      {
        // Don't suspend if there's data or the channel is closed
        return !ch_->buffer_.empty() || ch_->closed_;
      }

      void await_suspend(std::coroutine_handle<> h)
      {
        handle_ = h;
        ch_->waiting_receiver_ = h;

        if (control_block_) {
          control_block_->handle_ = h;
          control_block_->register_awaiter(&cancel_recv_awaiter, this);
        }
      }

      std::optional<T> await_resume()
      {
        if (control_block_) {
          control_block_->clear_awaiter();
        }
        if (control_block_ && control_block_->cancelled_) {
          throw cancelled{};
        }
        if (ch_->buffer_.empty()) {
          return std::nullopt;  // Channel closed, no more data
        }

        T value = std::move(ch_->buffer_.front());
        ch_->buffer_.pop_front();
        return value;
      }

      ~recv_awaiter()
      {
        // If we're being destroyed while still suspended, clear the dangling
        // reference in the channel to prevent use-after-free when try_send/close
        // tries to resume the coroutine.
        if (ch_ && ch_->waiting_receiver_ == handle_) {
          ch_->waiting_receiver_ = nullptr;
        }
      }

      static void cancel_recv_awaiter(void* ptr)
      {
        auto* recv = static_cast<recv_awaiter*>(ptr);
        if (recv->ch_ && recv->ch_->waiting_receiver_ == recv->handle_) {
          recv->ch_->waiting_receiver_ = nullptr;
          if (recv->exec_ptr_ && recv->control_block_) {
            recv->exec_ptr_->ready_task(recv->control_block_);
          }
        }
      }
    };

  private:

  private:
    size_t capacity_;
    std::deque<T> buffer_;
    bool closed_ = false;
    std::coroutine_handle<> waiting_receiver_ = nullptr;
  };
}

#endif // DRIPCORE_CHANNEL_H
