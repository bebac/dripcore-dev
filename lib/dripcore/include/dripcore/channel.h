#ifndef DRIPCORE_CHANNEL_H
#define DRIPCORE_CHANNEL_H

#include <dripcore/executor.h>

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

    auto recv()
    {
      return recv_awaiter{this};
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

      bool await_ready()
      {
        // Don't suspend if there's data or the channel is closed
        return !ch_->buffer_.empty() || ch_->closed_;
      }

      void await_suspend(std::coroutine_handle<> h)
      {
        ch_->waiting_receiver_ = h;
      }

      std::optional<T> await_resume()
      {
        if (ch_->buffer_.empty()) {
          return std::nullopt;  // Channel closed, no more data
        }

        T value = std::move(ch_->buffer_.front());
        ch_->buffer_.pop_front();
        return value;
      }
    };

  private:
    size_t capacity_;
    std::deque<T> buffer_;
    bool closed_ = false;
    std::coroutine_handle<> waiting_receiver_ = nullptr;
  };
}

#endif // DRIPCORE_CHANNEL_H
