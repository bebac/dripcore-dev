#ifndef DRIPCORE_IO_AWAITER_H
#define DRIPCORE_IO_AWAITER_H

#include <coroutine>

namespace dripcore
{
  class executor;
  struct task_control_block;

  struct io_awaiter
  {
    int fd_;
    short events_;         // POLLIN | POLLOUT
    executor* exec_ptr_;
    task_control_block* control_block_;

    // Cancellation / registry state, set while the awaiter is suspended.
    // These have default member initializers so aggregate initialization
    // (io_awaiter{fd, events, exec, control_block}) continues to work.
    bool linked_ = false;                      // in executor's pending-io list?
    io_awaiter* prev_ = nullptr;               // intrusive list links,
    io_awaiter* next_ = nullptr;               // owned by the executor

    void cancel();

    // Return true if task was cancelled, false to suspend
    bool await_ready() const noexcept;

    // Called just before the coroutine is suspended. Captures the handle and
    // registers with the executor (arms the platform event loop + links into
    // the registry) and with the control block. Out-of-line: needs the
    // full executor definition.
    void await_suspend(std::coroutine_handle<> h);

    // Called when the coroutine is resumed. Throws dripcore::cancelled if the
    // wait was cancelled; otherwise returns the requested event mask.
    // Out-of-line: needs dripcore::cancelled.
    short await_resume();

    // Deregisters from the event loop if still armed, preventing a
    // use-after-free if the frame is destroyed while suspended.
    // Out-of-line: needs the full executor definition.
    ~io_awaiter();
  };
}

#endif // DRIPCORE_IO_AWAITER_H
