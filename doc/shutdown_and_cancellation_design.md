---
title: Shutdown and Task Cancellation Design
tags: [dripcore, c++, coroutines, cancellation, shutdown, multithreading, design]
---

# Shutdown and Task Cancellation Design

## Summary

DripCore currently has no mechanism to stop a running task, cancel a pending
I/O operation, or shut down the scheduler while tasks are still suspended. The
`run()` loop only terminates when every task has completed on its own. This
document describes the current situation, explains why the naive fixes are
unsafe (particularly on kqueue), and proposes a cancellation and shutdown
design that is **built for the future multithreaded architecture first**
(`doc/multithreaded_scheduler_proposal.md`), then evaluated for the current
single-executor implementation.

This is a design proposal, not an implementation plan. It exists to reach
agreement on the model before any code is written.

**Status:** Draft — for discussion.

## Problem Statement

There is no reliable way to make a suspended task stop.

Concretely, the following all fail today:

1. **Stopping a task from the outside.** `executor::spawn<T>()` returns
   `void` (`executor.h:29`). The caller never receives a handle, so there is no
   object on which to request cancellation.
2. **Cancelling a pending I/O wait.** A task suspended on
   `co_await readable(fd)` registers a coroutine handle with kqueue/epoll and
   nothing else records it. Closing the fd locally does **not** wake the
   waiter on kqueue — the kernel silently removes the knote (see below). The
   coroutine is never resumed.
3. **Shutting down the scheduler.** `executor::run()` loops
   `while (!tasks_.empty())` (`executor.cpp:120`) and only reaps via
   `tasks_.remove_if(done())`. A task blocked forever on I/O keeps the loop
   alive indefinitely. There is no `shutdown()` on `scheduler`
   (`scheduler.h`).

Two properties of the current code make ad-hoc fixes dangerous:

- **Frame destruction while suspended is a use-after-free.**
  `task_handle::~task_handle()` calls `handle_.destroy()`
  (`task_handle.cpp:16-22`). If a frame is destroyed while a kqueue knote still
  references it via `udata`, a later event delivery resumes freed memory. The
  `io_awaiter` lives *inside* that frame and has no destructor to deregister
  (`io_awaiter.h`).
- **An escaping exception in a top-level task terminates the process.**
  `task_handle::promise_type::unhandled_exception()` calls `std::terminate()`
  (`task_handle.h:36`). This is *only* the top-level task promise:
  `awaitable<T>` already captures exceptions in `exception_` and rethrows them
  at `await_resume()` (`awaitable.h:72-75, 133-135`), so cancellation exceptions
  already compose through awaitable chains and only need a landing site at the
  task boundary. That boundary must stop terminating.

## Background: why local `close()` does not wake kqueue

From `kqueue(2)`:

> Calling `close()` on a file descriptor will remove any kevents that reference
> the descriptor.

The knote is **deleted, not triggered**. The coroutine handle stashed in
`udata` (`event_loop_kqueue.cpp:28`) is dropped without a wakeup.

epoll is not meaningfully better: `close()` removes the fd from the interest
set without delivering anything, and the "close wakes `epoll_wait`" folklore
only applies when a duplicated descriptor keeps the description alive — and
even then you get no readiness event. **Relying on local close to wake a
waiter is not portable.** The correct pattern on both backends is: deregister
first, resume the coroutine with a cancellation signal, then close.

`shutdown(fd, SHUT_RDWR)` is a useful shortcut for *connected* sockets — it
makes `EVFILT_READ` fire with `EV_EOF`, so the reader wakes naturally — but it
does nothing for listening sockets, pipes, regular files, or the general
"stop this task" case. It is an optimization, not the mechanism.

## Taxonomy of "stop waiting"

These are distinct problems that happen to share an outcome (a suspended
coroutine is resumed). The design must address all of them, and only some are
covered by the kernel.

| Scenario | Trigger | Tracked by | Status today |
|---|---|---|---|
| I/O readiness | kernel | kqueue/epoll | works |
| I/O cancellation | user / executor | nothing | **broken** |
| Timer cancellation | user / executor | `timer_queue_` | frame destroy leaves dangling handle |
| Channel close | producer | `waiting_receiver_` | works (`channel.h:58`) |
| Task cancellation | supervisor / user | nothing | **no mechanism** |
| Scheduler shutdown | user | nothing | **no mechanism** |

The channel already demonstrates the correct shape: `close()` sets a flag and
wakes the waiter (`channel.h:58-68`). The I/O path needs the equivalent, and
the executor needs a way to drive it across *all* suspension kinds at once.

## Design Principles

1. **Design for multiple threads first.** The single-executor version is a
   special case (one executor, one thread). The protocol must not assume a
   single thread and then need reworking in Phase 3.
2. **Do not duplicate the kernel's bookkeeping.** kqueue/epoll already maintain
   the fd → waiter structure. We must not maintain a parallel map keyed by fd
   for the normal I/O path.
3. **A task is pinned to one executor (executor-affinity, not thread-affinity).**
   This matches the multithreaded proposal ("Tasks pinned to executor after
   first resume", "I/O operations always on same thread") and the
   executor-binding fix in
   `.mistral-cli/notes/dripcore/multithreading/awaitable_executor_binding.md`,
   which replaces `awaitable`'s current thread-affinity assertion with
   executor-affinity. All manipulation of a task's suspension state happens on
   its owning executor. Cross-executor requests are messages, never direct
   state access.
4. **Cancellation is cooperative-by-unwinding.** A cancelled task is resumed so
   that its awaiter throws a cancellation exception; the stack unwinds through
   normal C++ destructors (RAII), giving deterministic *synchronous* resource
   cleanup. Cleanup that must itself `co_await` is handled explicitly (§6).
5. **Deregister before resume before close.** Ordering is part of the
   contract, not an implementation detail.

## On the question of a userspace map

An earlier discussion resisted maintaining a map in the executor. That
instinct is correct for the **normal I/O path** and mostly correct overall:

- **fd I/O readiness**: use the kernel's structure. No userspace map.
- **fd I/O cancellation**: needs only the fd, which the awaiter already holds.
  `EV_DELETE` by `(fd, filter)` requires no lookup.
- **Timers**: already tracked in `timer_queue_`. Cancellation removes an entry;
  use an opaque token returned by `register_timer` rather than a new map.
- **Channels**: already track `waiting_receiver_`; `close()` already wakes it.

The one capability nothing above provides is **enumeration for shutdown**:
"which coroutines are suspended on anything right now?" The kernel cannot
answer this (kqueue knows fd waiters, not timer or channel waiters), and it
cannot be reconstructed from `executor::tasks_` alone — a `task_handle` can
report `done()` but not *what* it is suspended on, which is exactly the
information required to deregister safely before resuming.

The resolution is that this is a **list, not a map**. It is appended on suspend
and removed on resume, and it is only *walked* at shutdown or for targeted
cancellation — never on the readiness hot path. See "Suspension registry"
below.

## Proposed Design (Multithreaded-First)

### 1. Cancellation token in every awaiter

Every *leaf* awaiter that actually suspends on an executor primitive
(`io_awaiter`, `sleep_awaiter`, channel `recv_awaiter`) gains:

- a back-pointer to its owning `executor`,
- the coroutine `handle_` captured in `await_suspend`,
- a cancellation flag, and
- an intrusive link for the suspension registry.

`await_resume()` inspects the flag and throws `dripcore::cancelled` when set,
instead of returning a readiness value. Because unwinding runs destructors,
each awaiter's destructor performs its own kind-specific deregistration:

```cpp
// io_awaiter (sketch)
struct io_awaiter {
    int fd_;
    short events_;
    executor* exec_;
    std::coroutine_handle<> handle_;
    bool cancelled_ = false;
    // intrusive registry links
    io_awaiter* prev_ = nullptr;
    io_awaiter* next_ = nullptr;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {
        handle_ = h;
        exec_->register_io(this);      // arms kqueue/epoll + adds to registry
    }

    short await_resume() {
        if (cancelled_) throw cancelled{};
        return events_;
    }

    ~io_awaiter() {
        if (exec_) exec_->unregister_io(this);  // EV_DELETE if still armed
    }
};
```

No fd-keyed map: `register_io`/`unregister_io` take the awaiter pointer, and
the fd needed for `EV_DELETE` is a field on the awaiter.

### 2. Suspension registry (a list, not a map)

Each executor owns an intrusive list of currently-suspended awaiters. Cost on
the hot path is a pointer append (suspend) and unlink (resume). It exists so
the executor can enumerate and cancel everything at shutdown. Because it is
per-executor and only touched by the owning thread, it needs no locking.

The `cancelled_` flag also closes the **double-resume race**: if a task is
cancelled and simultaneously its fd becomes ready, the first path to observe
the flag wins and the other is suppressed. In `poll()`, the event delivery
path must check-and-clear the flag before resuming, and must skip resumption
for an awaiter already cancelled.

### 3. `task_control` — the external handle

`spawn` returns a lightweight, copyable, thread-safe control handle
(distinct from the move-only `task_handle`, which owns the frame):

```cpp
class task_control {
public:
    void cancel();          // request cooperative cancellation
    bool done() const;      // completed or cancelled
private:
    std::shared_ptr<task_control_block> cb_;  // { executor*, coroutine_handle, state }
};
```

`cancel()` does **not** touch task state directly. It posts a cancellation
request to the owning executor:

- **Same thread:** enqueue locally and proceed.
- **Different thread:** push onto that executor's MPSC submission queue (the
  same queue the multithreaded proposal already introduces,
  `multithreaded_scheduler_proposal.md:238-239`) and wake it via the existing
  `trigger_wake_notification()` (`event_loop.h:31`).

The owning executor, on its own thread, then:

1. marks the awaiter `cancelled_`,
2. deregisters the kernel/timer/channel resource (via the awaiter or its
   subsystem),
3. schedules the coroutine on its ready queue.

When resumed, `await_resume()` throws `cancelled`, the stack unwinds, socket
and other RAII destructors run, and the frame reaches `final_suspend`.

### 4. Making exceptions survivable at the task boundary

`task_handle::promise_type::unhandled_exception()` must stop calling
`std::terminate()` (`task_handle.h:36`). Intermediate `awaitable<T>` frames
already do the right thing — they capture the exception and rethrow it at
`await_resume()` (`awaitable.h:72-75`) — so the only missing landing site is the
top-level task. It should treat `dripcore::cancelled` as normal termination
(mark the control block cancelled) and record any other exception in the control
block for a supervisor to observe (see `task_control`, §3) rather than aborting
the process. This is a prerequisite for the whole scheme and the single riskiest
change to existing behavior.

### 5. Shutdown protocol

Two phases, mirroring the proposal's `run()` / `shutdown()` split
(`multithreaded_scheduler_proposal.md:191-192`):

**Graceful.** `scheduler::shutdown()` sets the atomic `shutdown_requested_`
flag, wakes every executor, and injects `cancelled` into every task (via the
suspension registry, deregister → mark → schedule). Executors stop accepting
new external work but **keep pumping the run loop**, because a task's cleanup
may itself be asynchronous (§6) and needs the loop to make progress. The "wait
for cleanup" is therefore not a special step — it is the existing `run()` drain
(`executor.cpp:120`) continuing until every task reaches `done()`. A deadline
bounds this drain.

**Forced (deadline expiry).** For any task that has not finished draining, the
executor deregisters its leaf and **destroys the frame** (`handle_.destroy()`).
Frame destruction runs in-scope local destructors — so synchronous RAII cleanup
(e.g. `socket::~socket()` closing the fd) still happens — but does **not**
resume the coroutine, so `catch`-based *asynchronous* cleanup is skipped. Forced
shutdown is thus a downgrade from "sync + async cleanup" to "sync cleanup only."
Once all frames are destroyed and ready queues drain, `run()` returns.

In the multithreaded case the scheduler fans the shutdown signal out to all
executors and joins their threads once each reports drained. No executor
reaches into another's registry; each cancels and, on expiry, force-destroys
its own tasks in response to the broadcast.

### 6. Cancellation cleanup: synchronous vs. asynchronous

Cleanup during cancellation splits in two, and the split is forced by a C++
rule: **you cannot `co_await` in a destructor.**

- **Synchronous cleanup** runs in destructors during unwind (e.g.
  `socket::~socket()` calling `close(fd)`). It is automatic the moment
  `cancelled` propagates, and it runs in *both* the graceful and forced paths.
- **Asynchronous cleanup** needs to `co_await` (flush with backpressure, send a
  goodbye frame, TLS close-notify). It cannot live in a destructor. It is
  possible here *only because* cancellation is resume-then-throw rather than
  frame-destruction: the coroutine is alive and running its own body with an
  exception in flight, so it may catch, await, and rethrow:

```cpp
try {
    co_await readable(fd);
    // ...
} catch (const dripcore::cancelled&) {
    co_await flush_and_goodbye();   // legal: coroutine body, not a destructor
    throw;                          // must rethrow to complete cancellation
}
```

This is why cancellation is a *catchable* exception (see Decisions): catching
is not for swallowing but for async cleanup, under the rule "you may catch to
clean up, but you must rethrow." Because async cleanup re-suspends on executor
primitives, only the **graceful** phase can run it — it needs the loop to keep
pumping until the task reaches `done()`. The **forced** phase (frame destroy)
runs synchronous cleanup only. The shutdown deadline is precisely the budget
for async cleanup before that downgrade.

### 7. Interaction with `awaitable<T>` and external awaitables

`awaitable<T>` already exists (`awaitable.h`) and already stores an
`exception_ptr` per promise, rethrowing at `await_resume()`. So if a task is
cancelled while awaiting one, the `cancelled` exception thrown by the leaf
awaiter unwinds through the awaiting coroutine and through each intervening
`awaitable<T>` naturally — no new mechanism is required for *propagation*.

**Cancelling a parked task propagates *downward*.** When task `T` does
`co_await child`, `T` is not on a leaf of its own — it is parked in `child`'s
`continuation_` (a single `std::coroutine_handle<>` in the promise); the
entry in the executor's suspension registry belongs to `child`'s leaf
awaiter. So cancelling `T` means cancelling whatever `T` is currently
awaiting, recursively, down to the leaf. The leaf throws `cancelled`;
`child` unwinds and its `final_suspend` schedules its continuation (`T`)
via `ready_task`; `T` resumes and `child.await_resume()` rethrows the stored
`exception_`, so `T` sees `cancelled` too. Propagation is therefore free
via the existing
`exception_ptr` machinery — the only new state required is a single per-task
"await edge" (one pointer, not a map) recording the one thing each suspended
task is currently awaiting, so the executor can walk `T → child → leaf`.

**Two required fixes fall out of this:**

- `awaitable<T>` now has a **destructor** that destroys `handle_`, owning
  the child frame. The `final_suspend` is a custom `final_awaiter` that
  schedules the single continuation on the ready queue (not `suspend_always`).
  Unwinding `T` deterministically tears down the child via `~awaitable`.
  This destructor and the downward propagation above are hard prerequisites
  for the `task` base-class wrapper (§9), not merely a leak fix.
- Cancelling a task therefore **must** cancel the awaitable it is awaiting;
  there is no coherent alternative, since a live `child` would hold a
  continuation into a frame being unwound.

**External / third-party awaitables.** Tasks are not restricted to
`dripcore::awaitable`; a task may `co_await` any type satisfying the awaiter
interface. Cancellation reaches such a type only through the *leaf* it
ultimately suspends on:

- If an external awaitable is built *on top of* dripcore primitives (it
  eventually `co_await`s `readable`/`writable`/`recv`/an `awaitable<T>`),
  cancelling the task throws at that leaf and the external wrapper unwinds like
  any other frame. It participates for free.
- If an external awaitable suspends on a mechanism dripcore does not own (its
  own OS handle, a foreign event loop, a raw `std::coroutine_handle` parked
  where the executor cannot see it), the executor cannot wake or deregister it.
  Such a task can be *marked* cancelled but will not observe it until it next
  suspends on a dripcore primitive. This is an inherent boundary, not a fixable
  gap — cancellation is only as reliable as the leaf awaiter's cooperation — and
  should be documented as a constraint on integrating foreign awaitables.

### 8. Service stop semantics

The proposal's `service::on_stop()` example closes the listening socket directly
(`multithreaded_scheduler_proposal.md:414-417`) — which is the exact kqueue bug
this document opens with: the service loop suspended on
`co_await readable(listener_)` never wakes when the fd is closed. The service
abstraction must change to **stop by cancellation, not by close**:

- The supervisor holds each service's `task_control` and stops it via
  `cancel()`. `cancelled` is thrown into the loop, which unwinds; the socket's
  destructor releases the listener via RAII.
- `on_stop()` is repurposed from "manually close resources" (racy, broken) to an
  optional *async cleanup* hook — either a `catch` block in the service loop
  (§6) or a coroutine the supervisor `co_await`s after the main task has been
  cancelled and drained. Resource release moves to RAII-on-unwind.
- `restart_policy` reads the `exception_ptr` recorded in the control block to
  decide whether to restart.

### 9. Task base-class integration

The async-cleanup pattern (§6) is error-prone to hand-write in every task, and
C++ has no `co_await`-able `finally`. The `task` base class therefore owns it.
`execute()` becomes a fixed, non-virtual base-class coroutine (still the entry
point the executor spawns, so `spawn` is unchanged, `executor.h:41`) that
delegates to two virtual members:

```cpp
class task {
public:
    task_handle execute() {                 // fixed; executor spawns this
        std::exception_ptr ep;
        try {
            co_await body();                // virtual → derived logic
        } catch (...) {
            ep = std::current_exception();
        }
        co_await teardown();                // always runs; may co_await
        if (ep) std::rethrow_exception(ep); // includes dripcore::cancelled
    }
protected:
    virtual awaitable<> body() = 0;               // task's main logic
    virtual awaitable<> teardown() { co_return; } // always-run cleanup, default no-op
    // existing helpers (readable/writable/yield/sleep_for/spawn) stay here
};
```

**Semantics:**

- `teardown()` runs on *every* exit path — normal completion, error, and
  cancellation — so it is the async equivalent of `finally`. It must be cheap
  when there is nothing to do (the default is a no-op).
- Cancellation reaches the `catch` via the same downward propagation as §7:
  `execute()` is parked as a continuation of `body()`, so when `body()`'s leaf
  throws `cancelled` it unwinds to `body()`'s `final_suspend`, resumes
  `execute()`, and `body().await_resume()` rethrows. This makes the
  `awaitable<T>` destructor / propagation work of §7 a hard prerequisite for the
  base-class integration, not merely a leak fix.
- The rethrow at the end carries `cancelled` (or a real error) to the task
  promise, which per §4 records it in the `task_control` block instead of
  terminating.

**This is an interface break.** Tasks no longer write a monolithic `execute()`
returning `task_handle`; they override `body()` (returning `awaitable<>`) and
optionally `teardown()`. This unifies tasks with the continuations model
(`awaitable.h`) — every task becomes an awaitable-coroutine composition.

**Forced shutdown still skips `teardown()`.** On deadline expiry the frame is
destroyed (§5); that runs synchronous destructors inside `body()` and
`execute()` but does not resume `execute()`, so `teardown()` does not run.
`teardown()` is therefore a *graceful-path* guarantee bounded by the shutdown
deadline, not an unconditional one.

## Applicability to the current single executor

Everything above degrades cleanly to one executor / one thread:

- The MPSC path collapses to a local enqueue; `cancel()` from within a task is
  always same-thread.
- The suspension registry, cancellation flag, awaiter destructors, and the
  `unhandled_exception` change are all thread-agnostic and are the parts worth
  implementing now, because they fix the present bugs (kqueue close, frame
  destroy UAF, unstoppable `run()`).
- `scheduler::shutdown()` can be implemented immediately; the graceful drain is
  just the existing run-loop with a deadline, and the forced phase is
  frame-destroy on expiry.

Recommended near-term subset (single executor), assuming agreement on the
model:

1. Stop `unhandled_exception()` from terminating; define `dripcore::cancelled`.
2. Add executor back-pointer, `handle_`, `cancelled_`, and destructor
   deregistration to `io_awaiter` (and `sleep_awaiter`).
3. Add the per-executor suspension registry and check-and-clear in `poll()`.
4. Return `task_control` from `spawn`; implement same-thread `cancel()`.
5. Add `scheduler::shutdown()` with both phases (graceful drain + forced
   frame-destroy on deadline) and make `run()` exit on it.
6. Give `awaitable<T>` a destructor that owns/destroys its child frame (fixes
   the current leak, §7).
7. Convert `task` so `execute()` is a base-class wrapper delegating to virtual
   `body()`/`teardown()`; migrate existing tasks (§9).

Deferred to Phase 3: MPSC-based cross-thread `cancel()`, shutdown fan-out and
thread joining, downward cancellation via the per-task await edge, and the
`service`/`service_supervisor` stop protocol (§8).

## Decisions

These follow from the discussion and are treated as settled for this design:

- **Cancellation is a plain exception; swallowing is not prevented.**
  `dripcore::cancelled` is an ordinary exception type. Enforcing "cannot be
  caught" (re-arming after a `catch`, checking at `final_suspend`, etc.) adds
  machinery for no concrete use case, so the simplest option wins: a task *can*
  technically catch it, but doing so is documented as unsupported.
- **Shutdown defaults to graceful with a timeout.** `scheduler::shutdown()`
  first requests graceful stop and lets tasks unwind at their next suspension
  point; if the timeout elapses it escalates to the forced phase (§5).
- **Non-cancellation exceptions surface through `task_control`.** The control
  block records the `exception_ptr`; this same handle is what a future
  `service_supervisor` holds to observe task termination and apply restart
  policy.
- **Async cleanup is required, and it is the graceful phase itself.** A task may
  run asynchronous cleanup by catching `cancelled`, `co_await`-ing, and
  rethrowing (§6). Graceful shutdown waits for it by pumping the run loop to
  `done()` under a deadline; the forced phase destroys frames and runs
  synchronous cleanup only (§5).
- **Cancellation propagates downward along a per-task await edge.** Cancelling a
  task cancels the awaitable it is currently awaiting, recursively to the leaf;
  the exception unwinds back up for free. `awaitable<T>` gains a destructor that
  owns the child frame (§7).
- **Services stop by cancellation, not by close.** The supervisor cancels a
  service's `task_control`; RAII releases resources on unwind; `on_stop()`
  becomes an optional async-cleanup hook (§8).
- **The `task` base class owns the cancel/cleanup wrapper.** `execute()` becomes
  a fixed base-class coroutine delegating to virtual `body()` and always-run
  `teardown()` (§9); async cleanup lives in `teardown()`, not hand-written per
  task.

## Note: task bodies must be coroutines (not "must suspend")

A task body does **not** need to suspend before it can throw safely.
`unhandled_exception()` captures any exception thrown from a task body —
including one thrown before the first body-level `co_await` — because
`initial_suspend()` is `suspend_always`: creation suspends before the body, so
the first `resume()` runs the body inside the coroutine's implicit try block,
and the throw routes to `unhandled_exception()`. This is verified by the
`"exception thrown before first co_await is captured"` unit test, which passes
with no executor-side try/catch.

The one real pitfall is different: a task `execute()` that contains **no** `co_*`
keyword (`co_await`/`co_return`/`co_yield`) is not a coroutine at all. It is an
ordinary function that throws at `spawn()` time, before any coroutine handle
exists, and therefore cannot be captured by the executor. Every task body must
contain at least one `co_*` keyword.

Corollary: if `initial_suspend()` were ever changed from `suspend_always` to
`suspend_never`, the body would begin executing during `spawn()`, and a
pre-suspend throw would likewise escape at `spawn()`. Keeping `suspend_always`
is what makes body exceptions uniformly catchable.

## Open Questions

1. **`teardown()` shielding and budget.** Should `teardown()` (§9) be shielded
   from the graceful deadline with its own budget, or may the forced phase
   interrupt it mid-`co_await`?
2. **Default timeout.** What concrete graceful deadline is sensible as a
   default, and should it be per-scheduler, per-service, or both?
3. **Fate of `on_stop()`.** Keep it as the async-cleanup hook, or drop it
   entirely in favor of `catch`-block cleanup in the service loop?

## Relationship to Existing Proposals

- **`multithreaded_scheduler_proposal.md`**: This design supplies the
  cancellation and shutdown protocol that proposal lists as an open question
  ("Cancellation: Should tasks be cancellable? How to propagate cancellation?",
  line 706) and depends on its planned MPSC submission queue and per-executor
  ownership model.
- **`awaitable.h` / `dripcore_continuations_proposal.md`**: `awaitable<T>` is
  implemented and propagates exceptions via a per-promise `exception_ptr`.
  It uses symmetric transfer with a single continuation for performance, and
  owns the child frame via `~awaitable`. This design adds downward cancellation
  propagation (§7). See `dripcore_continuations_proposal.md` §Design Evolution
  for the rationale behind the single-continuation design.
- **`.mistral-cli/notes/dripcore/multithreading/awaitable_executor_binding.md`**:
  the executor-affinity fix is a prerequisite for cross-thread `cancel()`, since
  it establishes the executor (not the OS thread) as the unit a task is bound
  to.
