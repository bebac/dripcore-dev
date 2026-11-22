---
title: Dripcore Blocking Offload and Cross-Thread Resume Proposal
tags: [dripcore, c++, coroutines, async, threading, design, implementation]
---

# Dripcore Blocking Offload and Cross-Thread Resume Proposal

## Summary

This document proposes a **blocking offload** primitive for dripcore:
`co_await run_blocking(f)` runs a blocking callable `f` on a small pool of
OS threads and resumes the awaiting coroutine on its executor when `f`
completes. It is the direct analogue of Tokio's `spawn_blocking` and
libuv's threadpool (`uv_queue_work`), and it is the standard answer for
blocking syscalls that have no non-blocking form — `getaddrinfo` being the
canonical example.

The proposal also introduces the minimal cross-thread substrate it needs:
**`executor::resume_external(handle)`**, a thread-safe way to resume a
single coroutine handle on an executor's thread. This is a scoped-down
slice of the "Cross-Thread Submission" item from
`doc/multithreaded_scheduler_proposal.md` (Phase 3) — it requires no
multiple executors, no work stealing, and no cross-thread task or channel
semantics.

**Status: implemented.** `executor::resume_external()`, the executor-owned
`blocking_pool`, and the `task::run_blocking()` awaiter (including the
deadline overload) live in `lib/dripcore/`; tests are in
`test/unittest/blocking_test.cpp`. Deviations from the original sketch
are marked *(as implemented)* below.

## Problem Statement

A single-threaded coroutine scheduler is only as responsive as its least
cooperative task. Any task that calls a blocking syscall inline stalls
*every* task on the executor: I/O multiplexing, timers, UI rendering,
everything. Real offenders are syscalls with no non-blocking variant:

- `getaddrinfo` / `getnameinfo` — seconds on a slow or broken resolver;
  the only "truly async" alternative (c-ares) reimplements the resolver
  and loses `nsswitch.conf`, `/etc/hosts` overrides, mDNS,
  systemd-resolved, and SSSD.
- Certificate and key file loading, large synchronous reads.
- Any third-party library with a blocking-only API.

Concrete motivating case (mistral-cli): the HTTP coordinator's connect
path calls `getaddrinfo` inline on the dripcore thread. A bad resolver
freezes the entire agent — TUI, tool streaming, MCP servers — for the
duration of the lookup.

The only architecturally honest options are (a) reimplement each
blocking API asynchronously (losing OS resolver behavior, as c-ares
demonstrates), or (b) get the blocking call off the scheduler thread and
resume the coroutine when it finishes. Every major runtime chose (b):

| System | Blocking work | Notes |
|---|---|---|
| Tokio | `task::spawn_blocking` | dedicated blocking pool, `.await` resumes on the runtime |
| libuv / Node | `uv_queue_work`, `uv_getaddrinfo` | threadpool; `dns.lookup` uses it by default |
| Go | goroutines on OS threads | blocking is cheap because goroutines are not bound to a poller |
| asyncio | `loop.run_in_executor` | explicit offload for blocking calls |
| Seastar | alien threads / `smp` queues | thread-per-core with explicit cross-core submission |

Dripcore should provide (b) as a first-class primitive rather than
leaving each application to wire its own thread plus wake pipe.

## What Dripcore Already Has

The hard-to-build pieces already exist:

- **Cross-thread wake notification.** `event_loop` provides
  `setup_wake_notification()` / `trigger_wake_notification()`
  (eventfd on Linux, self-pipe on macOS) precisely so `poll()` can be
  interrupted from another thread. The worker→executor signal path is
  therefore already implemented and thread-safe.
- **Thread-local executor.** `executor::current()` gives the awaiting
  coroutine's executor without passing pointers through awaiters.
- **A cancellation protocol.** `dripcore::cancelled`,
  `task_control::cancel()`, `executor::cancel_pending()`, and the
  two-phase shutdown design (`doc/shutdown_and_cancellation_design.md`)
  already define how a suspended awaiter is cancelled and resumed.
- **A timer wheel.** `register_timer()` / `sleep_awaiter` provide the
  deadline machinery a timeout overload can reuse.

What is missing is small: `ready_task()` is a plain vector with no
thread-safe entry point, and there is no awaitable that pairs a worker
thread with a continuation.

## Proposed Design

### 1. Cross-thread resume: `executor::resume_external`

```cpp
class executor {
public:
  // Thread-safe: enqueue a handle for resumption on this executor's
  // thread. Wakes poll() via the existing wake-notification path.
  // Called from worker threads; must not touch any other executor
  // state.
  void resume_external(std::coroutine_handle<> h);

private:
  std::mutex external_mutex_;
  std::deque<std::coroutine_handle<>> external_queue_;
};
```

- `resume_external` locks, pushes the handle, unlocks, and calls
  `trigger_wake_notification()`.
- The `run()` loop drains `external_queue_` into `ready_queue_` at the
  top of each iteration (before `process_ready_tasks()`), so a woken
  `poll()` dispatches the resumed handles promptly.
- A mutex is acceptable here: this path fires once per completed
  blocking call, not per I/O event. A lock-free MPSC queue is a
  compatible future optimization.

This is deliberately the *only* new thread-safe surface. Callers hand
over a bare handle to resume — never task objects, channels, or other
executor state — so nothing else in the executor must become
thread-aware.

### 2. The blocking pool

```cpp
namespace dripcore {
  // Small pool of OS threads that run blocking callables.
  // Owned by scheduler; created lazily on first use.
  class blocking_pool {
  public:
    explicit blocking_pool(unsigned threads = 2);
    ~blocking_pool();  // see "Shutdown Semantics"

    // Thread-safe. F must be invocable with no arguments.
    template <typename F>
    void post(F&& f);
  };
}
```

- **Ownership** *(as implemented)*: the `executor` owns the pool
  (`unique_ptr`, created lazily by `executor::get_blocking_pool()`), so
  the awaitable reaches it via `executor::current()` with no scheduler
  wiring, and standalone executor users get the feature too.
- **Sizing:** 2–4 threads is plenty. Blocking work in a dripcore app is
  rare by construction; the pool exists so that concurrent blocking
  calls cannot starve each other, not to maximize throughput.
- **Queue:** condition variable + `std::deque` of move-only jobs. No
  sophistication warranted at this call frequency.

### 3. The `run_blocking` awaitable

```cpp
namespace dripcore {
  // (as implemented) task methods, matching sleep_for()/readable(),
  // which also need the task's control block for cancellation:
  class task {
  protected:
    // Runs f on the executor's blocking pool; resumes this task with
    // f's result, or rethrows f's exception. Cancellation and
    // deadlines abandon the worker job (see "Cancellation Semantics").
    template <typename F>
    auto run_blocking(F&& f);

    // Same, with a deadline enforced by the executor's timer wheel.
    // On expiry the task resumes with dripcore::cancelled.
    template <typename F, typename Rep, typename Period>
    auto run_blocking(F&& f, std::chrono::duration<Rep, Period> timeout);
  };
}
```

The awaiter and the worker job share a heap-allocated state block — the
same shape resolve20 (a standalone resolver library built on this
pattern) independently validated:

```cpp
enum class blocking_state { pending, completed, cancelled };

struct blocking_state_base {
  std::atomic<blocking_state> state{blocking_state::pending};
  std::coroutine_handle<> waiter;  // valid only while state == pending
  executor* exec;                  // where to resume the waiter
};

template <typename T>
struct blocking_state : blocking_state_base {
  std::optional<T> result;
  std::exception_ptr error;
};
```

Flow:

1. `await_suspend(h)` stores `h` and `executor::current()` in the state
   block, links the awaiter into the executor's pending-blocking list
   (an intrusive list mirroring `io_head_`, so cancellation can find it),
   and posts the job to the pool.
2. The worker runs `f` inside a try/catch, stores `result` or `error`,
   then atomically transitions `pending → completed`. If the transition
   succeeds, it calls `exec->resume_external(waiter)`. If it fails
   (state was already `cancelled`), it discards the result and returns.
3. `await_resume()` returns `*result` or `std::rethrow_exception(error)`.

The `shared_ptr<blocking_state>` ownership means the state outlives both
the awaiting frame and the worker job, whichever finishes last. No dead
handle is ever resumed: the atomic transition is the once-only resume
guard, playing the same role as the io awaiter's once-only guard.

### 4. Deadline overload

*(As implemented)* `run_blocking(f, timeout)` registers a **deadline
timer**: a timer-wheel entry carrying an expiry hook
(`executor::register_deadline()`) instead of a handle to resume. On
expiry the executor invokes the hook on the executor thread; the hook
runs the same claim-and-resume path as cancellation
(`blocking_awaiter::cancel()`), so the single-resume invariant holds
across all three racing parties (worker, cancel, deadline). The worker
keeps running and discards its result. The deadline entry is
deregistered when the await completes first. Plain sleep timers are
unchanged: entries without a hook resume their handle directly, as
before.

## Cancellation Semantics

**Cancellation is abandonment, and is documented as such.**

`task_control::cancel()` on a task suspended in `run_blocking` walks the
executor's pending-blocking list, marks the state `cancelled`, and
resumes the frame — which throws `dripcore::cancelled` so `teardown()`
runs per the shutdown design. The worker thread finishes the syscall
into the void and discards the result.

This differs from io and timer awaiters, which genuinely cancel their
kernel registration. A blocking syscall cannot be interrupted; the
proposal's contract is:

- The awaiting coroutine becomes cancellable *immediately* (the frame
  resumes and unwinds without waiting for the syscall).
- The worker thread is never interrupted; it may occupy a pool thread
  until the syscall returns.
- No resource owned by `f` is released before `f` returns — `f` must
  clean up after itself, as any blocking function must.

Deadlines have the same semantics: expiry cancels the *await*, not the
*work*.

## Shutdown Semantics

The two-phase shutdown protocol (`doc/shutdown_and_cancellation_design.md`
§5) must never be hostage to a straggling syscall:

1. `scheduler::shutdown()` → `executor::shutdown()` → `cancel_pending()`
   marks every pending blocking awaiter `cancelled` and resumes it, so
   all awaiting frames reach `done()` on the usual drain path —
   regardless of what the worker threads are doing.
2. The pool destructor stops accepting jobs and signals workers. It
   joins threads that finish within a short grace period and **detaches
   any thread still inside a syscall**. Detachment is safe: the job
   holds the `shared_ptr` state block, the state is already
   `cancelled`, so the straggler merely destroys the state block when
   it finishes. The existing forced-frame-destroy path remains the
   final backstop.

Consequence: a stuck `getaddrinfo` can leak a thread until the process
exits, but shutdown latency stays bounded by the drain deadline. That
is the right trade for a CLI process; a long-running server embedding
dripcore may want a configurable policy (see Open Questions).

## Relationship to the Multithreaded Scheduler Proposal

This proposal is a **scoped-down slice of Phase 3**, not a step on its
critical path:

- It implements exactly one Phase 3 line item — "Cross-Thread
  Submission" — reduced to *resuming a handle*, not *submitting a task*.
- It requires none of: multiple executors, work stealing, global queue,
  cross-thread channel semantics, or cross-thread `cancel()`.
- It is forward-compatible: `external_queue_` + wake notification is a
  building block Phase 3 needs anyway, and `resume_external` is the
  natural primitive a future multi-executor `submit()` would call.

**Scope discipline:** the value here is precisely that it is the
~200-line slice that solves real blocking problems *now*. It must not
become a wedge into full multi-threading.

## Usage

The motivating case — DNS resolution without stalling the scheduler:

```cpp
class connector_task : public dripcore::task {
  dripcore::awaitable<> body() override {
    // Off the scheduler thread; honors the OS resolver config.
    auto addrs = co_await run_blocking(
      [host = host_, port = port_] {
        return resolve(host, port);  // blocking getaddrinfo
      },
      std::chrono::seconds(5));

    // Resumed on the executor; proceed with non-blocking connect.
    co_await connect_nonblocking(addrs);
  }
};
```

Applications with their own thread pool can use the substrate directly:
`resume_external` is the whole integration surface. (The standalone
`resolve20` library's `scheduler_like` concept maps to it one-to-one:
`post(h)` → `resume_external(h)`.)

## Testing Strategy

Following `doc/testing_strategy.md`:

- **Deterministic state machine tests:** a fake pool whose `post` runs
  the job inline (or on a controllable thread) exercises every
  transition: complete-before-suspend edge, cancel-pending-complete
  race, complete-pending-cancel race, destroy-while-suspended. Assert
  exactly one resume per awaitable and no dead-handle resume.
- **Cancellation integration:** cancel a task suspended in
  `run_blocking`; assert the frame unwinds promptly while the fake
  worker is still blocked; assert the late completion does not resume.
- **Timeout tests:** tiny deadlines against a worker that sleeps;
  assert `dripcore::cancelled` and that the timer is deregistered on
  the completion-first path.
- **Thread-safety tests:** real pool, many concurrent awaiters, under
  ThreadSanitizer (and AddressSanitizer, per the usual dripcore test
  defaults).
- **Shutdown test:** a worker blocked past the drain deadline must not
  extend `scheduler::shutdown()` latency.

## Alternatives Considered

- **Thread-per-call (`std::jthread` spawned per `run_blocking`):**
  simpler, no pool class; costs a thread create/join per call and makes
  shutdown policy awkward. Acceptable as a first implementation behind
  the same awaitable API; the pool is an internal upgrade.
- **Full Phase 3 multi-threading:** solves this incidentally but drags
  in work stealing, cross-thread channels, and cross-thread
  cancellation — a much larger, riskier change for no additional
  benefit to blocking offload.
- **Application-level wiring (thread + wake pipe per app):** what
  consumers do today. Duplicates the tricky lifetime/cancellation
  logic in every application; the pattern is exactly what a runtime
  should own.
- **Reimplement blocking APIs asynchronously (c-ares et al.):** loses
  OS resolver configuration; per-API effort with no generality.

## Open Questions

1. **Pool default size:** resolved — 2 threads, executor-owned, created
   lazily. Revisit if workloads demand it.
2. **Timeout exception type:** resolved for now — deadline expiry
   throws `dripcore::cancelled`, consistent with sleep cancellation. A
   distinct `dripcore::timeout` remains a compatible future addition.
3. **Straggler policy at shutdown:** resolved — detach (right for
   CLIs). A configurable join grace for long-running embedders remains
   open.
4. **`resume_external` vs. Phase 3 `submit()`:** resolved — public and
   stable; Phase 3's task-level `submit()` is expected to build on it.
5. **Naming:** resolved — `run_blocking`, as a `task` method matching
   `sleep_for()`/`readable()` (which also need the control block).

## References

- Tokio: `task::spawn_blocking`
- libuv: threadpool, `uv_getaddrinfo`
- `doc/multithreaded_scheduler_proposal.md` — Phase 3, of which this is
  a scoped slice
- `doc/shutdown_and_cancellation_design.md` — the cancellation and
  shutdown protocol this integrates with
- resolve20 — standalone `co_await`-able `getaddrinfo`/`getnameinfo`
  library built on the same shared-state pattern; its
  `scheduler_like` concept maps 1:1 onto `resume_external`
