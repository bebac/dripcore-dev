---
title: Multiple Waiters per File Descriptor
tags: [dripcore, c++, coroutines, io, epoll, kqueue, design, limitation]
---

# Multiple Waiters per File Descriptor

## Summary

`event_loop` allows **at most one waiter per `(fd, direction)`**: one task may
`co_await readable(fd)` while another `co_await writable(fd)` on the same fd,
but two tasks may not wait on the same direction of the same fd.

The restriction exists because neither backend can natively represent it.
epoll keeps a single interest-list entry per fd, and kqueue keeps a single
knote per `(fd, filter)`. Two waiters on the same direction would have to share
one kernel registration, so the backend itself must fan a fired event out to
several coroutine handles.

The epoll backend can do that fan-out cheaply, and an earlier revision of this
work did. It was rejected because it would make the two platforms behave
differently: code that works on Linux would hang on macOS. The restriction is
therefore enforced on epoll and documented for kqueue.

This document records the limitation and sketches what it would take to lift it
on both platforms at once by adding the same waiter multiplexing to the kqueue
backend.

**Status:** Limitation documented and enforced in the epoll backend
(`lib/dripcore/source/event_loop_epoll.cpp`). kqueue multiplexing is **not**
implemented.

## The limitation

Supported on both backends:

```cpp
// task A
co_await readable(fd);
// task B, concurrently
co_await writable(fd);
```

Not supported on either backend:

```cpp
// task A
co_await readable(fd);
// task B, concurrently
co_await readable(fd);   // epoll: throws std::logic_error
                         // kqueue: silently replaces A
```

The second form is a programming error under the current model: both tasks are
woken for the same readiness and race to consume it, and whichever loses gets
`EAGAIN`. That is a legitimate pattern for a *multi-acceptor* server — several
tasks racing to `accept()` on one listening socket — which is the main thing
the restriction forecloses.

## Current behaviour

| | one reader + one writer | two readers |
|---|---|---|
| epoll | both armed, both delivered | `register_event` throws `std::logic_error` |
| kqueue | both armed, both delivered | second `EV_ADD` replaces the first knote's `udata`; the first waiter is never resumed |

The epoll backend detects the violation because it already keeps a per-fd
registry (`entries_`). The kqueue backend has no registry and so cannot.

## Why fan-out was not added to epoll alone

An earlier revision multiplexed waiters per fd in the epoll backend only: a
per-fd vector of `(mask, udata)` pairs, armed with the union of the masks, with
`poll()` partitioning the list on delivery. It works, and it is fast (see
*Cost*), but it makes the two platforms disagree — and the disagreement is
silent. A program that fans several readers onto one fd runs on Linux and hangs
on macOS.

A runtime whose semantics depend on the host kernel is a bug factory. Either
both backends support N waiters per direction, or neither does.

The epoll half of that implementation is preserved in
`doc/epoll-multiple-waiters.patch` as the reference for the fan-out logic the
kqueue backend would need to mirror.

## Proposed solution: multiplex waiters in kqueue

Mirror the epoll approach. kqueue's `udata` is per-knote, so a knote can carry
a token identifying an fd-level entry, and that entry can carry the waiter
lists.

### Data structure

```cpp
struct fd_entry
{
  std::uint32_t generation = 0;
  std::vector<void*> read_waiters;
  std::vector<void*> write_waiters;
};

std::unordered_map<int, fd_entry> entries_;
std::uint32_t next_generation_ = 0;
```

### `register_event(fd, events, udata)`

1. Look up or create the `fd_entry`; a new entry takes `++next_generation_`.
2. Push `udata` onto the direction's vector.
3. If that vector was **empty** before the push, `EV_ADD` the knote for
   `(fd, filter)` with the entry's token. Otherwise do nothing — the knote is
   already armed and will fan out.

### `deregister_event(fd, events, udata)`

1. Erase `udata` from the direction's vector.
2. If that vector is now empty, `EV_DELETE` the knote for the direction. With
   `EV_ONESHOT` the knote may already be gone; `ENOENT` is expected and already
   ignored.
3. If both vectors are empty, erase the entry.

### `poll()`

kqueue is simpler than epoll here. `EV_ONESHOT` removes the knote on delivery,
and read and write waiters sit on separate knotes, so nothing needs re-arming
after a delivery:

1. Decode the token from `kevent::udata`, look up the entry, and reject it if
   the generation does not match.
2. Swap the fired direction's vector into batch-local storage.
3. If both vectors are now empty, erase the entry.
4. Invoke the callbacks.

A callback that immediately re-awaits the same fd registers into an empty
vector, which re-arms the knote via step 3 of `register_event`.

### The `udata` token

`udata` is a `void*`, so the packed `(fd, generation)` token used by the epoll
backend can be stored as
`reinterpret_cast<void*>(static_cast<std::uintptr_t>(token))` wherever
`uintptr_t` is 64-bit — true on both supported platforms. On a 32-bit target
the token does not fit and either a side table or a stable per-registration
allocation is needed.

Storing a raw `fd_entry*` instead is tempting because the map is node-based,
but it reintroduces the stale-pointer problem the generation counter exists to
solve: a callback earlier in the same `kevent` batch can erase the entry, and a
later event in that batch would dereference freed memory before it could check
anything.

## What changes when the restriction is lifted

- `lib/dripcore/include/dripcore/event_loop.h` — the contract returns to "any
  number of waiters per `(fd, direction)`" and the `std::logic_error` note goes
  away.
- `lib/dripcore/source/event_loop_epoll.cpp` — `fd_entry` grows the two waiter
  vectors, `register_event` stops throwing, and `poll()` gains the fan-out.
  This is exactly `doc/epoll-multiple-waiters.patch`.
- `lib/dripcore/source/event_loop_kqueue.cpp` — the registry and fan-out
  described above.
- `test/unittest/event_loop_epoll_test.cpp` — the fan-out test returns; a
  kqueue equivalent is needed for the macOS CI leg.
- `test/unittest/scheduler_test.cpp` — "two readers, second task fails"
  becomes "two readers, both complete".

## Cost

Measured on Linux, pinned to one CPU, with
`test/benchmark/io_multiplex_benchmark.cpp`.

Fan-out dispatch, ns per round, one fd event delivered to N waiters:

| waiters | multiplexed | per-event allocation |
|---|---|---|
| 1 | 1075 | 1078 |
| 2 | 1060 | 1287 |
| 4 | 990 | 1776 |
| 8 | 1063 | 2856 |

Multiplexing is flat in the number of waiters because the fired list is swapped
out rather than partitioned. The naive variant allocates two vectors per fired
event and grows linearly.

For reference, the current one-waiter design costs 4 `epoll_ctl` per
register/deregister cycle for a reader and a writer on one fd, with no failed
calls, and runs at 739 ns/cycle against 824 for the unpatched tree.

The kqueue side has not been measured. Expected cost is one hash lookup per
`register_event`/`deregister_event`, one allocation per fd entry, and the
vectors' first-push allocations — the same shape as the epoll backend.

## Alternatives considered

| Option | Verdict |
|---|---|
| Fan out in epoll only | Rejected: silent divergence between Linux and macOS |
| One waiter per `(fd, direction)` (current) | Simple and symmetric; nothing used the restricted case, but it forecloses multi-acceptor |
| Multiplex in both backends | The proposal here |
| Enforce the policy in `executor` instead of the backends | Rejected: the executor has no per-fd index, so it would need an O(pending waits) scan or a second registry duplicating the backend's |
| Multiple knotes per `(fd, filter)` | Not possible: knotes are keyed by `(fd, filter)` |

## Open questions

- **Multi-acceptor semantics.** Once several readers can share one fd, all of
  them are woken and all but one get `EAGAIN`. That needs documenting as the
  expected contract, or `accept`-style use needs a dedicated awaiter.
- **Fan-out ordering.** The epoll backend dispatches readers before writers.
  Nothing depends on it today; it should be stated either way.
- **Duplicate registration.** With multiplexing this stops being an error, so
  the `std::logic_error` path and its test are deleted rather than relaxed.

## References

- `doc/epoll-multiple-waiters.patch` — reference epoll fan-out implementation.
- `lib/dripcore/include/dripcore/event_loop.h` — the interface contract.
- `lib/dripcore/source/event_loop_epoll.cpp` — current epoll backend.
- `lib/dripcore/source/event_loop_kqueue.cpp` — current kqueue backend.
- `test/benchmark/io_multiplex_benchmark.cpp` — the numbers above.
