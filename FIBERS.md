# SBCL Fibers: Lightweight Cooperative Threads

## Table of Contents

1. **Introduction**
   1. Motivation: Why Fibers?
   2. Design Goals
   3. Terminology

2. **Programming API**
   1. Creating and Running Fibers
   2. Yielding and Waiting
   3. Fiber Sleep and Timed Waits
   4. Fiber Parking (Condition-Based Suspend/Resume)
   5. Fiber Join
   6. Spawning Fibers from Fibers
   7. Multi-Carrier Scheduling
      1. `run-fibers` (Simple, Blocking)
      2. `start-fibers` / `submit-fiber` / `finish-fibers` (Dynamic, Long-Lived)
   8. Fiber-Aware Blocking Primitives
      1. Mutex Acquisition
      2. Condition Variables
      3. Semaphores
      4. I/O (`wait-until-fd-usable`)
      5. `sleep` and `wait-for`
   9. Fiber Pinning (Preventing Yield)
   10. Introspection (`list-all-fibers`, `fiber-state`, Backtraces)
   11. Idle Hooks
   12. API Reference Summary

3. **Architecture Overview**
   1. Carrier Threads and Schedulers
   2. Fiber Lifecycle State Machine
   3. Data Flow: Yield and Resume

4. **Context Switching**
   1. Register Save/Restore Convention
   2. The `fiber_switch` Assembly Routine
   3. Stack Frame Initialization for New Fibers
   4. The Entry Trampoline (Assembly to C to Lisp)
   5. Zero-Allocation Design (No SAPs, No GC Pressure)
   6. Thread Register Patching on Carrier Migration

5. **Stack Management**
   1. Control Stack Layout and Guard Pages
   2. Binding Stack (Separate Allocation)
   3. Stack Pooling (`madvise(MADV_DONTNEED)` Recycling)
   4. Stack Overflow Detection in Signal Handlers
   5. Stack Size Tradeoffs (No Dynamic Growth)

6. **Dynamic Variable Bindings (TLS)**
   1. The Problem: `unbind_to_here` Zeroes Entries
   2. TLS Overlay Arrays (Save/Restore Without Unbinding)
   3. Carrier Value Update on Migration
   4. Catch Block and Unwind-Protect Chain Save/Restore
   5. Empty Binding Stack Fast Path
   6. Same-Carrier Resume Optimization

7. **Garbage Collector Integration**
   1. The Two-List Design: Suspended Fibers vs. Active Contexts
   2. `fiber_gc_info`: Conservative Control Stack Scanning
   3. `active_fiber_context`: Carrier + Fiber Stack Visibility
   4. Precise Binding Stack Scavenging
   5. Persistent Carrier Context (Lock-Free Hot Path)
   6. The `fiber-context` Thread Struct Slot (O(1) Lookup)
   7. GC Safety Windows (`without-interrupts` vs. `without-gcing`)
   8. Correctness Argument: Why Partial Updates Are Safe

8. **Scheduler Design**
   1. The Scheduler Loop
   2. Fast Path: Skip Maintenance When Deque Is Hot
   3. Maintenance: Pending Queue Drain, Deadline Expiry, Wake Checks
   4. Post-Switch Dispatch (Suspended, Dead)
   5. Idle Detection and Carrier Parking
   6. Maintenance Frequency Backstop (Every 64 Fibers)

9. **Work Stealing**
   1. Chase-Lev Lock-Free Deque
   2. Owner Operations (Push/Pop from Bottom, LIFO)
   3. Thief Operations (Steal from Top, FIFO, CAS)
   4. Buffer Growth (Power-of-Two Circular Array)
   5. Random Victim Selection
   6. Fiber Migration and Thread Register Fixup

10. **I/O Multiplexing**
    1. Platform Abstraction (epoll, kqueue, poll Fallback)
    2. Edge-Triggered Mode with One-Shot (`EPOLLET | EPOLLONESHOT`)
    3. Indexed I/O Waiters (fd-to-Fiber Table)
    4. Bounded Epoll Drain Loop
    5. `fd-ready-p` and `wait-until-fd-usable` Integration
    6. The Default I/O Idle Hook
    7. Batched FD Polling vs. Per-Fiber Polling

11. **Deadline Scheduling**
    1. Binary Min-Heap with Inline Index
    2. O(log N) Insert and Remove
    3. Batch Expiry (Pop All Expired in One Pass)
    4. Interaction with I/O Waiters (Dual-Indexed Fibers)

12. **Fiber Death and Cleanup**
    1. The Lisp Trampoline (`fiber-trampoline`)
    2. Error Handling and Result Capture
    3. Binding Stack Cleanup on Death (Without `unbind_to_here`)
    4. GC Info Unregistration Timing
    5. Stack Return to Pool

13. **Integration with SBCL**
    1. Thread Struct Extension (`fiber-context` Slot)
    2. `serve-event` Dispatch (Fiber-Aware `wait-until-fd-usable`)
    3. `sleep` and `wait-for` Dispatch
    4. Mutex and Condition Variable Dispatch
    5. Pinned Blocking Fallback to OS Primitives
    6. `*pinned-blocking-action*` Warning/Error Policy
    7. `interrupt-thread`
    8. Debugger Integration

14. **Performance**
    1. HTTP Benchmark
    2. Memory Efficiency
    3. Scalability Under High Connection Counts
    4. Context Switch Microbenchmark
    5. GC Impact

15. **Platform Support**
    1. x86-64 (Linux, macOS, Windows)
    2. ARM64
    3. ARM32
    4. PPC64
    5. PPC32
    6. RISC-V
    7. Feature Flag: `:sb-fiber`
    8. Platform-Specific Assembly and I/O Backends

**Appendix A: Using Hunchentoot with Fibers**
   1. The Fiber Taskmaster
   2. Taskmaster Methods
   3. Starting the Server
   4. How It Works
   5. SSL
   6. Considerations

---

## 1. Introduction

### 1.1 Motivation: Why Fibers?

Many server workloads are concurrent but not parallel.  A web server
handling 10,000 connections spends almost all of its time waiting for
network I/O; the actual computation per request is trivial.  The
natural programming model is one thread of control per connection ---
read a request, compute a response, write it back --- but OS threads
are too expensive to use this way at scale.

Each OS thread in SBCL carries a full-sized control stack (typically
8 MB), a binding stack, signal handling infrastructure, and a kernel
task_struct.  Creating a thread requires `mmap`, `clone`, and TLS
setup; destroying one requires the reverse.  Context switching between
threads requires a kernel transition, TLB management, and scheduler
bookkeeping.  At 10,000 concurrent connections, this means 80 GB of
virtual address space for stacks alone, and the kernel scheduler ---
designed for dozens to hundreds of runnable tasks --- begins to degrade.

The conventional alternative is event-driven programming: a single
thread multiplexes connections using `epoll` or `kqueue`, dispatching
callbacks on I/O readiness.  This scales well but inverts control
flow.  Sequential logic must be broken into state machines or
continuation chains.  Error handling, resource cleanup, and debugging
all become harder.  Backtraces show the event loop, not the logical
call stack of the request being served.

Fibers offer a third option.  A fiber is a user-space thread with its
own control stack and binding stack, scheduled cooperatively by a
library-level scheduler rather than the kernel.  Fibers preserve the
sequential programming model --- the code reads top-to-bottom like a
normal thread --- while achieving the resource efficiency of
event-driven I/O.  A fiber's control stack is 256 KB by default (not
8 MB), context switching saves and restores six registers in user space
(not a full kernel transition), and thousands of fibers can share a
small pool of OS carrier threads.

Common Lisp complicates this picture because the language carries
more implicit per-thread state than most: special variable bindings
live in a separate binding stack, non-local exits thread through
catch tags and `unwind-protect` chains, and all of these must be
saved and restored correctly across suspension points.  A fiber
implementation that only switches the control stack will corrupt this
state.  SBCL's fiber implementation handles all of it: dynamic
bindings made within a fiber are local to that fiber,
`unwind-protect` cleanup forms run on fiber death, and
`handler-case`/`handler-bind` work as expected.

### 1.2 Design Goals

The implementation is guided by several priorities, roughly in order:

**Correctness under GC.**  SBCL uses a generational, compacting,
stop-the-world garbage collector.  The GC must be able to find every
live Lisp object, including those on suspended fiber stacks and in
fiber binding stacks.  Getting this wrong means silent heap corruption.
Every design decision in the fiber runtime is constrained by the
requirement that GC can fire at almost any point (the only exclusion is
`without-gcing` regions) and must see a consistent view of all fiber
state.

**Transparent integration.**  Existing SBCL code should work inside
fibers without modification.  `grab-mutex`, `condition-wait`,
`wait-until-fd-usable`, `sleep`, and `wait-for` all detect when they
are running inside a fiber and yield cooperatively instead of blocking
the carrier thread.  Code that is not fiber-aware does not need to
change.  Code that cannot safely yield (e.g., in the middle of a
multi-step foreign library interaction) can pin the fiber, causing
blocking primitives to fall through to their OS implementations.

**Low per-switch overhead.**  The context switch hot path ---
yield-to-scheduler-to-resume --- must be fast, because fibers switch
far more frequently than OS threads.  The implementation targets
sub-microsecond switches by using a hand-written assembly routine that
saves only callee-saved registers, performing zero heap allocation on
the switch path, and eliminating all mutex acquisitions from the
per-switch code.

**Scalability to many fibers.**  The system should handle tens of
thousands of concurrent fibers without degradation.  This requires
small per-fiber memory footprint (256 KB + 16 KB stacks, pooled for
reuse), O(1) or O(log N) scheduling operations, and efficient I/O
multiplexing that avoids per-fiber polling.

**Multi-core utilization.**  A pool of carrier threads should keep all
cores busy when there is enough work.  Idle carriers should not waste
CPU.  Work should migrate between carriers without programmer
intervention.  The implementation uses lock-free work-stealing deques
(Chase-Lev) so that busy carriers never contend on a shared queue, and
idle carriers can take work from busy ones without locks.

### 1.3 Terminology

- **Fiber**: A lightweight cooperative thread with its own control
  stack and binding stack, scheduled in user space.

- **Carrier thread** (or **carrier**): An OS thread that runs fibers.
  Each carrier has its own scheduler.  A fiber executes on exactly one
  carrier at a time, but may migrate between carriers via work
  stealing.

- **Scheduler**: A per-carrier structure that manages a run queue
  (work-stealing deque), waiting lists, deadline heaps, and I/O
  multiplexer state.  One scheduler per carrier thread.

- **Scheduler group**: A collection of schedulers (one per carrier)
  that cooperate via work stealing.  The unit of fiber lifecycle
  management: fibers are submitted to a group and results are collected
  from it.

- **Yield**: A fiber voluntarily suspends itself, returning control to
  its carrier's scheduler.  The scheduler may then run another fiber on
  the same carrier.

- **Resume**: The scheduler restores a suspended fiber's context and
  transfers control to it.

- **Pin**: Temporarily prevent a fiber from yielding.  While pinned,
  blocking primitives fall through to their OS implementations,
  blocking the carrier thread.

- **Wake condition**: A predicate function associated with a suspended
  fiber.  The scheduler calls it periodically; when it returns true,
  the fiber becomes runnable.

- **Deque** (double-ended queue): The per-carrier run queue, a
  Chase-Lev work-stealing deque.  The owner pushes and pops from the
  bottom (LIFO); thieves steal from the top (FIFO) via CAS.

- **Work stealing**: When a carrier's local run queue is empty, it
  attempts to take a fiber from another carrier's queue.  This
  balances load across cores without centralized scheduling.

## 2. Programming API

All public symbols are exported from the `SB-THREAD` package.  The
fiber API is available when SBCL is built with the `:sb-fiber` feature
(enabled by adding `:sb-fiber` to `local-target-features.lisp-expr`).

### 2.1 Creating and Running Fibers

A fiber is created with `make-fiber` and submitted for execution on a
scheduler group.  It does not begin running until the scheduler picks
it up.

```lisp
(make-fiber function &key name stack-size binding-stack-size initial-bindings)
```

**`function`** --- a function of no arguments.  This is the fiber's
entry point.  When it returns normally, all return values are captured
as a list in the fiber's result slot (retrievable via `fiber-result`,
or as multiple values via `fiber-join`).  If it signals an unhandled
error, the condition object becomes the result instead.

**`name`** --- an optional string for debugging.  Appears in
`print-object` output and backtrace annotations.

**`stack-size`** --- the size of the fiber's control stack in bytes.
Defaults to 256 KB.  This is the usable region; an additional guard
page (typically 4 KB) is allocated below it for stack overflow
detection.  Stacks are drawn from a per-size pool and recycled with
`madvise(MADV_DONTNEED)`, so the cost of creation is usually a pool
hit rather than a fresh `mmap`.

**`binding-stack-size`** --- the size of the fiber's binding stack in
bytes.  Defaults to 16 KB.  Each `(let ((*var* val)) ...)` or `progv`
form pushes a two-word entry (old value + TLS index), so 16 KB
accommodates 1024 nested special variable bindings.  A guard page is
allocated above the usable region; overflow triggers a segfault with
a diagnostic message, just like control stack overflow.

**`initial-bindings`** --- an alist of `(symbol . value)` pairs.
These are established as dynamic bindings (via `progv`) before the
fiber's function is called, analogous to the `:initial-bindings`
argument to `sb-thread:make-thread`.  The values in the alist are
used directly (not evaluated); the caller is responsible for computing
them before passing the alist.  This is the primary mechanism for
giving each fiber its own copy of a special variable:

```lisp
(make-fiber (lambda () (format t "~A~%" *my-connection*))
            :initial-bindings `((*my-connection* . ,conn)))
```

After creation, a fiber is in the `:created` state.  It must be
submitted to a scheduler before it can run (see Section 2.7).

### 2.2 Yielding and Waiting

```lisp
(fiber-yield &optional wake-condition)
```

`fiber-yield` suspends the current fiber and returns control to the
carrier thread's scheduler.  The scheduler may then run another
fiber on the same carrier.  When the fiber is eventually resumed,
`fiber-yield` returns normally and execution continues from the point
of suspension.

If **`wake-condition`** is provided, it must be a function of no
arguments.  The scheduler calls it periodically; when it returns a
true value, the fiber is moved from the waiting list to the run queue.
If `wake-condition` is `nil`, the fiber becomes immediately runnable
again (it is pushed back onto the scheduler's deque and will be
picked up on a subsequent iteration).

`fiber-yield` signals an error if called outside a fiber context or
if the fiber is pinned (pin-count > 0).

The yield path is designed to be fast.  It saves the fiber's binding
stack pointer, catch block, and unwind-protect chain; optionally saves
TLS overlay values (skipped when the binding stack is empty); restores
the carrier thread's binding state; and then performs a register-level
context switch back to the scheduler.  The entire operation involves
zero heap allocation and zero mutex acquisition.

### 2.3 Fiber Sleep and Timed Waits

```lisp
(fiber-sleep seconds)
```

Suspends the current fiber for at least `seconds` (which may be
fractional).  Implemented by setting a deadline in the fiber struct
and yielding with no wake condition.  The scheduler's deadline heap
expires the fiber when `get-internal-real-time` passes the target.
This avoids allocating a closure for the wake condition --- the
scheduler checks the deadline field directly.

`fiber-sleep` integrates with the standard `cl:sleep` function.
When `sleep` is called inside a fiber, SBCL dispatches to
`fiber-sleep` automatically.  If the fiber is pinned, `sleep` falls
through to the OS implementation (blocking the carrier thread).

### 2.4 Fiber Parking (Condition-Based Suspend/Resume)

```lisp
(fiber-park predicate &key timeout)
```

`fiber-park` is the general-purpose suspension primitive.  It
combines a wake predicate with an optional timeout:

- **`predicate`** --- a function of no arguments.  The scheduler
  calls it during maintenance passes; when it returns true, the fiber
  is woken.

- **`timeout`** --- seconds (may be fractional) after which the fiber
  is woken regardless of the predicate.

Returns `t` if the predicate was satisfied, `nil` if the timeout
expired first.

Internally, `fiber-park` stores the timeout as a deadline in the
fiber's `deadline` slot and passes the predicate to `fiber-yield`.
The scheduler handles deadline expiry and predicate checking in its
maintenance loop, so `fiber-park` itself allocates nothing beyond what
the caller passes in.  This two-channel wake design (predicate OR
deadline) is the building block for all higher-level waiting
operations: mutex acquisition, condition variables, I/O waits, and
`fiber-join`.

For a pure timed wait with no wake condition, use `fiber-sleep`
(Section 2.3) rather than `fiber-park` with a dummy predicate.
`fiber-park` is intended for cases where you need to wait until a
condition becomes true.

### 2.5 Fiber Join

```lisp
(fiber-join target &key timeout)
```

Waits until the fiber `target` completes and returns the fiber
function's return values (via `values-list`), analogous to
`join-thread`.  If the fiber terminated due to an unhandled error, the
single return value is the condition object; use `fiber-error-p` to
distinguish this from a normal return.

Returns `nil` (single value) if the timeout expires first.
A fiber cannot join itself.

```lisp
;; Single return value:
(fiber-join child)  => result

;; Multiple return values:
(multiple-value-bind (x y) (fiber-join child)
  (format t "Got ~S and ~S~%" x y))

;; Error checking:
(let ((result (fiber-join child)))
  (if (fiber-error-p child)
      (format t "Fiber failed: ~A~%" result)
      (format t "Fiber returned: ~S~%" result)))
```

`fiber-join` works from two contexts:

- **From a fiber**: Parks the calling fiber with a predicate that
  checks `(eq (fiber-state target) :dead)`.  This is cooperative ---
  the calling fiber yields and other fibers continue running on the
  same carrier.

- **From an OS thread**: Spin-waits with 1 ms sleeps between checks.
  This is the appropriate behavior for a thread that is collecting
  results from a fiber group.

### 2.6 Spawning Fibers from Fibers

A running fiber can create and submit new fibers using the same
`make-fiber` and `submit-fiber` API.  The new fiber is submitted to
the current scheduler's group (if any), making it visible to the
work-stealing infrastructure:

```lisp
(let ((child (make-fiber (lambda () (compute-something)))))
  (submit-fiber (fiber-scheduler-group *current-scheduler*) child)
  ;; ... do other work ...
  (fiber-join child))
```

There is no implicit parent-child relationship.  The spawning fiber
is not suspended while the child runs; both are independently
schedulable.  The parent must explicitly join if it needs the child's
result.

When `submit-fiber` targets a scheduler group, the fiber is pushed
onto a randomly chosen carrier's pending queue (a thread-safe atomic
list) and a parked carrier is woken if one exists.  If all carriers
are busy, the fiber remains in the pending queue until the next
maintenance pass drains it.  This is safe to call from any thread,
including non-carrier threads and fibers on other carriers.

### 2.7 Multi-Carrier Scheduling

Two API styles are provided: a simple blocking interface for
batch-style workloads, and a dynamic interface for long-lived server
applications.

#### 2.7.1 `run-fibers` (Simple, Blocking)

```lisp
(run-fibers fibers &key carrier-count idle-hook)
  => list-of-results
```

Creates a scheduler group with `carrier-count` carrier threads
(defaulting to the number of available CPUs, cgroup-aware on Linux),
distributes `fibers`
round-robin across the carriers, runs them all to completion, and
returns a list of their results in the same order as the input.
This is a convenience wrapper around `start-fibers` + `finish-fibers`.

Typical use:

```lisp
(let ((fibers (loop for url in urls
                    collect (make-fiber (lambda () (fetch url))))))
  (run-fibers fibers :carrier-count 4))
```

#### 2.7.2 `start-fibers` / `submit-fiber` / `finish-fibers` (Dynamic, Long-Lived)

For applications that submit work over time (e.g., a web server
submitting a new fiber per accepted connection):

```lisp
(start-fibers initial-fibers &key carrier-count idle-hook)
  => fiber-scheduler-group
```

Starts `carrier-count` carrier threads and distributes
`initial-fibers` round-robin.  Returns a `fiber-scheduler-group`
handle immediately.  All carriers run on background threads.

```lisp
(submit-fiber group fiber)
```

Submits a new fiber to the group.  Atomically increments the group's
active count (so carriers know not to exit), pushes the fiber onto a
randomly chosen carrier's pending queue, and wakes a parked carrier
if one exists.  This is safe to call from any thread.

```lisp
(finish-fibers group)  => list-of-results
(fiber-group-done-p group) => boolean
```

`finish-fibers` blocks until all fibers in the group have completed,
then returns their results.
`fiber-group-done-p` is a non-blocking check that returns `t` when
the group's active count reaches zero.

The dynamic API is the intended interface for server integration.
A server's accept loop creates fibers and submits them; the carrier
threads run them; the fibers yield on I/O and resume when data is
available.  The carrier count controls parallelism, and work
stealing keeps all carriers busy regardless of which carrier a fiber
was submitted to.

### 2.8 Fiber-Aware Blocking Primitives

Existing SBCL blocking primitives detect fiber context and yield
cooperatively rather than blocking the carrier thread.  This is the
key to transparent integration: library code that calls `grab-mutex`
or `sleep` works correctly inside fibers without modification.

Each primitive follows the same pattern: check if running in a fiber
and if the fiber can yield (not pinned); if so, park the fiber with
an appropriate wake condition; if pinned, invoke the configured
`*pinned-blocking-action*` and fall through to the OS implementation.

#### 2.8.1 Mutex Acquisition

When a fiber calls `sb-thread:grab-mutex` and the mutex is contended,
the fiber parks with a predicate that checks whether the mutex state
is free.  On wake, it attempts a CAS to acquire the mutex.  If the
CAS fails (another fiber grabbed it first), the fiber parks again.
This retry loop is necessary because wake conditions are checked
periodically, not atomically with the state change.

#### 2.8.2 Condition Variables

`sb-thread:condition-wait` in a fiber context: releases the mutex,
parks the fiber with a generation-counter predicate (woken when
`condition-notify` increments the waitqueue's generation), re-acquires
the mutex on wake.  The generation counter avoids lost wakeups ---
if `condition-notify` fires between the mutex release and the yield,
the generation will already have changed when the predicate is first
checked.

#### 2.8.3 Semaphores

Semaphore operations (`wait-on-semaphore`, `signal-semaphore`) follow
the same pattern as mutexes: park until the count is positive, then
CAS to decrement.

#### 2.8.4 I/O (`wait-until-fd-usable`)

```lisp
(sb-sys:wait-until-fd-usable fd direction &optional timeout)
```

In fiber context, this registers the file descriptor with the
scheduler's event multiplexer (epoll on Linux, kqueue on BSDs) and
parks the fiber.  On Linux with epoll available, the registration
uses edge-triggered mode with one-shot (`EPOLLET | EPOLLONESHOT`):
the kernel delivers a single event when the fd becomes ready, then
disables the registration.  This eliminates spurious wakeups and
avoids the thundering-herd problem when multiple fibers wait on
related fds.

The scheduler's idle hook calls `epoll_wait` when no fibers are
runnable, waking fibers whose fds have become ready.  The fiber
stores its waited-on fd and direction in struct fields so the
scheduler can index into an fd-to-fiber table for O(1) dispatch.

When epoll is not available (or on non-Linux platforms), the fallback
is a batched `poll()` call over all waiting fds.

#### 2.8.5 `sleep` and `wait-for`

`cl:sleep` dispatches to `fiber-sleep` when called in a fiber context
(see Section 2.3).

`sb-ext:wait-for` dispatches to `fiber-park` with the test function
as the predicate and the timeout converted to a deadline.

### 2.9 Fiber Pinning (Preventing Yield)

```lisp
(fiber-pin &optional fiber)
(fiber-unpin &optional fiber)
(fiber-can-yield-p &optional fiber)
(with-fiber-pinned ((&optional fiber)) &body body)
```

Pinning increments a per-fiber counter; unpinning decrements it.  A
fiber with a non-zero pin count cannot yield.  Any attempt to call
`fiber-yield` while pinned signals an error.

The purpose of pinning is to protect regions of code that cannot
tolerate suspension.  The primary case is thread-affine foreign
state: many C libraries use thread-local storage internally (OpenSSL
contexts, database handles, GPU contexts).  If a fiber yields
mid-interaction and is later resumed on a different carrier via work
stealing, the C library's thread-local state belongs to the old
carrier --- the fiber is now on the wrong OS thread.  This should be
rare in practice, but pinning provides a simple guard against it.

When a pinned fiber calls a blocking primitive (`grab-mutex`, `sleep`,
`wait-until-fd-usable`, etc.), the primitive detects the pin and falls
through to the OS implementation, blocking the carrier thread.  The
variable `*pinned-blocking-action*` controls how this is reported:

- **`:warn`** (default) --- emits a warning identifying the fiber and
  the operation.
- **`:error`** --- signals an error.
- **`nil`** --- silently falls through.

This policy exists because blocking a carrier thread degrades
throughput for all fibers on that carrier.  The warning helps
developers identify code that should either be restructured to avoid
blocking while pinned, or that legitimately needs to pin (in which
case the policy can be set to `nil`).

`with-fiber-pinned` is the recommended interface.  It pins on entry,
executes the body, and unpins in an `unwind-protect` cleanup form,
ensuring the pin count is always restored even if the body signals.

### 2.10 Introspection (`list-all-fibers`, `fiber-state`, Backtraces)

```lisp
(list-all-fibers)  => list
```

Returns a copy of the global fiber list.  Includes all fibers that
have been created and not yet destroyed (i.e., all states: `:created`,
`:runnable`, `:running`, `:suspended`, `:dead`).  The list is captured
under a mutex to provide a consistent snapshot.

```lisp
(fiber-state fiber)  => keyword
```

Returns the fiber's current state: `:created` (made but not yet
submitted), `:runnable` (on a run queue), `:running` (actively
executing on a carrier), `:suspended` (yielded with a wake condition
or deadline), or `:dead` (function returned or signaled).

```lisp
(fiber-name fiber)     => string-or-nil
(fiber-result fiber)   => list
(fiber-alive-p fiber)  => boolean
```

`fiber-result` returns the fiber's result values as a list (or a
single-element list containing the condition object on error) after
death.  Use `fiber-join` to receive the values via `values-list`.  `fiber-alive-p` is shorthand for
`(not (eq (fiber-state fiber) :dead))`.

```lisp
(print-fiber-backtrace fiber &key stream count)
```

Prints a full symbolic backtrace for a suspended fiber, using SBCL's
standard debugger infrastructure.  Produces the same human-readable
output as `print-backtrace` --- function names, arguments, source
locations, and local variables.  Internally it binds the debug stack
bounds to the fiber's control stack and walks frames starting from
`fiber-top-frame`.  Only works for `:suspended` or `:created` fibers;
a running fiber's stack is live on a carrier and being actively
modified.

### 2.11 Idle Hooks

```lisp
(make-fiber-scheduler &key idle-hook)
(start-fibers fibers &key carrier-count idle-hook)
(run-fibers fibers &key carrier-count idle-hook)
```

The **idle hook** is a function of one argument (the scheduler) called
when the scheduler has no runnable fibers but has suspended fibers
waiting for a condition.  The hook's job is to block the carrier
thread efficiently until something changes --- typically by performing
I/O multiplexing.

The default idle hook (`fiber-io-idle-hook`) calls `epoll_wait` (on
Linux) or a batched `poll()` (elsewhere) with a timeout derived from
the nearest deadline in the scheduler's deadline heap.  When an fd
event arrives, the corresponding fiber is moved to the run queue.

Custom idle hooks are useful for integrating fibers with external
event sources that are not file descriptors.  For example, a message
queue consumer might check for new messages in its idle hook.  The
hook should block for a bounded time (the default uses the nearest
deadline as its timeout) to ensure deadline-based fibers are not
starved.

### 2.12 API Reference Summary

| Function | Description |
|---|---|
| `make-fiber` | Create a fiber (does not start it) |
| `submit-fiber` | Submit a fiber to a scheduler or group |
| `run-fibers` | Create group, run fibers, return results (blocking) |
| `start-fibers` | Create group and start carriers (non-blocking) |
| `finish-fibers` | Block until all fibers complete, return results |
| `fiber-group-done-p` | Non-blocking completion check |
| `fiber-yield` | Suspend current fiber |
| `fiber-sleep` | Suspend for a duration |
| `fiber-park` | Suspend until predicate or timeout |
| `fiber-join` | Wait for fiber completion |
| `fiber-pin` / `fiber-unpin` | Prevent/allow yielding |
| `with-fiber-pinned` | Pin for duration of body |
| `fiber-can-yield-p` | Check if fiber can yield |
| `current-fiber` | Return current fiber or nil |
| `fiber-state` | Return fiber's lifecycle state |
| `fiber-name` | Return fiber's name |
| `fiber-result` | Return fiber's result values (as list) |
| `fiber-error-p` | Check if fiber terminated with an error |
| `fiber-alive-p` | Check if fiber is not dead |
| `list-all-fibers` | Snapshot of all live fibers |
| `print-fiber-backtrace` | Symbolic backtrace for suspended fiber |
| `*current-fiber*` | Dynamic variable: current fiber |
| `*current-scheduler*` | Dynamic variable: current scheduler |
| `*pinned-blocking-action*` | Policy for pinned blocking (`:warn`, `:error`, `nil`) |

## 3. Architecture Overview

### 3.1 Carrier Threads and Schedulers

The fiber runtime is organized around a two-level hierarchy: carrier
threads and schedulers.

Each carrier thread is a normal SBCL OS thread (`sb-thread:thread`)
that runs a scheduler loop.  The scheduler loop pulls fibers from a
local work-stealing deque, runs them until they yield or die, handles
post-switch bookkeeping, and repeats.  When the deque is empty, the
scheduler performs maintenance (draining pending queues, checking wake
conditions, expiring deadlines, polling I/O) and attempts work
stealing from sibling schedulers.

A scheduler group (`fiber-scheduler-group`) binds multiple schedulers
together.  Each group has a vector of scheduler structs (one per
carrier), a shared active-count for lifecycle tracking, a mutex and
condition variable for carrier parking, and a list of submitted
fibers for result collection.

The separation between scheduler and group is deliberate.  A single
carrier with a single scheduler is the simplest configuration: no
work stealing, no cross-thread coordination.  Multiple carriers in
a group add parallelism via work stealing.  The group's active-count
is the coordination mechanism: carriers spin down when it reaches
zero; `submit-fiber` increments it atomically and wakes a parked
carrier.

### 3.2 Fiber Lifecycle State Machine

A fiber passes through five states:

```
  :created ──submit──> :runnable ──schedule──> :running
                            ^                      |
                            |                  yield/wake
                            |                      |
                            +────────────── :suspended
                                                   |
                                               (or from :running)
                                                   |
                                                   v
                                                :dead
```

- **`:created`** --- `make-fiber` returns a fiber in this state.  It
  has allocated stacks and a GC info record, but has not been
  submitted to any scheduler.

- **`:runnable`** --- `submit-fiber` or a wake event transitions the
  fiber here.  The fiber is on some scheduler's deque or pending
  queue, waiting to be picked up.

- **`:running`** --- The scheduler has popped the fiber and is
  executing it on a carrier.  Exactly one fiber per carrier can be
  in this state.

- **`:suspended`** --- `fiber-yield` transitions here.  The fiber has
  a saved stack frame and optionally a wake condition, deadline, or
  I/O wait registration.

- **`:dead`** --- The fiber's function returned (or signaled an
  unhandled error).  Results are captured, resources are cleaned up,
  and the fiber is removed from the global list.

### 3.3 Data Flow: Yield and Resume

A complete yield-and-resume cycle involves these steps:

**Yield** (fiber → scheduler):

1. Save the fiber's binding stack pointer, catch block, and
   unwind-protect chain into the fiber struct.
2. Update the fiber's `gc_info` binding stack pointer (so GC can
   still scan the binding stack after the carrier's BSP is restored).
3. Save the fiber's TLS overlay values and restore the carrier's TLS
   values (skipped if binding stack is empty).
4. Restore the carrier's BSP, catch block, and unwind-protect chain.
5. Set fiber state to `:suspended` and store the wake condition.
6. Call `fiber_switch`: save six callee-saved registers, write RSP to
   the fiber's saved-rsp slot, load RSP from the scheduler's
   saved-rsp slot, restore six registers, `ret` into the scheduler.

**Resume** (scheduler → fiber):

1. Save the carrier's BSP, catch block, and unwind-protect chain.
2. Update the active fiber GC context (fiber stack boundaries and
   carrier stack scan range) --- zero mutex locks.
3. Set the carrier's BSP to the fiber's binding stack pointer.
4. Restore the fiber's catch block and unwind-protect chain.
5. If the fiber has bindings: update carrier values in the binding
   stack (only on migration) and replay the TLS overlay.
6. Widen the fiber's `gc_info` to cover the full stack (conservative
   scan during the window before `fiber_switch`).
7. Set effective stack bounds to the fiber's stack.
8. Call `fiber_switch`: save six registers, write RSP to the
   scheduler's saved-rsp slot, load RSP from the fiber's saved-rsp
   slot, restore six registers, `ret` into the fiber (at the point
   after the yield's `fiber_switch` call).
9. On return: restore effective stack bounds, update or unregister
   the fiber's GC info, clear the active fiber context, restore the
   carrier's BSP.

## 4. Context Switching

### 4.1 Register Save/Restore Convention

The fiber context switch saves only the callee-saved registers defined
by the platform ABI.  This is the minimum set that a C or Lisp
function is entitled to assume is preserved across a call.  All other
registers are either caller-saved (the compiler already assumes they
are clobbered) or are restored by normal function return.

**x86-64 SysV ABI** (Linux, macOS): 6 registers (48 bytes)
- `rbp`, `rbx`, `r12`, `r13`, `r14`, `r15`

**x86-64 Win64 ABI** (Windows): 8 GPRs (64 bytes) + 10 XMMs (160 bytes) = 224 bytes
- `rbp`, `rbx`, `rdi`, `rsi`, `r12`-`r15`
- `xmm6`-`xmm15`

**ARM64 (AAPCS64)**: 20 registers (160 bytes)
- `x19`-`x28`, `x29` (FP), `x30` (LR)
- `d8`-`d15` (SIMD/FP callee-saved)

**ARM32 (AAPCS)**: 104 bytes
- `d8`-`d15` (VFP, 64 bytes), `r3`-`r11`, `lr` (40 bytes)

**PPC64 (ELFv2)**: 320 bytes
- LR, CR, `r14`-`r31` (18 GPRs), `f14`-`f31` (18 FPRs)

**PPC32**: 240 bytes
- LR, CR, `r14`-`r31` (18 GPRs × 4 bytes), `f14`-`f31` (18 FPRs × 8 bytes)

**RISC-V (RV64)**: 208 bytes
- `ra`, `cfp` (for debugger), `s0`-`s11` (12 GPRs), `fs0`-`fs11` (12 FPRs)

The choice to save only callee-saved registers (rather than the full
register file) is the primary reason fiber switches are fast.  A
kernel context switch must save and restore all registers plus
segment registers, FPU state, and signal masks.  A fiber switch saves
6 registers on x86-64 SysV (48 bytes), stores one pointer, loads one
pointer, restores 6 registers, and returns.

### 4.2 The `fiber_switch` Assembly Routine

`fiber_switch` is the core of the context switch.  It is implemented
as a Lisp assembly routine (via `define-assembly-routine`) rather than
a hand-written `.S` file, which means it is assembled by SBCL's
assembler and stored in the code heap alongside compiled Lisp
functions.

The routine takes three arguments in ABI registers:

- **old-slot**: raw address of the word where the current RSP should
  be saved (either a fiber's `saved-rsp` slot or a scheduler's
  `saved-rsp` slot).
- **new-slot**: raw address of the word containing the target RSP to
  load.
- **thread-ptr**: if non-zero, patch the thread register (`r13` on
  x86-64, `x21` on ARM64) after restoring registers.  This handles
  carrier migration.

The x86-64 implementation:

```asm
  push rbp           ; save callee-saved registers
  push rbx
  push r12
  push r13
  push r14
  push r15
  mov [old-slot], rsp ; save current RSP
  mov rsp, [new-slot] ; load target RSP
  pop r15            ; restore callee-saved registers
  pop r14
  pop r13
  pop r12
  pop rbx
  pop rbp
  test thread-ptr, thread-ptr
  jz done
  mov r13, thread-ptr ; patch thread register
done:
  ret                ; return to caller on new stack
```

The `ret` is the critical instruction.  When switching from a running
fiber to the scheduler, `ret` pops the return address that was pushed
when the scheduler originally called `fiber_switch`, returning
execution to the scheduler loop.  When switching from the scheduler
to a suspended fiber, `ret` pops the return address from the fiber's
stack, returning to the point just after the fiber's `fiber_switch`
call inside `fiber-yield`.

### 4.3 Stack Frame Initialization for New Fibers

A brand-new fiber has never been switched to, so its stack has no
saved registers.  `initialize-fiber-stack` constructs a synthetic
stack frame that makes `fiber_switch`'s register restore sequence
"return" into the entry trampoline.

On x86-64 SysV, the initial stack layout (from top, growing down):

```
  stack_top - 8:   0              (alignment padding)
  stack_top - 16:  0              (spare slot)
  stack_top - 24:  trampoline_addr (return address for ret)
  stack_top - 32:  fiber_lispobj  (popped into rbp)
  stack_top - 40:  0              (popped into rbx)
  stack_top - 48:  0              (popped into r12)
  stack_top - 56:  0              (popped into r13)
  stack_top - 64:  0              (popped into r14)
  stack_top - 72:  0              (popped into r15)
              ^--- initial saved RSP
```

When `fiber_switch` loads this RSP and pops registers, `rbp` receives
the fiber's lispobj (a tagged Lisp pointer to the fiber struct).  The
`ret` instruction then jumps to `fiber_entry_trampoline`.

### 4.4 The Entry Trampoline (Assembly to C to Lisp)

`fiber_entry_trampoline` is the landing pad for first-time fiber
execution.  It receives the fiber lispobj in `rbp` (placed there by
the synthetic stack frame) and:

1. Moves the fiber lispobj into the first C argument register (`rdi`
   on SysV, `rcx` on Win64).
2. Clears `rbp` (no valid frame pointer for a new fiber).
3. Calls `fiber_run_and_finish` (a C function).
4. Terminates with `hlt` (unreachable; the C function does not
   return).

`fiber_run_and_finish` in turn calls a Lisp trampoline function
(`fiber-trampoline`) registered at scheduler startup.  This Lisp
function runs the fiber's user function with `handler-case` error
handling, captures the result, marks the fiber dead, cleans up
bindings, and switches back to the scheduler.  The chain is:

```
fiber_switch → fiber_entry_trampoline (asm)
             → fiber_run_and_finish (C)
             → fiber-trampoline (Lisp)
             → user function
```

The Lisp trampoline never returns; it ends with a `fiber_switch` back
to the scheduler.  The `hlt` after the C call is a safety net.

### 4.5 Zero-Allocation Design (No SAPs, No GC Pressure)

The `%fiber-switch` VOP passes arguments as raw machine words, not
Lisp objects.  The saved-rsp slot addresses are computed from the
fiber struct's tagged pointer using arithmetic (adding the slot offset
and subtracting the lowtag), producing unsigned integers that never
become heap-allocated SAPs.

This matters because SAP allocation can trigger GC, which would
move the fiber struct (invalidating the address just computed) and
require stopping the world at an inconvenient point.  By keeping
everything as raw words, the switch path creates zero GC pressure.

The TLS scratch hash table and overlay arrays are pre-allocated in the
scheduler and fiber structs, not on the switch path.  The wake
condition closures are allocated by the caller before yielding.

### 4.6 Thread Register Patching on Carrier Migration

SBCL reserves a machine register as a pointer to the current thread
struct: `r13` on x86-64 (when `gs`-segment TLS is not used), `x21`
on ARM64.  This register is set once when a thread starts and never
changes during normal execution.

When a fiber migrates between carriers via work stealing, it was
suspended on carrier A with A's thread pointer in its saved
registers.  If it resumes on carrier B, the restored thread register
would point to A's thread struct --- the wrong thread.

The `resume-fiber-internal` function handles this by passing the
current carrier's thread SAP as the third argument to `fiber_switch`.
The assembly routine checks if this is non-zero and, if so, overwrites
the just-restored thread register with the correct value.  This
happens after all other registers are restored but before the `ret`,
so the fiber resumes with a valid thread pointer on its new carrier.

For same-carrier resumes (the common case), the third argument is
still passed (the current thread pointer), but the patching is
harmless --- it sets the register to its already-correct value.

## 5. Stack Management

### 5.1 Control Stack Layout and Guard Pages

Each fiber's control stack is a contiguous region obtained via
`os_allocate` (`mmap` on Unix, `VirtualAlloc` on Windows).  The
layout, from low to high address:

```
  +-------------------+  ← base (mmap'd address)
  | Guard page (4 KB) |  PROT_NONE — segfault on access
  +-------------------+  ← control-stack-start (usable region)
  |                   |
  |   Usable stack    |  256 KB default (grows downward)
  |                   |
  +-------------------+  ← control-stack-end (stack top)
```

The guard page at the bottom catches stack overflow.  When a fiber's
stack pointer descends into the guard page, the hardware generates a
segfault.  SBCL's signal handler calls `check_fiber_guard_page`,
which walks both the active fiber contexts and the suspended fiber
GC info list to match the fault address against fiber stack ranges.
If matched, it calls `lose()` with a diagnostic message.

The usable stack region is what the fiber's code sees.  RSP starts
at `control-stack-end` (the top) and grows downward toward the guard
page.

### 5.2 Binding Stack (Separate Allocation)

Each fiber has a separate binding stack for dynamic variable bindings.
This is a flat array of two-word entries (old value, TLS index),
allocated via `alloc_fiber_binding_stack`.  The default size is 16 KB,
accommodating 1024 nested bindings.

The binding stack grows upward (binding-stack-pointer increases with
each bind).  A guard page (`PROT_NONE`) is placed above the usable
region, at the top of the allocation.  If a fiber exhausts its
binding stack, the next bind writes into the guard page and triggers
a segfault.  `check_fiber_guard_page` matches the fault address
against the binding stack guard regions in `all_fiber_gc_info` and
reports a diagnostic message via `lose()`.

The 16 KB default is generous for nearly all workloads; programs
that use deeply nested dynamic bindings can increase it via the
`:binding-stack-size` keyword to `make-fiber`.

The binding stack is a separate allocation because its access pattern
differs from the control stack.  The control stack is accessed
randomly (function calls, local variables); the binding stack is
accessed linearly (push on bind, pop on unbind).  Keeping them
separate also simplifies GC scanning: the binding stack contains
only TLS indices and Lisp object references, scavenged precisely,
while the control stack is scanned conservatively.

### 5.3 Stack Pooling (`madvise(MADV_DONTNEED)` Recycling)

Creating a fiber requires two `mmap` calls (control stack + binding
stack) and destroying one requires two `munmap` calls.  At high fiber
creation rates (e.g., one per HTTP request), these syscalls become a
bottleneck.

The stack pool eliminates this cost.  Two global pools (one for
control stacks, one for binding stacks) cache up to 4096 stacks each.
When a fiber dies and its stack is returned, the pool first calls
`madvise(MADV_DONTNEED)` on the usable region, which tells the kernel
to release the physical pages while keeping the virtual mapping.  The
next access to those pages returns zero-filled memory from the
kernel's page fault handler --- effectively a fresh stack without an
`mmap`/`munmap` round trip.

Pool lookup is O(1): each pool is a singly-linked free list protected
by a mutex.  Stacks are pooled only when their size matches the
pool's cached size (typically the default 256 KB or 16 KB); mismatched
sizes bypass the pool and are deallocated immediately.  This avoids
complexity while handling the common case efficiently.

The guard page's `PROT_NONE` protection is unaffected by `madvise`,
so recycled control stacks retain their overflow protection without
needing to re-set page permissions.

### 5.4 Stack Overflow Detection in Signal Handlers

SBCL's signal handler for `SIGSEGV` (or `SIGBUS` on some platforms)
calls `check_fiber_guard_page` to determine whether the fault address
is in a fiber's guard page.  This function walks two data structures:

1. **`all_active_fiber_contexts`** --- fibers currently executing on
   carriers.  Their `fiber_stack_start` marks the top of the guard
   page.

2. **`all_fiber_gc_info`** --- suspended fibers.  Their
   `control_stack_base` marks the top of the guard page.

If the fault address falls within one guard page width below a stack's
start, the fiber has overflowed.  The handler calls `lose()` with a
diagnostic message including the fault address and stack boundaries.

### 5.5 Stack Size Tradeoffs (No Dynamic Growth)

The default control stack size of 256 KB is a compromise.  Smaller
stacks allow more concurrent fibers (10,000 fibers at 256 KB = 2.5 GB
virtual, versus 80 GB for OS threads at 8 MB), but too-small stacks
cause overflow in code that uses deep recursion or large
stack-allocated arrays.

The binding stack default of 16 KB is conservative for most
workloads.  Each `let` over a special variable or `progv` form pushes
one 16-byte entry, so 16 KB supports 1024 nesting levels.  Server
request handlers typically use a handful of special variables.

Both sizes are configurable per-fiber via `make-fiber` keywords.

Dynamic stack growth (as seen in Erlang/BEAM processes) is not
supported.  Fiber stacks are fixed-size `mmap` allocations; growing
them would require either relocating the stack (invalidating all
interior pointers, return addresses, and GC-conservative references)
or using discontiguous segments (requiring compiler support for
segment-crossing checks on every function call).  Neither approach
is feasible within SBCL's native-code compilation model.  The
practical mitigation is to choose an appropriate stack size at
`make-fiber` time and use heap allocation for large data structures.

## 6. Dynamic Variable Bindings (TLS)

### 6.1 The Problem: `unbind_to_here` Zeroes Entries

SBCL implements special variable bindings as a thread-local storage
(TLS) array indexed by per-symbol TLS indices.  When code binds a
special variable, the runtime pushes a two-word entry (old value at
offset 0, TLS index at offset +word) onto
the binding stack and writes the new value into the thread's TLS
array.  When the binding is unwound, `unbind_to_here` pops entries,
restoring old values and zeroing the binding stack entries.

This create a problem for fibers.  When a fiber yields, it must save
its TLS state and restore the carrier thread's TLS state.  But the
binding stack entries are the mechanism for tracking what needs to be
restored --- and `unbind_to_here` destroys them.  If we unwound the
fiber's bindings on yield (calling `unbind_to_here`), the binding
stack would be zeroed out, and we would have no record of which TLS
indices were bound or what the fiber's values were.

The fiber implementation therefore never calls `unbind_to_here` during
yield or resume.  Instead, it uses a TLS overlay approach.

### 6.2 TLS Overlay Arrays (Save/Restore Without Unbinding)

Each fiber has two parallel arrays: `tls-indices` and `tls-values`.
On yield, `save-fiber-tls-and-restore-carrier` performs a two-pass
walk of the binding stack:

**Pass 1**: Count unique TLS indices using a scratch hash table
(pre-allocated in the scheduler struct to avoid consing).

**Pass 2**: For each unique TLS index, save the current TLS value
(the fiber's value) into the fiber's `tls-values` array, then restore
the carrier's value by writing the outermost `old_value` from the
binding stack into the TLS array.

On resume, `restore-fiber-tls-overlay` replays the fiber's saved
values back into the TLS array:

```lisp
(dotimes (i (length indices))
  (setf (sap-ref-word thread-sap (aref indices i))
        (aref values i)))
```

This is a simple indexed store loop with no hash lookups, no binding
stack manipulation, and no calls into the runtime.  The binding stack
itself is never modified --- it retains the full history of binds
needed for `unwind-protect` and error handling.

### 6.3 Carrier Value Update on Migration

When a fiber migrates between carriers via work stealing, its binding
stack's old-value entries still contain values from the previous
carrier's TLS array.  If the fiber later yields or dies, the restore
path would write the old carrier's values into the new carrier's TLS
--- corrupting the new carrier's state.

`update-binding-stack-carrier-values` fixes this.  Before replaying
the TLS overlay, it walks the binding stack and overwrites each
outermost old-value with the current carrier's TLS value for that
index.  This ensures that when the fiber yields, the carrier's TLS
is restored correctly regardless of which carrier the fiber was
originally on.

This operation is skipped on same-carrier resumes (the common case),
gated by a migration flag computed from
`(not (eq (fiber-carrier fiber) *current-thread*))`.

### 6.4 Catch Block and Unwind-Protect Chain Save/Restore

Beyond the binding stack, SBCL maintains per-thread pointers to the
current catch block (`*current-catch-block*`) and the current
unwind-protect block (`*current-unwind-protect-block*`).  These are
singly-linked lists threaded through the control stack, used by
`throw`, `handler-case`, and `unwind-protect`.

On yield, the fiber saves these pointers from the thread struct into
`fiber-saved-catch-block` and `fiber-saved-unwind-protect-block`, then
restores the carrier's values (saved when the fiber was mounted).

On resume, the carrier's values are saved and the fiber's are
restored.  For new fibers, these are zero (no catch or unwind-protect
blocks established yet).

### 6.5 Empty Binding Stack Fast Path

Many fibers (especially benchmark and compute-intensive fibers) never
bind any special variables.  Their binding stack pointer equals their
binding stack start.

The yield and resume paths check for this condition:

```lisp
(when (/= (fiber-binding-stack-pointer fiber)
          (fiber-binding-stack-start fiber))
  ...)
```

When the binding stack is empty, the entire TLS save/restore sequence
is skipped: no hash table operations, no array allocation, no binding
stack walk.  This fast path reduces context switch cost for fibers
that do not use dynamic bindings.

### 6.6 Same-Carrier Resume Optimization

`update-binding-stack-carrier-values` is expensive: it walks the
entire binding stack with a hash table for deduplication.  But it is
only needed when a fiber has migrated between carriers, because the
old-value entries already match the current carrier's TLS if the fiber
hasn't moved.

The scheduler loop computes a migration flag:

```lisp
(let ((migrated (not (eq (fiber-carrier fiber) *current-thread*))))
  ...)
```

This flag is propagated to `resume-fiber-internal`, which guards the
carrier value update:

```lisp
(when migrated
  (update-binding-stack-carrier-values fiber thread-sap scheduler))
```

In a single-carrier configuration or when work stealing is
infrequent, this eliminates the binding stack walk entirely from the
resume path.

## 7. Garbage Collector Integration

### 7.1 The Two-List Design: Suspended Fibers vs. Active Contexts

The GC needs to find all live Lisp objects, including those on fiber
stacks.  Two data structures serve this purpose:

1. **`all_fiber_gc_info`** --- a doubly-linked list of `fiber_gc_info`
   structs, one per fiber that has been created and not yet destroyed.
   Each entry holds the fiber's control stack base/pointer/end and
   binding stack start/pointer.  Used for scanning suspended fibers'
   stacks.

2. **`all_active_fiber_contexts`** --- a doubly-linked list of
   `active_fiber_context` structs, one per carrier thread that is
   currently running a scheduler.  Each entry holds the running
   fiber's stack boundaries and a nested `carrier_gc_info` that
   represents the carrier thread's suspended stack.

During stop-the-world GC, all mutator threads are stopped.  The
collector iterates both lists without locking (all writers are
stopped).  For each suspended fiber, it conservatively scans the
control stack and precisely scavenges the binding stack.  For each
active context, it scans the running fiber's stack and the carrier's
suspended stack.

### 7.2 `fiber_gc_info`: Conservative Control Stack Scanning

Each fiber's `fiber_gc_info` contains:

```c
struct fiber_gc_info {
    lispobj* control_stack_base;     // mmap'd base
    lispobj* control_stack_pointer;  // saved RSP
    lispobj* control_stack_end;      // base + size
    lispobj* binding_stack_start;
    lispobj* binding_stack_pointer;
    struct fiber_gc_info* next;
    struct fiber_gc_info* prev;
    int registered;
};
```

During GC, the collector scans words from `control_stack_pointer` to
`control_stack_end`:

```c
for (ptr = fi->control_stack_pointer; ptr < fi->control_stack_end; ptr++) {
    lispobj word = *ptr;
    if (word >= BACKEND_PAGE_BYTES
        && !(exclude_from <= word && word < exclude_to))
        preserve_pointer(word, 0);
}
```

The scan is conservative: every word that looks like a valid heap
pointer is treated as a potential reference, pinning the pointed-to
object.  The self-reference exclusion (`exclude_from`/`exclude_to`)
prevents the stack's own address range from being treated as heap
pointers.

### 7.3 `active_fiber_context`: Carrier + Fiber Stack Visibility

When a fiber is actively running on a carrier, RSP points into the
fiber's control stack, not the carrier's.  The normal thread scanning
code in `gencgc.c` would miss the carrier's suspended frames (above
the point where `fiber_switch` was called).

The `active_fiber_context` solves this with a nested `carrier_gc_info`
that records the carrier's stack boundaries.  The carrier's
`control_stack_pointer` is set conservatively to
`__builtin_frame_address(0) - 16384`, ensuring that all frames
between the `update_fiber_gc_context` call and the actual suspension
point (inside `fiber_switch`) are included in the scan.

The GC checks for active fiber contexts in two places:

1. **SP validation**: If a thread's interrupt-context SP does not fall
   within the carrier's control stack, the GC checks whether it falls
   within the fiber's stack via `find_active_fiber_context`.

2. **Stack scanning**: If a fiber is active, the GC scans the fiber's
   stack (from SP to `fiber_stack_end`), then separately scans the
   carrier's stack (from `carrier_start + 3*page_size` to
   `carrier_stack_end`), skipping the guard pages.

### 7.4 Precise Binding Stack Scavenging

Unlike control stacks, binding stacks are scavenged precisely.  Each
two-word entry has a known format (old value + TLS index), so the
collector can process them with `scav_binding_stack` rather than
conservative word-by-word scanning.

For the active fiber, BSP points into the fiber's binding stack:

```c
struct active_fiber_context* fctx = find_active_fiber_context(th);
if (fctx && fctx->fiber_binding_stack_start
    && !(bsp >= bs_start && bsp <= bs_start + BINDING_STACK_SIZE)) {
    scav_binding_stack(fctx->fiber_binding_stack_start, bsp, ...);
}
```

For suspended fibers, the collector iterates `all_fiber_gc_info`:

```c
for (fi = all_fiber_gc_info; fi; fi = fi->next) {
    scav_binding_stack(fi->binding_stack_start,
                       fi->binding_stack_pointer, ...);
}
```

### 7.5 Persistent Carrier Context (Lock-Free Hot Path)

The original implementation called `enter_fiber_gc_context` and
`leave_fiber_gc_context` on every resume/yield, acquiring four mutex
locks per context switch (two for `fiber_gc_lock`, two for
`active_fiber_context_lock`).  Under load, these global locks became a
bottleneck.

The optimized implementation uses persistent carrier contexts:

- **`init_carrier_fiber_context()`** --- called once when a carrier
  starts its scheduler.  Allocates the `active_fiber_context`,
  registers the `carrier_gc_info` with empty ranges (pointer = end, so
  GC scan loops execute zero iterations), links into
  `all_active_fiber_contexts`, and stores the pointer in the thread
  struct's `fiber-context` slot.  Two mutex acquisitions total.

- **`update_fiber_gc_context()`** --- called per resume.  Reads the
  context from `thread->fiber_context` (no TLS lookup).  Updates fiber
  stack boundaries and carrier stack scan range.  **Zero mutex locks.**

- **`clear_fiber_gc_context()`** --- called per yield-return.  Sets
  fiber fields to NULL and carrier ranges to empty.  **Zero mutex
  locks.**

- **`destroy_carrier_fiber_context()`** --- called once when a carrier
  exits.  Unregisters from both lists.  Two mutex acquisitions total.

This reduces per-switch mutex overhead from 4 to 0.

### 7.6 The `fiber-context` Thread Struct Slot (O(1) Lookup)

`find_active_fiber_context` is called by the GC for every thread
during stop-the-world.  The original implementation walked the
`all_active_fiber_contexts` linked list to find the entry matching a
given thread --- O(n) in the number of carriers.

The optimized implementation stores the `active_fiber_context` pointer
directly in the thread struct:

```c
struct active_fiber_context*
find_active_fiber_context(struct thread* th) {
    return (struct active_fiber_context*)th->fiber_context;
}
```

This required adding a `fiber-context` slot to the thread struct
definition in `objdef.lisp`:

```lisp
#+sb-fiber
(fiber-context :c-type "void *" :pointer t)
```

The slot is set by `init_carrier_fiber_context` and cleared by
`destroy_carrier_fiber_context`.  For non-carrier threads, it is NULL,
and `find_active_fiber_context` returns NULL (no fiber context to
process).

### 7.7 GC Safety Windows (`without-interrupts` vs. `without-gcing`)

Two SBCL mechanisms control GC timing:

- **`without-interrupts`** --- defers delivery of asynchronous
  signals (including the stop-for-GC signal) but does not prevent
  GC from occurring if triggered by allocation within the body.

- **`without-gcing`** --- prevents GC entirely (sets a flag that
  causes allocation to block until the region exits).

The yield and resume paths use `without-interrupts` for most of their
work.  This prevents the stop-for-GC signal from arriving mid-update,
which could expose partially-written fiber state to the collector.

Specific `without-gcing` regions protect narrow critical sections
where the GC must not fire:

1. **`register_fiber_for_gc` / `unregister_fiber_for_gc`** --- these
   modify the `all_fiber_gc_info` linked list.  If GC fired during
   a list splice, the collector could follow a dangling pointer.

2. **Binding stack pointer update in `gc_info`** --- on yield, the
   fiber's `gc_info` binding stack pointer must be updated before the
   carrier's BSP is restored.  Between these two operations,
   `without-interrupts` alone does not prevent GC (an allocation could
   trigger it), so the `gc_info` write is wrapped in `without-gcing`.

### 7.8 Correctness Argument: Why Partial Updates Are Safe

The persistent carrier context design appears to have a race: the
`update_fiber_gc_context` call writes multiple fields (fiber stack
start, end, binding stack start, carrier stack pointer) without
holding any lock.  Could GC see a half-updated context?

No, because of SBCL's stop-the-world GC protocol.  Before the
collector reads any of these fields, it sends a stop signal to every
mutator thread and waits for all to reach a safepoint.  Once stopped,
no thread is executing `update_fiber_gc_context` or
`clear_fiber_gc_context`.  The fields are therefore always in a
consistent state when the GC reads them.

The only edge case is the empty state: `clear_fiber_gc_context` sets
fiber fields to NULL and carrier ranges to empty.  If GC reads this
state, it sees no fiber stack to scan and an empty carrier range (no
additional scanning).  The carrier's normal thread stack is still
scanned via the standard thread scanning path, so no objects are
missed.

## 8. Scheduler Design

### 8.1 The Scheduler Loop

The scheduler loop (`run-fiber-scheduler`) is the core of each
carrier thread.  Its structure:

```
init-carrier-fiber-context()
loop {
    fiber = fast-path-pop() or (maintenance + pop-or-steal)
    if fiber:
        run-fiber(fiber)
        handle-post-switch(fiber)
    else if has-waiters:
        idle-hook()
    else if group-active:
        park-carrier()
    else:
        return
}
destroy-carrier-fiber-context()
```

The loop runs until either all fibers in the group are dead (the
active count reaches zero) or, in single-carrier mode, the local
deque is empty and no fibers are waiting.

### 8.2 Fast Path: Skip Maintenance When Deque Is Hot

The common case in a busy scheduler is a non-empty deque.  The fast
path tries `wsd-pop` at the top of each iteration.  If it succeeds,
the fiber is run immediately without any maintenance work.  This
avoids the overhead of epoll queries, deadline heap checks, and
waiting list walks when there is plenty of local work.

A maintenance counter tracks how many fibers have been run without
a maintenance pass.  When it reaches 64, a full maintenance cycle is
forced regardless of deque state.  This backstop ensures that I/O
waiters and deadline-based fibers are not starved when the deque is
continuously fed by productive fibers.

### 8.3 Maintenance: Pending Queue Drain, Deadline Expiry, Wake Checks

When the deque is empty (or the backstop fires), the scheduler
performs a full maintenance cycle:

1. **Pending queue drain** --- External threads (e.g., a server's
   accept loop) submit fibers to a scheduler's pending list via
   `atomic-push`.  Maintenance atomically swaps the list to NIL (via
   CAS) and pushes each fiber onto the local deque.

2. **Epoll harvest** (Linux) --- `epoll_wait` with timeout=0 drains
   all pending I/O events into the scheduler's `ready-fds` hash
   table.  `%scheduler-wake-ready-io-waiters` then moves ready
   fibers from the `io-waiters` table to the deque.

3. **Deadline heap expiry** --- `%heap-pop-expired` pops all fibers
   whose deadline has passed (comparing against a single
   `get-internal-real-time` captured once per maintenance cycle) and
   pushes them to the deque.

4. **Waiting list walk** --- Each fiber on the intrusive waiting list
   is checked: if its deadline has passed or its wake condition
   returns true, it is moved to the deque.  Fibers that remain
   waiting are re-linked into a new list.

Note that fiber-aware mutex acquisition (Section 2.8.1) and condition
variable waits (Section 2.8.2) currently use predicate-based parking,
which places the waiting fiber on the generic waiting list.  This
means mutex and condition variable waiters are checked via the O(W)
waiting list walk rather than a targeted wakeup mechanism.  This is
adequate when mutex contention among fibers is low (the typical case
for I/O-heavy server workloads), but a per-mutex fiber wait queue
with explicit wakeup from `release-mutex` would provide O(1) wakeup
and is a planned improvement.

### 8.4 Post-Switch Dispatch (Suspended, Dead)

After `fiber_switch` returns (the fiber yielded or died), the
scheduler examines the fiber's state:

**`:suspended`** --- The fiber yielded.  The scheduler routes it
based on its wait metadata:

- No wake condition, no deadline, no fd → immediately runnable;
  push back to deque.
- Has fd + epoll → index in the `io-waiters` table (and optionally
  the deadline heap if it also has a timeout).
- Has deadline only (e.g., `fiber-sleep`) → insert into deadline
  heap.
- Has deadline + wake condition → insert into both the heap and the
  generic waiting list.
- Has wake condition only → prepend to the generic waiting list.

**`:dead`** --- The fiber's function returned or signaled.  The
scheduler decrements the group's active count (CAS loop) and calls
`destroy-fiber` to free stacks and GC info.

### 8.5 Idle Detection and Carrier Parking

When the deque is empty, no maintenance produced work, and work
stealing failed, the scheduler must wait efficiently.

If **waiters exist** (suspended fibers with conditions or deadlines),
the idle hook is called.  The default idle hook
(`fiber-io-idle-hook`) calls `epoll_wait` with a timeout derived from
the nearest deadline, blocking the carrier thread until either an I/O
event arrives or the timeout expires.

If **no waiters exist** but the group's active count is positive (some
fibers are running on other carriers or pending in their queues), the
carrier **parks** on the group's condition variable with a 1 ms
timeout.  `submit-fiber` calls `condition-notify` on this CV when new
work arrives, waking a parked carrier.

If the **active count is zero**, the carrier exits the scheduler loop.

### 8.6 Maintenance Frequency Backstop (Every 64 Fibers)

The backstop value of 64 is a tuning parameter.  Too small, and
maintenance overhead slows down tight fiber loops.  Too large, and I/O
waiters or deadline timers see increased latency.  64 is a compromise:
at 2 million switches/sec, maintenance runs every ~32 microseconds,
providing sub-millisecond responsiveness for I/O and timers.

## 9. Work Stealing

### 9.1 Chase-Lev Lock-Free Deque

Each scheduler's run queue is a Chase-Lev work-stealing deque, a
data structure designed for exactly this use case: one owner thread
with fast, non-contended access, and multiple thief threads that can
steal work without blocking the owner.

The deque has three fields:

- **`bottom`** --- index of the next slot the owner will push to.
  Only the owner writes this.
- **`top`** --- index of the next slot a thief will steal from.
  Read by everyone; written by thieves via CAS.
- **`buffer`** --- a power-of-two circular array of fiber references.

### 9.2 Owner Operations (Push/Pop from Bottom, LIFO)

The owner pushes to and pops from the bottom:

**Push**: Write the fiber to `buffer[bottom & mask]`, issue a write
barrier, increment `bottom`.  No atomic operations needed because only
the owner writes `bottom`.

**Pop**: Decrement `bottom`, issue a memory barrier, read `top`.  If
`bottom > top`, there are multiple elements and the pop is safe.  If
`bottom == top`, this is the last element and the owner must CAS
`top` to `top+1` to resolve the race with a concurrent steal.  If
`bottom < top`, the deque was empty; restore `bottom` to `top`.

LIFO order (pop from bottom) gives the owner good cache locality: the
most recently pushed fiber is likely still warm in cache.

### 9.3 Thief Operations (Steal from Top, FIFO, CAS)

Thieves steal from the top:

**Steal**: Read `top`, issue a read barrier, read `bottom`.  If
`top < bottom`, there are elements to steal.  Read
`buffer[top & mask]`, then CAS `top` from the read value to
`top+1`.  If the CAS succeeds, the steal is complete.  If it fails
(another thief got there first, or the owner popped the last
element), return nil.

FIFO order (steal from top) gives breadth-first work distribution,
which tends to move larger chunks of work to idle carriers.

### 9.4 Buffer Growth (Power-of-Two Circular Array)

When `bottom - top >= length(buffer)`, the deque is full.  The owner
doubles the buffer by allocating a new array and copying elements.
The circular indexing (`index & (size - 1)`) works correctly for any
power-of-two size, so the copy is straightforward:

```lisp
(loop for i from top below bottom
      do (setf (svref new-buf (logand i new-mask))
               (svref old-buf (logand i old-mask))))
```

Growth is safe because only the owner thread calls `wsd-push` (and
therefore `wsd-grow`).  Thieves operate exclusively on `top` via
CAS, and they index into the buffer using the current buffer
reference and its length.  The owner publishes the new buffer via a
single `setf` of the buffer slot; any thief that reads the old
buffer reference before the swap sees a consistent (if stale) view
and its CAS on `top` will fail or succeed harmlessly.  The old
buffer becomes garbage and is collected by GC.  A write barrier
after each push ensures the new element is visible before `bottom`
is incremented.

Growth is rare in practice because the initial size (64)
accommodates most workloads.

### 9.5 Random Victim Selection

When a carrier's deque is empty and maintenance produced no work, the
scheduler attempts to steal.  `try-steal-fiber` selects victims
randomly:

```lisp
(let ((start (random n)))
  (loop for i from 0 below n
        for idx = (mod (+ start i) n)
        for victim = (aref schedulers idx)
        unless (eq victim scheduler)
          do (let ((stolen (wsd-steal (fiber-scheduler-run-deque victim))))
               (when stolen (return stolen)))))
```

Starting from a random index and wrapping around avoids the
thundering-herd problem where all idle carriers try to steal from
the same victim.  The loop tries each sibling once before giving up.

### 9.6 Fiber Migration and Thread Register Fixup

When a fiber is stolen from carrier A and resumed on carrier B, two
things must be fixed:

1. **Thread register** --- The saved register set contains carrier A's
   thread pointer.  The `fiber_switch` assembly patches this (see
   Section 4.6).

2. **Binding stack carrier values** --- The old-value entries in the
   binding stack reflect carrier A's TLS values.
   `update-binding-stack-carrier-values` rewrites them with carrier
   B's values (see Section 6.3).

These fixups happen during `resume-fiber-internal`, gated by the
migration flag.  After fixup, the fiber runs on carrier B as if it
had always been there.

## 10. I/O Multiplexing

### 10.1 Platform Abstraction (epoll, kqueue, poll Fallback)

The fiber scheduler uses platform-specific I/O multiplexing:

- **Linux**: `epoll` (created via `epoll_create1` at scheduler init)
- **BSD/macOS**: `kqueue` (created via `kqueue()` at scheduler init)
- **Fallback**: batched `poll()` call over all waiting fds

The event multiplexer fd is stored in `fiber-scheduler-event-fd`.
If creation fails (returns -1), the scheduler falls back to the poll
path.

### 10.2 Edge-Triggered Mode with One-Shot (`EPOLLET | EPOLLONESHOT`)

On Linux, fd registrations use edge-triggered mode combined with
one-shot:

```lisp
(let ((events (logior (if (eq direction :input) EPOLLIN EPOLLOUT)
                      EPOLLET EPOLLONESHOT)))
  (epoll-ctl-add efd fd events))
```

**Edge-triggered** (`EPOLLET`) means the kernel delivers an event only
when the fd transitions from not-ready to ready, not continuously
while data is available.  This eliminates the thundering-herd problem:
if multiple fibers wait on related fds (e.g., listening socket and
accepted connections), only the relevant fibers are woken.

**One-shot** (`EPOLLONESHOT`) disables the registration after
delivering one event.  This prevents duplicate wakeups and simplifies
the lifecycle: each `wait-until-fd-usable` call registers, gets one
event, and the registration is consumed.  A subsequent wait must
re-register (via `epoll-ctl-mod` to re-arm).

### 10.3 Indexed I/O Waiters (fd-to-Fiber Table)

When epoll delivers events, the scheduler needs to find which fibers
are waiting on which fds.  The scheduler maintains an `io-waiters`
hash table mapping fd numbers to lists of waiting fibers.

When a fiber parks with an fd wait, `%scheduler-index-io-waiter` adds
it to this table.  When epoll events arrive,
`%scheduler-wake-ready-io-waiters` looks up each ready fd and moves
all waiting fibers for that fd to the run deque.

This O(1) lookup replaces the alternative of walking the entire
waiting list checking each fiber's fd, which would be O(W) where W
is the number of waiting fibers.

### 10.4 Bounded Epoll Drain Loop

`%scheduler-harvest-epoll` calls `epoll_wait` in a loop to drain all
pending events:

```lisp
(loop for ms = timeout-ms then 0
      do (let ((n (epoll-wait efd sap 64 ms)))
           (when (or (null n) (<= n 0)) (return))
           (dotimes (i n) (setf (gethash (aref buf i) ready-fds) t))
           (incf total n)
           (when (< n 64) (return))))
```

The first call uses the provided timeout (allowing the carrier to
block when idle).  Subsequent calls use timeout=0 to drain any
remaining events without blocking.  The loop terminates when
`epoll_wait` returns fewer than 64 events (the buffer size),
indicating no more pending events.

With `EPOLLET | EPOLLONESHOT`, each fd fires at most once, so the
drain naturally terminates.

### 10.5 `fd-ready-p` and `wait-until-fd-usable` Integration

`fd-ready-p` is a non-blocking readiness check using `poll()` with a
zero timeout.  It is used for quick checks before parking and for
timeout=0 calls to `wait-until-fd-usable`.

`%fiber-wait-until-fd-usable` implements the full fiber-aware path:

1. If timeout is zero or negative, return the result of
   `fd-ready-p` immediately (with a fast-path check against the
   scheduler's `ready-fds` hash table on Linux).
2. Set the fiber's `wait-fd` and `wait-direction` fields.
3. Register the fd with epoll/kqueue.
4. If epoll is available: yield with the `%always-false` sentinel
   (the scheduler wakes the fiber via epoll events, not predicate
   polling).  If a deadline is set, also store it for the deadline
   heap.
5. If no epoll: `fiber-park` with a predicate that calls
   `fd-ready-p`.
6. On wake, clear the `wait-fd` field in an `unwind-protect`.

### 10.6 The Default I/O Idle Hook

`fiber-io-idle-hook` is called when the scheduler has no runnable
fibers but has suspended fibers waiting.  It computes a timeout from
the nearest deadline in the heap (or 100 ms as a cap), then:

- On Linux with I/O waiters: calls `%scheduler-harvest-epoll` with
  that timeout, blocking until an event arrives or the timeout
  expires.
- Without I/O waiters: calls `nanosleep` for a brief period (capped
  at 10 ms) to avoid busy-waiting on timer-only waits.

### 10.7 Batched FD Polling vs. Per-Fiber Polling

On platforms without epoll (or when epoll creation fails), the
fallback uses `%batched-fd-poll`, which collects all waiting fibers'
fds into a single `poll()` call.  This is O(W) where W is the number
of I/O waiters, but still far better than calling `poll()` once per
fiber.

## 11. Deadline Scheduling

### 11.1 Binary Min-Heap with Inline Index

The scheduler maintains a binary min-heap ordered by
`fiber-deadline` (an `internal-real-time` value).  The heap is stored
as a `simple-vector` in the scheduler struct, with the count tracked
separately.

Each fiber has a `heap-index` slot (-1 when not in the heap) that
enables O(log N) removal: instead of searching the heap for the
fiber, the scheduler reads the index and sifts from that position.

### 11.2 O(log N) Insert and Remove

**Insert** (`%heap-insert`): Append the fiber at position `n`
(the current count), increment the count, sift up.  If the heap
array is full, double it.

**Remove** (`%heap-remove`): Swap the fiber at position `i` with the
last element, decrement the count, then sift the swapped element up
or down as needed to restore the heap property.

**Sift up**: Compare the fiber's deadline with its parent's; swap if
smaller; repeat until the root or a larger-or-equal parent is found.

**Sift down**: Compare the fiber's deadline with both children's;
swap with the smaller child if needed; repeat until a leaf or no
swap is needed.

### 11.3 Batch Expiry (Pop All Expired in One Pass)

`%heap-pop-expired` pops all fibers whose deadline has passed in a
single loop:

```lisp
(loop while (plusp count)
      for fiber = (svref heap 0)
      while (<= (fiber-deadline fiber) now)
      do (%heap-remove scheduler fiber)
         (setf (fiber-state fiber) :runnable)
         (wsd-push deque fiber))
```

Since the heap is ordered by deadline, the root always has the
earliest deadline.  Once the root's deadline is in the future, no
other fiber can be expired, so the loop terminates.

### 11.4 Interaction with I/O Waiters (Dual-Indexed Fibers)

A fiber waiting on both I/O and a timeout (e.g.,
`wait-until-fd-usable` with a timeout) is indexed in both the
`io-waiters` table and the deadline heap.  Whichever triggers first
wakes the fiber:

- If epoll fires first, `%scheduler-wake-ready-io-waiters` also
  removes the fiber from the deadline heap (via `%heap-remove`).
- If the deadline expires first, `%heap-pop-expired` removes the
  fiber from the `io-waiters` table (via
  `%scheduler-remove-io-waiter`).

This dual-indexing avoids the need for a wrapper closure that checks
both conditions, keeping the wake path allocation-free.

## 12. Fiber Death and Cleanup

### 12.1 The Lisp Trampoline (`fiber-trampoline`)

When a fiber starts for the first time, the entry path is:

```
fiber_switch → fiber_entry_trampoline (asm)
             → fiber_run_and_finish (C)
             → fiber-trampoline (Lisp)
```

`fiber-trampoline` wraps the user function in `unwind-protect` and
`handler-case`:

```lisp
(unwind-protect
     (handler-case
         (setf (fiber-result fiber)
               (multiple-value-list (funcall (fiber-function fiber))))
       (sb-sys:interactive-interrupt (c)
         (error c))  ; propagate Ctrl-C to carrier
       (serious-condition (c)
         (setf (fiber-result fiber) (list c))))
  ;; cleanup forms ...
  )
```

If the user function returns normally, all of its return values are
captured as a list in the fiber's result slot (matching the
`thread-result` convention).  If it signals an unhandled error, the
condition object is stored as a single-element list.  Either way,
execution reaches the `unwind-protect` cleanup.

### 12.2 Error Handling and Result Capture

The `handler-case` around the user function catches all conditions of
type `serious-condition` (which includes `error`, `storage-condition`,
and `sb-ext:timeout`).  This prevents an unhandled condition in a
fiber from killing the carrier thread (which would take down the
scheduler and all other fibers on that carrier).

The one exception is `sb-sys:interactive-interrupt` (Ctrl-C / SIGINT),
which is re-signaled so it propagates to the carrier thread.  This
preserves the user's ability to break into the debugger interactively.

Because the `handler-case` catches conditions without invoking the
debugger, errors in fibers are silently captured rather than
triggering an interactive break.  If interactive debugging is desired,
the user should establish a `handler-bind` with `invoke-debugger`
inside the fiber's function.  Restarts like `abort` work within the
fiber's own catch/block scope.

The captured condition or return values are stored in `fiber-result`
as a list and can be retrieved by `fiber-join` (which returns them via
`values-list`, analogous to `join-thread`) or directly after the fiber
is dead.  When an error is caught, the fiber's `errorp` flag is set to
`T`.  The predicate `fiber-error-p` returns this flag, so callers can
distinguish a fiber that intentionally returned a condition object
from one that signaled an error.

Non-local exits via `throw`, `return-from`, or `go` that target
tags or blocks within the fiber's own function work normally --- the
catch block and `unwind-protect` chains are saved and restored per
fiber (see Section 6.4).  A `throw` to a tag not established within
the fiber will signal a "tag does not exist" error (caught by the
`handler-case`), because the fiber's catch block chain only contains
tags established during the fiber's own execution.  Lexical non-local
exits (`return-from`, `go`) cannot cross fiber boundaries because
blocks and tagbodies are lexically scoped; closures that capture them
from outside the fiber's function would target stale frames, which
is undefined behavior (the same as for threads).

### 12.3 Binding Stack Cleanup on Death (Without `unbind_to_here`)

When a fiber dies with active bindings, its binding stack still
contains entries that reference TLS indices with the fiber's values.
The carrier's TLS must be restored to its pre-fiber state.

The cleanup in `fiber-trampoline` walks the binding stack from start
to the current BSP, using the same deduplication approach as the yield
path: for each unique TLS index, the outermost old-value is written
back to the carrier's TLS array.  This effectively undoes the fiber's
bindings without calling `unbind_to_here` (which would zero the
entries and is designed for the unwind path, not the fiber death
path).

### 12.4 GC Info Unregistration Timing

A fiber's `gc_info` is registered at creation time and must remain
registered until after the last `fiber_switch` returns.  The timing
is critical:

1. During `resume-fiber-internal`, the fiber's `gc_info` is widened to
   cover the full stack (control_stack_pointer = control_stack_start).
   This ensures GC can see the entire fiber stack during the window
   between the widen and `fiber_switch`.

2. After `fiber_switch` returns (the fiber yielded or died), the
   scheduler updates or unregisters the `gc_info`.  For dead fibers,
   `unregister-fiber-gc-roots` removes it from the list.  For live
   fibers, `register-fiber-for-gc-from-fiber` updates the fields
   with the new saved RSP.

Unregistering before `destroy-fiber` frees the `gc_info` struct is
essential to prevent the GC from following a dangling pointer.

### 12.5 Stack Return to Pool

After a fiber dies, `destroy-fiber` returns its stacks to the pools:

1. Remove the fiber from `*all-fibers*` (under mutex).
2. Free the `gc_info` struct.
3. Call `free-fiber-stack` for both the control stack and binding
   stack SAPs.  `free_fiber_stack` routes each to the appropriate
   pool based on the `guard_size` field (non-zero for control stacks,
   zero for binding stacks).

The pool calls `madvise(MADV_DONTNEED)` on the usable region,
releasing physical pages.  The virtual mapping and guard page
protection are preserved for the next user.

## 13. Integration with SBCL

### 13.1 Thread Struct Extension (`fiber-context` Slot)

The SBCL thread struct (defined in `objdef.lisp`) is extended with
three fiber-related slots when `:sb-fiber` is in the feature set:

```lisp
#+sb-fiber (effective-control-stack-start ...)
#+sb-fiber (effective-control-stack-end ...)
#+sb-fiber (fiber-context ...)
```

`effective-control-stack-start` and `effective-control-stack-end` are
set to the running fiber's stack bounds so that stack overflow checks
and `stack-allocated-p` tests use the correct range without
disturbing the GC's use of `control-stack-start`/`control-stack-end`.

`fiber-context` stores a pointer to the carrier's
`active_fiber_context` for O(1) GC lookup (see Section 7.6).

### 13.2 `serve-event` Dispatch (Fiber-Aware `wait-until-fd-usable`)

`sb-sys:wait-until-fd-usable` is defined in `serve-event.lisp`.  When
fibers are enabled, it checks `*current-fiber*` at the top of the
function.  A single special variable check suffices because
`*current-fiber*` is only non-nil inside the scheduler loop where
`*current-scheduler*` is already bound --- the invariant
`*current-fiber*` → `*current-scheduler*` holds by construction:

```lisp
#+sb-fiber
(when *current-fiber*
  (let ((result (%fiber-wait-until-fd-usable fd direction timeout)))
    (unless (eq result :pinned-fall-through)
      (return-from wait-until-fd-usable result))))
```

If the fiber is pinned, the result is `:pinned-fall-through`, and
execution continues to the normal (blocking) path.  Otherwise, the
fiber-aware path handles the wait cooperatively.

### 13.3 `sleep` and `wait-for` Dispatch

`cl:sleep` and `sb-ext:wait-for` are patched similarly.  In fiber
context, they dispatch to `%fiber-sleep` and `%fiber-wait-for`
respectively.  The fiber-aware implementations use `fiber-sleep` and
`fiber-park` under the hood, converting to fiber-compatible
suspension.

### 13.4 Mutex and Condition Variable Dispatch

`sb-thread:grab-mutex` checks for fiber context before entering the
futex-based wait path:

```lisp
#+sb-fiber
(when *current-fiber*
  (let ((result (%fiber-grab-mutex mutex timeout)))
    (unless (eq result :pinned-fall-through)
      (return-from grab-mutex result))))
```

The same pattern applies to `grab-mutex-no-check-deadlock` and
`%condition-wait`.  `condition-notify` increments a per-waitqueue
`fiber-generation` counter that fiber-aware waiters use as their wake
condition.

### 13.5 Pinned Blocking Fallback to OS Primitives

Every fiber-aware dispatch follows the same pattern:

1. Check `fiber-can-yield-p`.  If the fiber is pinned, call
   `check-pinned-blocking` and return `:pinned-fall-through`.
2. The caller sees `:pinned-fall-through` and falls through to the
   normal OS blocking implementation.

This ensures that pinned fibers (e.g., mid-interaction with a
thread-affine C library) still work correctly --- they simply block
the carrier thread instead of yielding.

### 13.6 `*pinned-blocking-action*` Warning/Error Policy

The default value `:warn` causes a warning whenever a pinned fiber
blocks the carrier.  This helps developers identify code that needs
restructuring.  Setting it to `:error` makes pinned blocking a hard
error (useful during development).  Setting it to `nil` silences the
warning (appropriate for code that legitimately pins and blocks, such
as FFI calls that hold foreign resources).

### 13.7 `interrupt-thread`

`interrupt-thread` is not patched for fiber awareness.  If you call
`(sb-thread:interrupt-thread carrier-thread function)`, the function
executes on the carrier thread in whatever context happens to be
active at the time.  If the carrier is running a fiber, the interrupt
function runs with that fiber's dynamic bindings and stack --- not
the carrier's.  This is analogous to delivering a signal to an OS
thread: the handler runs in the interrupted context.

In general, `interrupt-thread` should not be used on carrier threads.
To communicate with a fiber, use fiber-aware primitives: signal a
condition variable, set a flag checked by a `fiber-park` predicate,
or submit a new fiber to the scheduler group.

### 13.8 Debugger Integration

SBCL's standard debugger operates on the current thread's control
stack.  Since fibers have their own control stacks, several debugger
internals are extended for fiber awareness:

- **`control-stack-pointer-valid-p`** checks
  `*debug-control-stack-start/end*` when bound, so frame-walking code
  validates pointers against the fiber's stack rather than the
  carrier's.

- **`with-fiber-debug-bounds`** binds the debug stack bounds to a
  fiber's control stack region, enabling `frame-down` and other
  debugger functions to walk the fiber's frames.

- **`fiber-top-frame`** extracts the frame pointer and return address
  from the fiber's `fiber-switch` save area (architecture-specific
  offsets) and calls `compute-calling-frame` to produce the initial
  `compiled-frame` object.

- **`frame-down`** stops when it encounters `fiber_run_and_finish` in
  the call chain, since that is the absolute bottom of a fiber's
  stack.

- **`print-fiber-backtrace`** combines these pieces: it binds the
  debug bounds, gets the top frame, and delegates to
  `print-backtrace` for full symbolic output.

## 14. Performance

Native Linux threads are highly performant.  With modern kernels, a
thread-per-connection model delivers excellent throughput up to a few
thousand concurrent connections --- there is no need to use fibers if
your workload stays within that range.

Where fibers provide a decisive advantage is at high connection
counts, where the thread-per-connection model hits fundamental
resource limits.  Each OS thread allocates an 8 MB stack by default,
so 10,000 threads require ~80 GB of virtual address space.  Fibers
use 256 KB stacks by default (configurable per fiber via
`:stack-size`), so 10,000 fibers require ~2.5 GB --- a 32x
reduction at the default size.  Beyond the memory wall, the kernel scheduler's
performance degrades with thousands of runnable threads competing for
CPU time, while the fiber scheduler's work-stealing design scales
more gracefully.

### 14.1 HTTP Benchmark

All benchmarks use a Hunchentoot "Hello, World!" HTTP server
measured with `wrk -t4 -cN -d10s` on a 16-core Linux machine.
Thread mode uses Hunchentoot's standard one-thread-per-connection
taskmaster (unlimited threads, 8 GB heap).  Fiber mode uses a
fiber-per-connection taskmaster with carrier count equal to the
number of CPUs (16).

| Connections | Threads (r/s) | Fibers (r/s) | Fiber advantage |
|---|---|---|---|
| 1,000 | 129,724 | 160,947 | 1.24x |
| 2,500 | 89,188 | 142,105 | 1.59x |
| 5,000 | 79,169 | 124,745 | 1.58x |
| 10,000 | 55,493 | 102,710 | 1.85x |
| 25,000 | 62,107 | 83,036 | 1.34x |

At lower connection counts (below ~500), we expect threads to have
an advantage due to the low cost of kernel thread context switching
(see Section 14.4).  The benchmarks above focus on the concurrency
range where the thread-per-connection model begins to degrade and
fibers provide a clear benefit.

Fibers outperform threads at every concurrency level tested.  The
advantage grows with connection count, peaking at nearly 2x at
10,000 connections.

**1,000 connections:** fibers deliver 160,947 r/s versus 129,724 r/s
for threads --- a 1.24x advantage.  Thread performance is already
degrading due to kernel scheduling overhead across 1,000 OS threads.

**2,500-5,000 connections:** fibers sustain 124,745-142,105 r/s
while threads drop to 79,169-89,188 r/s.  The thread-per-connection
model suffers from increasing context switch costs and lock contention
in the Hunchentoot taskmaster.  Threads also begin experiencing
socket timeouts at 2,500 connections.

**10,000+ connections:** fibers maintain over 100,000 r/s at 10,000
connections while threads drop to 55,493 r/s --- a 1.85x advantage.
At 25,000 connections, both models are I/O-bound, but fibers still
lead at 83,036 r/s versus 62,107 r/s.  Fibers maintain the memory
advantage: threads consume ~200 GB of virtual address space while
fibers use ~6.4 GB.

### 14.2 Memory Efficiency

The defining advantage of fibers is memory efficiency at scale:

| Connections | Thread stacks | Fiber stacks | Ratio |
|---|---|---|---|
| 100 | 800 MB | 25 MB | 32x |
| 1,000 | 8 GB | 256 MB | 32x |
| 10,000 | 80 GB | 2.5 GB | 32x |
| 100,000 | 800 GB | 25 GB | 32x |

Thread stacks assume the Linux default of 8 MB; fiber stacks assume
the default of 256 KB (reducible via `:stack-size` at `make-fiber`
time).  Both figures represent virtual address space --- physical
memory usage depends on how many stack pages are touched
(demand-paged).

In practice, the thread-per-connection model becomes infeasible well
before 100,000 connections because the kernel cannot allocate enough
thread stacks.  Fibers handle 100,000 idle connections routinely
(see Section 14.3).

### 14.3 Scalability Under High Connection Counts

The C100K (100,000 idle connections) test verifies that the system
handles large numbers of suspended fibers without degradation.  Each
idle fiber occupies ~256 KB of virtual address space (physical pages
are demand-allocated), a `fiber_gc_info` entry (64 bytes), and a
slot in the epoll interest set.

GC pause times increase linearly with the number of fibers because
each suspended fiber's control stack must be conservatively scanned.
The per-fiber overhead is small for mostly-empty stacks (only a few
live frames to scan), but scales with fiber count.

### 14.4 Context Switch Microbenchmark

A tight yield loop (two fibers yielding back and forth on a single
carrier) measures raw context switch throughput:

| Configuration | Switches/sec | Cost/switch |
|---|---|---|
| Fiber context switch | 2,100,000 | ~0.48 us |
| Kernel thread switch (reference) | 4,000,000-7,000,000 | ~150-250 ns |

A fiber switch is roughly 2-3x slower than a kernel thread context
switch.  The additional cost comes from TLS save/restore
orchestration and GC metadata updates that kernel threads do not
need --- a kernel thread's stack is always at a known location and
its TLS is managed by the OS.

### 14.5 GC Impact

Fibers increase GC work in two ways:

1. **Conservative stack scanning** --- each suspended fiber's stack
   is scanned word-by-word.  Stacks with few live frames have little
   data to scan; deeply recursive fibers contribute more.

2. **Binding stack scavenging** --- each fiber's binding stack is
   precisely scavenged.  This is proportional to the number of active
   bindings, which is typically small.

The persistent carrier context (Section 7.6) avoids mutex contention
during stop-the-world collection --- carriers do not need to acquire
locks to update their GC metadata on every switch.

## 15. Platform Support

### 15.1 x86-64 (Linux, macOS, Windows)

The primary development platform.  All features are available:

- `fiber_switch`: 6 callee-saved GPRs (SysV), 8 GPRs + 10 XMMs
  (Win64)
- I/O: `epoll` on Linux, `kqueue` on macOS, `poll` fallback on
  Windows
- Thread register: `r13` (patched on migration)
- Stack pool: `madvise(MADV_DONTNEED)` on Linux/macOS,
  `VirtualAlloc`/`VirtualFree` on Windows

### 15.2 ARM64

Full support with 160-byte register save frame.  Uses `stp`/`ldp`
instructions for paired register saves (ARM64's most efficient
store/load pattern).  Thread register is `x21`.

I/O multiplexing depends on the OS: `epoll` on Linux, `kqueue` on
iOS/macOS (via the BSD layer).

### 15.3 ARM32

Full support with 104-byte register save frame.  Saves VFP registers
`d8`-`d15` (64 bytes) and GPRs `r3`-`r11`, `lr` (40 bytes).  Thread
register is `r10`.  Uses `blx` for indirect calls.

### 15.4 PPC64

Full support with 320-byte register save frame.  Saves LR, CR, 18
callee-saved GPRs (`r14`-`r31`), and 18 callee-saved FPRs
(`f14`-`f31`).  Thread register is `r30`.  Frame includes backchain
pointer for ABI compliance.

### 15.5 PPC32

Full support with 240-byte register save frame.  Same register set as
PPC64 but with 4-byte GPRs: LR, CR, `r14`-`r31` (72 bytes),
`f14`-`f31` (144 bytes).  Thread register is `r17`
(`thread-base-tn`).

### 15.6 RISC-V (RV64)

Full support with 208-byte register save frame.  Saves `ra`, `cfp`
(for debugger backtraces), `s0`-`s11` (12 GPRs), and `fs0`-`fs11`
(12 FPRs).  Thread register is `s10` (`r26`).  Uses `sd`/`ld` for
GPRs and `fsd`/`fld` for FPRs.

### 15.7 Feature Flag: `:sb-fiber`

Fiber support is controlled by the `:sb-fiber` feature.  When absent,
all fiber code is excluded via `#+sb-fiber` reader conditionals.  The
thread struct does not include fiber slots, the dispatcher hooks in
`grab-mutex`/`sleep`/`wait-until-fd-usable` are not compiled, and
the `fiber.c`/`fiber.h` runtime files are not linked.

To enable fibers, add `:sb-fiber` to
`local-target-features.lisp-expr` before building SBCL.

### 15.8 Platform-Specific Assembly and I/O Backends

Each supported architecture has its own assembly file:

| Platform | Assembly file | Save frame |
|---|---|---|
| x86-64 | `src/assembly/x86-64/fiber.lisp` | 48-224 bytes |
| ARM64 | `src/assembly/arm64/fiber.lisp` | 160 bytes |
| ARM32 | `src/assembly/arm/fiber.lisp` | 104 bytes |
| PPC64 | `src/assembly/ppc64/fiber.lisp` | 320 bytes |
| PPC32 | `src/assembly/ppc/fiber.lisp` | 240 bytes |
| RISC-V | `src/assembly/riscv/fiber.lisp` | 208 bytes |

I/O backends:

| Platform | Backend | Registration mode |
|---|---|---|
| Linux | `epoll` | `EPOLLET \| EPOLLONESHOT` |
| BSD/macOS | `kqueue` | `EV_ADD \| EV_ONESHOT` |
| Other | `poll()` | Batched per maintenance |

## Appendix A: Using Hunchentoot with Fibers

Hunchentoot's standard `one-thread-per-connection-taskmaster` creates an
OS thread for every accepted connection.  A fiber-based taskmaster
replaces those threads with fibers: one fiber per connection, all
multiplexed across a fixed set of carrier threads.  Because SBCL fibers
make `sleep`, `grab-mutex`, `condition-wait`, and `wait-until-fd-usable`
transparently fiber-aware, Hunchentoot's request processing code works
without modification --- only the taskmaster needs to change.

### A.1 The Fiber Taskmaster

```lisp
(defclass fiber-taskmaster (hunchentoot:taskmaster)
  ((group :accessor fiber-taskmaster-group
          :initform nil
          :documentation "The fiber scheduler group.")
   (carrier-count :initarg :carrier-count
                  :initform nil
                  :accessor fiber-taskmaster-carrier-count
                  :documentation "Number of carrier threads.
NIL means use the number of available CPUs."))
  (:documentation "A Hunchentoot taskmaster that runs each connection
in a fiber instead of a dedicated OS thread."))
```

### A.2 Taskmaster Methods

```lisp
(defmethod hunchentoot:execute-acceptor ((taskmaster fiber-taskmaster))
  "Start the fiber scheduler group, then run the accept loop on a
carrier fiber so that the accept() call itself is fiber-aware."
  (let* ((acceptor (hunchentoot:taskmaster-acceptor taskmaster))
         (accept-fiber
           (sb-thread:make-fiber
             (lambda () (hunchentoot:accept-connections acceptor))
             :name (format nil "hunchentoot-acceptor-~A:~A"
                           (or (hunchentoot:acceptor-address acceptor) "*")
                           (hunchentoot:acceptor-port acceptor)))))
    (setf (fiber-taskmaster-group taskmaster)
          (sb-thread:start-fibers
            (list accept-fiber)
            :carrier-count (fiber-taskmaster-carrier-count taskmaster)))))

(defmethod hunchentoot:handle-incoming-connection
    ((taskmaster fiber-taskmaster) socket)
  "Submit a new fiber to process this connection."
  (let ((acceptor (hunchentoot:taskmaster-acceptor taskmaster)))
    (sb-thread:submit-fiber
      (fiber-taskmaster-group taskmaster)
      (sb-thread:make-fiber
        (lambda ()
          (hunchentoot:process-connection acceptor socket))
        :name "hunchentoot-worker"))))

(defmethod hunchentoot:shutdown ((taskmaster fiber-taskmaster))
  "Shut down the fiber scheduler group."
  (when (fiber-taskmaster-group taskmaster)
    (sb-thread:finish-fibers (fiber-taskmaster-group taskmaster)))
  taskmaster)
```

### A.3 Starting the Server

```lisp
;; Load Hunchentoot (via Quicklisp, OCICL, or ASDF)
(ql:quickload :hunchentoot)  ; or (asdf:load-system :hunchentoot)

;; Define a handler
(hunchentoot:define-easy-handler (hello :uri "/") ()
  (format nil "Hello from fiber ~A on carrier ~A!"
          (sb-thread:fiber-name sb-thread:*current-fiber*)
          (sb-thread:thread-name sb-thread::*current-thread*)))

;; Start with the fiber taskmaster
(defvar *server*
  (hunchentoot:start
    (make-instance 'hunchentoot:easy-acceptor
                   :port 8080
                   :taskmaster (make-instance 'fiber-taskmaster
                                              :carrier-count 4))))
```

To stop:

```lisp
(hunchentoot:stop *server*)
```

### A.4 How It Works

1. `execute-acceptor` creates a scheduler group with the specified
   number of carrier threads and submits the accept loop as the
   initial fiber.  The accept loop calls `usocket:wait-for-input`
   and `usocket:socket-accept` --- both perform I/O that goes through
   `wait-until-fd-usable`, which yields the fiber cooperatively.

2. For each accepted connection, `handle-incoming-connection` creates
   a fiber and submits it to the scheduler group.  The fiber runs
   `process-connection`, which reads the HTTP request, calls the
   handler, and writes the response.  Every blocking operation
   (socket reads/writes, mutex grabs, sleeps) yields the fiber
   instead of blocking the carrier thread.

3. Work stealing distributes fibers across carriers automatically.
   If one carrier is busy with a compute-heavy handler while another
   is idle, the idle carrier steals fibers from the busy one.

4. When the acceptor shuts down, `finish-fibers` joins all carrier
   threads and cleans up.

### A.5 SSL

Hunchentoot's `ssl-acceptor` uses `cl+ssl` (OpenSSL) for TLS.
OpenSSL maintains per-connection state in thread-local storage, so a
fiber must not migrate to a different carrier mid-handshake.  Pin the
fiber during SSL operations:

```lisp
(defmethod hunchentoot:initialize-connection-stream
    ((acceptor hunchentoot:ssl-acceptor) stream)
  ;; Pin to prevent work-stealing during SSL handshake
  (sb-thread:with-fiber-pinned ()
    (call-next-method)))
```

In practice, single-carrier setups (`(make-instance 'fiber-taskmaster
:carrier-count 1)`) avoid the migration issue entirely and are a
simpler starting point when using SSL.  For multi-carrier SSL, pinning
is only needed during the handshake --- once the TLS session is
established, the `cl+ssl` stream wrapper uses ordinary fd reads/writes
that are carrier-safe.

**Downsides of pinning.** A pinned fiber cannot be stolen by another
carrier.  If the TLS handshake blocks on I/O (network round-trips for
certificate exchange, key agreement), the pinned fiber falls through
to OS-level blocking rather than yielding cooperatively (see Section
2.9).  This blocks the carrier thread entirely, preventing it from
running other fibers.  Under a burst of new TLS connections, all
carriers can end up blocked in handshakes simultaneously, stalling
the entire scheduler.  Pinning also defeats load balancing: work
stealing cannot redistribute pinned fibers, so carriers become
unevenly loaded.

**Alternative: pure-TLS.** [pure-tls](https://github.com/puercopop/pure-tls)
is a TLS 1.3 implementation written entirely in Common Lisp, with no
foreign (C/OpenSSL) dependencies.  Because it uses only Lisp-level
I/O and data structures, there is no thread-local foreign state ---
fibers using pure-tls can migrate freely between carriers without
pinning.  This preserves cooperative yielding during the handshake
(the fiber yields on socket reads/writes instead of blocking the
carrier) and allows work stealing to balance TLS connections across
all cores.  For fiber-based servers handling many concurrent TLS
connections, pure-tls eliminates the pinning bottleneck entirely.

### A.6 Considerations

**Session locking.** Hunchentoot's session store uses a global lock.
Under fibers, `grab-mutex` yields cooperatively, so there is no
deadlock risk, but high-contention sessions may cause unnecessary
yielding.  For high-throughput servers, consider a lock-free session
store or per-session locks.

**Timeouts.** Hunchentoot sets socket-level read/write timeouts via
`set-timeouts`.  These are OS-level `SO_RCVTIMEO`/`SO_SNDTIMEO`
timeouts that work at the fd level, independent of whether the caller
is a thread or fiber.  They continue to function correctly.

**Logging.** Hunchentoot's logging functions are thread-safe and work
unchanged under fibers.  The `*acceptor*` and `*request*` special
variables are bound per-fiber via the normal dynamic binding mechanism,
which fibers fully support (see Section 6).

**Max connections.** The thread-per-connection taskmaster limits
concurrency via `max-thread-count`.  The fiber taskmaster shown above
has no such limit --- fibers are cheap enough that tens of thousands
of concurrent connections are practical.  If you need to limit
concurrent connections (e.g., to bound memory), add a semaphore:

```lisp
(defvar *connection-semaphore* (sb-thread:make-semaphore :count 10000))

(defmethod hunchentoot:handle-incoming-connection
    ((taskmaster fiber-taskmaster) socket)
  (let ((acceptor (hunchentoot:taskmaster-acceptor taskmaster)))
    (sb-thread:submit-fiber
      (fiber-taskmaster-group taskmaster)
      (sb-thread:make-fiber
        (lambda ()
          (sb-thread:wait-on-semaphore *connection-semaphore*)
          (unwind-protect
               (hunchentoot:process-connection acceptor socket)
            (sb-thread:signal-semaphore *connection-semaphore*)))
        :name "hunchentoot-worker"))))