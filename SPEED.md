# Fiber Performance Investigation Notes

Last updated: February 28, 2026
Repo: `/home/green/sbcl-fibers`
Benchmark app: `/home/green/fiber-benchmark`

## Scope

This document summarizes what was investigated about poor fiber performance, what was measured, what was changed so far, and what to do next.

Primary symptom: fiber mode performs far worse than thread mode at very high connection counts (especially `-c10000`).

## Environment and baseline setup

Commands used to ensure a fresh runtime:

```bash
cd /home/green/sbcl-fibers
sh clean.sh && sh make.sh
sh install.sh --prefix=/home/green/.local
/home/green/.local/bin/sbcl --version
```

Observed version:

```text
SBCL 2.6.1.225.fibers
```

Machine: 16 CPUs, Linux 6.18.13

Benchmark environment assumptions used in most runs:

- `PATH` includes `/home/green/.local/bin` so benchmark uses freshly built SBCL
- `ulimit -n 524288`
- Kernel tunables for high-concurrency testing:
  - `net.core.somaxconn = 65535`
  - `net.ipv4.tcp_max_syn_backlog = 65535`
  - `net.ipv4.ip_local_port_range = 1024 65535`

## What was investigated

## 1) Sanity checks and benchmark validity

1. Confirmed benchmark was using the intended SBCL binary from `~/.local/bin`.
2. Confirmed default carrier count path is used (no forced carrier-count arg), so `start-fibers` picks carriers from CPU/cgroup.
3. Verified fiber benchmark server starts with 17 carriers on this machine (16 CPU + listener thread observed in logging context).
4. Verified `ulimit -n` can strongly affect high-concurrency behavior; low limits can produce misleading failures.

## 2) Code-path analysis in `src/code/fiber.lisp`

Focused on scheduler and I/O wait paths:

- `run-fiber-scheduler` wait handling loop
- `%fiber-wait-until-fd-usable`
- event registration (`%event-register` / `%event-deregister`)
- idle hook (`fiber-io-idle-hook`, `compute-nearest-deadline`)

Observed design issues:

1. Generic wait handling scans intrusive waiting list every scheduler cycle.
2. Additional full scans happen in idle/deadline code paths.
3. I/O wait path still triggers large `poll()` volume despite epoll usage.

## 3) Syscall-level profiling

Used `strace -f -qq -c` for fibers under high load.

Representative command:

```bash
strace -f -qq -c -e trace=poll,epoll_wait,epoll_ctl \
  /home/green/.local/bin/sbcl --dynamic-space-size 4096 --load server.lisp fibers 4250
```

Representative result under `wrk -t4 -c10000 -d4s` (before EPOLLET):

- `poll`: ~78.84% time, 128876 calls
- `epoll_wait`: ~18.74% time, 33337 calls
- `epoll_ctl`: ~2.43% time, 3129 calls

Interpretation: hot path is still poll-heavy; epoll is active but not dominant.

## Code changes made

### Phase 1: Indexed I/O waiters

File changed: `src/code/fiber.lisp`

High-level change: introduced a Linux epoll-indexed I/O waiter map for no-timeout FD waiters so they avoid generic waiting-list scans.

Added:

1. New fiber slot:
   - `io-indexed-p`
2. New scheduler slots:
   - `io-waiters` (fd -> list of fibers)
   - `io-waiter-count`
3. New helpers:
   - `%scheduler-index-io-waiter`
   - `%scheduler-wake-ready-io-waiters`
   - `scheduler-has-waiters-p`
4. Scheduler integration:
   - wake indexed I/O waiters from epoll-ready fds each tick
   - place suspended no-timeout FD waiters into indexed map instead of generic waiting list
   - idle/loop waiter checks include indexed waiters

### Phase 2: Deadline min-heap

Replaced linear deadline scans with a min-heap for O(log n) insert/remove and O(1) nearest-deadline lookup.

### Phase 3: Edge-triggered epoll (EPOLLET | EPOLLONESHOT)

Root cause of the c=10000 livelock: **level-triggered epoll** re-reported all ready fds every `epoll_wait` call, causing a drain loop to spin (previously infinite, then capped at 16 iterations). The `ready-fds` hash table was rebuilt every tick.

Fix: switched to `EPOLLET | EPOLLONESHOT`.

Files changed:
- `tools-for-build/grovel-headers.c` — added `EPOLLET` and `EPOLLONESHOT` constants
- `src/cold/exports.lisp` — exported the new constants from `SB-UNIX`
- `src/code/fiber.lisp`:
  - `%event-register`: events mask now includes `EPOLLET | EPOLLONESHOT` via `logior`; removed early-return on `(= current events)` since EPOLLONESHOT requires re-arming every time via `EPOLL_CTL_MOD`
  - `%event-deregister`: remaining-events check now uses `(logand remaining (logior epollin epollout))` to ignore flag bits when deciding DEL vs MOD
  - `%scheduler-harvest-epoll`: replaced drain loop (up to 16 iterations) with single `epoll_wait` call — with edge-triggered + one-shot, each fd fires at most once per arm cycle

## Benchmark results

### Before all changes (baseline)

Full sweep from `run-benchmark.sh`:

| Concurrency | Threads r/s | Fibers r/s |
|-------------|-------------|------------|
| c=10 | 88,542 | 51,433 |
| c=50 | 209,574 | 173,933 |
| c=100 | 208,207 | 163,571 |
| c=250 | 188,527 | 132,888 |
| c=500 | 163,982 | 128,647 |
| c=1000 | 106,762 | 114,342 |
| c=2500 | 98,669 | 101,763 |
| c=5000 | 81,777 | 77,830 |
| c=10000 | 77,428 | 3,219 |

Fibers competitive up to ~5k connections, then collapse at 10k (livelock).

### After Phase 2 (min-heap) — before EPOLLET

| Concurrency | Threads r/s | Fibers r/s |
|-------------|-------------|------------|
| c=10000 | ~84,000 | ~11,000 |

Min-heap fixed the infinite livelock (0 r/s → 11k r/s) but fibers still far behind threads.

### After Phase 3 (EPOLLET) — current results

Tested with `wrk -t4 -cN -d10s --latency http://127.0.0.1:4242/`

Includes Java 21+ virtual threads (`com.sun.net.httpserver.HttpServer` + `newVirtualThreadPerTaskExecutor()`) as a reference point.

**c=100:**

| Runtime | r/s | p50 latency | Socket errors |
|---------|-----|-------------|---------------|
| Threads | 183,897 | 354us | 0 |
| Fibers/4 | 98,687 | 66us | 0 |
| Fibers/16 | 137,613 | 286us | 0 |

**c=1000:**

| Runtime | r/s | p50 latency | Socket errors |
|---------|-----|-------------|---------------|
| Threads | 108,608 | 3.03ms | 0 |
| Fibers/4 | 95,374 | 62us | 0 |
| Fibers/16 | 108,972 | 3.22ms | 0 |

**c=10000:**

| Runtime | r/s | p50 latency | Socket errors |
|---------|-----|-------------|---------------|
| Java vthread | 175,610 | 1.06ms | 40k read + 1.2k timeout |
| Fibers/4 | 86,560 | 62us | 125 timeout only |
| Threads | 61,942 | 0.89ms | 7k read + 208k write + 249 timeout |
| Fibers/16 | 11,541 | — | 12k read + 1.5M write |

Key observations at c=10000:

- **Fibers/4 went from 3,219 r/s (baseline) → 11,000 (min-heap) → 86,560 (EPOLLET)** — 27x total improvement
- **Fibers/4 are the cleanest runtime**: zero socket errors, only 125 timeouts
- **Fibers/4 beat threads** by 1.4x (86k vs 62k)
- **Java virtual threads lead** at 176k r/s but with 40k socket errors (aggressive connection handling)
- **Fibers/16 regresses severely** at c=10000 (12k r/s) — carrier contention on work-stealing deques

**Scaling characteristics:**

| Runtime | c=100 | c=1000 | c=10000 | Degradation 100→10000 |
|---------|-------|--------|---------|----------------------|
| Threads | 183,897 | 108,608 | 61,942 | 3.0x drop |
| Fibers/4 | 98,687 | 95,374 | 86,560 | 1.1x drop (nearly flat) |
| Java vthread | — | — | 175,610 | — |

Fibers/4 scale nearly flat across 100x increase in concurrency. Threads degrade 3x.

## Architecture notes

- Each carrier thread gets its own `fiber-scheduler` with its own epoll fd (one epoll per carrier, not shared)
- EPOLLONESHOT auto-disables an fd after one event, preventing races when fibers migrate between carriers via work-stealing
- Re-arm via `EPOLL_CTL_MOD` happens in `%event-register` before the fiber parks again
- SBCL's stream code already satisfies the EAGAIN contract: it only calls `wait-until-fd-usable` after `unix-read` returns EAGAIN, so any new data triggers a fresh edge

## Remaining issues and next steps

1. **Carrier count scaling**: fibers/16 regresses badly at high concurrency. Investigate work-stealing contention. Consider Go's approach: GOMAXPROCS = CPU count, but dynamically adjust active Ps based on workload.

2. **Gap vs Java**: Java virtual threads do 2x fibers/4 at c=10000 (176k vs 87k). Contributing factors:
   - Java's `HttpServer` is a mature, optimized HTTP stack vs Hunchentoot's CLOS-heavy dispatch
   - Java has no socket errors but trades correctness for throughput (40k read errors)
   - JIT warmup advantage

3. **Low-concurrency gap**: at c=100, threads (184k) beat fibers/4 (99k) by ~2x. Fiber scheduler overhead doesn't pay off until connection counts are high.

4. **Remaining `poll()` calls**: investigate whether residual `poll(0)` calls on the readiness-check path can be eliminated now that EPOLLET is authoritative.

## Reproduction guide

### Build and install

```bash
cd /home/green/sbcl-fibers
sh clean.sh && sh make.sh
sh install.sh --prefix=/home/green/.local
/home/green/.local/bin/sbcl --version
```

### Kernel tuning (for high-concurrency tests)

```bash
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

### Benchmark

```bash
cd /home/green/fiber-benchmark

# Threads
/home/green/.local/bin/sbcl --load server.lisp threads 4242
# Fibers (4 carriers)
/home/green/.local/bin/sbcl --load server.lisp fibers 4242 4
# Fibers (all CPUs)
/home/green/.local/bin/sbcl --load server.lisp fibers 4242

# In another shell:
wrk -t4 -c10000 -d10s --latency http://127.0.0.1:4242/
```

### Java virtual threads comparison

```bash
cd /home/green/fiber-benchmark
javac VThreadServer.java
java VThreadServer 4242
# In another shell:
wrk -t4 -c10000 -d10s --latency http://127.0.0.1:4242/
```
