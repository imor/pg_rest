# pg_rest vs pg_net benchmarks

## Setup

- **Machine:** one Apple Silicon laptop running macOS. The database, the workers, the benchmark
  client and the mock server all ran on it.
- **Postgres:** 18.6, the build from `cargo pgrx init`. It has assertions enabled, so absolute
  numbers are pessimistic; comparisons between runs are fair.
- **Extensions:** pg_rest as a release build, and pg_net 0.20.4 with default settings (batch size
  200) unless stated otherwise. Both are preloaded in the same cluster and send requests to the
  same mock server (`tests/mock_server.py`).
- **Latency and throughput:** `bench/bench.py`. Each run enqueues 10,000 GETs in one transaction.
  A fraction of them go to an endpoint that takes 2 s ("slow"), the rest to one that answers
  immediately.
- **CPU:** `bench/cpu_bench.py`. It measures user + system CPU time of the worker process,
  including pg_rest's tokio threads. It does not count the cost of enqueueing, which is the same
  for both extensions.
  - **idle:** 30 s with nothing to do.
  - **burst:** 10,000 requests enqueued at once, measured until every response is visible.
  - **steady:** 100 req/s for 30 s, a rate both extensions can sustain.

Columns:

- **total:** time from the enqueue commit until the last response is visible.
- **fast req/s / fast p99:** throughput and p99 latency of the fast requests alone (the earliest
  N − slow completions).
- **p50:** median latency from the enqueue commit until the response is visible.
- **max xact:** the longest time the worker had a transaction open.

Every run completed all 10,000 requests with zero errors.

Run-to-run noise: pg_rest's no-slow-request throughput varied between about 32k and 48k req/s across runs. At that rate the single-threaded Python mock server is probably the bottleneck, so treat those numbers as a lower bound.

## 1. Default configurations

pg_net as shipped (it pauses 1 s after every batch) against pg_rest's defaults.

| slow | ext | total | req/s | fast req/s | p50 | p99 | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 0% | pg_net | 50.1 s | 200 | 200 | 25.6 s | 50.1 s | 50.1 s | 0.02 s |
| 0% | pg_rest | 0.22 s | 45,325 | 48,050 | 0.13 s | 0.21 s | 0.21 s | 0 |
| 0.1% | pg_net | 70.1 s | 143 | 143 | 35.6 s | 70.1 s | 70.1 s | 2.02 s |
| 0.1% | pg_rest | 2.28 s | 4,388 | 47,123 | 0.14 s | 0.21 s | 0.21 s | 0 |
| 1% | pg_net | 150.0 s | 67 | 66 | 77.5 s | 150.0 s | 150.0 s | 2.02 s |
| 1% | pg_rest | 2.32 s | 4,312 | 36,454 | 0.19 s | 0.27 s | 0.27 s | 0 |
| 10% | pg_net | 150.0 s | 67 | 67 | 77.5 s | 149.9 s | 134.8 s | 2.02 s |
| 10% | pg_rest | 4.13 s | 2,422 | 4,377 | 0.18 s | 2.29 s | 1.46 s | 0 |

CPU per 1,000 requests:

| scenario | pg_net | pg_rest |
|---|---:|---:|
| idle (share of a core) | 0.0% | 0.2% |
| burst, 0% slow | 107 ms | 58 ms |
| burst, 1% slow | 116 ms | 65 ms |
| steady 100 req/s | 137 ms | 348 ms |

## 2. pg_net without its pause

pg_net patched so that its 1 s pause between batches is 0 s. It still processes interrupts
between batches, and still waits for a wake-up when the queue is empty. pg_rest runs its
unmodified defaults.

| slow | ext | total | req/s | fast req/s | p50 | p99 | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 0% | pg_net | 0.59 s | 16,819 | 17,180 | 0.32 s | 0.58 s | 0.58 s | 0.01 s |
| 0% | pg_rest | 0.32 s | 31,695 | 32,800 | 0.20 s | 0.30 s | 0.30 s | 0 |
| 0.1% | pg_net | 20.7 s | 483 | 483 | 10.3 s | 20.7 s | 20.7 s | 2.01 s |
| 0.1% | pg_rest | 2.28 s | 4,392 | 45,983 | 0.13 s | 0.22 s | 0.22 s | 0 |
| 1% | pg_net | 101.0 s | 99 | 98 | 52.6 s | 101.0 s | 101.0 s | 2.02 s |
| 1% | pg_rest | 2.33 s | 4,293 | 36,057 | 0.19 s | 0.27 s | 0.27 s | 0 |
| 10% | pg_net | 101.0 s | 99 | 99 | 52.5 s | 101.0 s | 90.9 s | 2.02 s |
| 10% | pg_rest | 4.12 s | 2,425 | 4,299 | 0.18 s | 2.31 s | 1.44 s | 0 |

CPU per 1,000 requests:

| scenario | pg_net (no pause) | pg_rest |
|---|---:|---:|
| idle (share of a core) | 0.0% | 0.2% |
| burst, 0% slow | 53 ms | 46 ms |
| burst, 1% slow | 114 ms | 56 ms |
| steady 100 req/s | 358 ms | 389 ms |

## 3. pg_rest with a pg_net-style pause

These runs used a temporary patch that is not part of pg_rest. The worker pauses for a fixed time
after every transaction, as pg_net does after every batch. Each transaction then commits every
response collected since the previous one, instead of a bucket of at most 100.

- **1 s, 1,000 in flight:** pg_net's pause with pg_rest's default pipeline depth.
- **1 s, 200 in flight:** pg_net's pause and pg_net's batch size. The remaining difference is
  pipelining versus batching.
- **50 ms, 1,000 in flight:** a much shorter pause.

pg_net is shown with its default settings (1 s pause, batch size 200).

| slow | variant | total | req/s | fast req/s | p50 | p99 | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 0% | pg_net | 50.1 s | 200 | 200 | 25.6 s | 50.1 s | 50.1 s | 0.02 s |
| 0% | pg_rest 1 s, 1,000 in flight | 11.2 s | 892 | 893 | 7.11 s | 11.2 s | 11.2 s | 0.02 s |
| 0% | pg_rest 1 s, 200 in flight | 51.5 s | 194 | 194 | 27.3 s | 51.5 s | 51.5 s | 0.01 s |
| 0% | pg_rest 50 ms, 1,000 in flight | 0.70 s | 14,356 | 14,672 | 0.43 s | 0.68 s | 0.68 s | 0.01 s |
| 0.1% | pg_net | 70.1 s | 143 | 143 | 35.6 s | 70.1 s | 70.1 s | 2.02 s |
| 0.1% | pg_rest 1 s, 1,000 in flight | 12.8 s | 780 | 924 | 6.71 s | 10.8 s | 10.8 s | 0.02 s |
| 0.1% | pg_rest 1 s, 200 in flight | 53.1 s | 189 | 196 | 26.8 s | 51.0 s | 51.0 s | 0.01 s |
| 0.1% | pg_rest 50 ms, 1,000 in flight | 2.75 s | 3,641 | 13,614 | 0.43 s | 0.69 s | 0.69 s | 0.01 s |
| 1% | pg_net | 150.0 s | 67 | 66 | 77.5 s | 150.0 s | 150.0 s | 2.02 s |
| 1% | pg_rest 1 s, 1,000 in flight | 13.8 s | 725 | 841 | 6.69 s | 11.8 s | 10.8 s | 0.03 s |
| 1% | pg_rest 1 s, 200 in flight | 53.0 s | 189 | 194 | 26.8 s | 51.0 s | 51.0 s | 0.01 s |
| 1% | pg_rest 50 ms, 1,000 in flight | 2.76 s | 3,628 | 13,345 | 0.41 s | 0.74 s | 0.74 s | 0.01 s |
| 10% | pg_net | 150.0 s | 67 | 67 | 77.5 s | 149.9 s | 134.8 s | 2.02 s |
| 10% | pg_rest 1 s, 1,000 in flight | 13.8 s | 726 | 838 | 6.66 s | 11.8 s | 10.7 s | 0.03 s |
| 10% | pg_rest 1 s, 200 in flight | 57.0 s | 175 | 177 | 28.8 s | 56.0 s | 51.0 s | 0.01 s |
| 10% | pg_rest 50 ms, 1,000 in flight | 4.18 s | 2,393 | 4,268 | 0.53 s | 3.32 s | 2.11 s | 0.01 s |

CPU per 1,000 requests:

| scenario | pg_net | pg_rest 1 s, 1,000 in flight | pg_rest 1 s, 200 in flight | pg_rest 50 ms, 1,000 in flight |
|---|---:|---:|---:|---:|
| idle (share of a core) | 0.0% | 0.1% | 0.1% | 0.2% |
| burst, 0% slow | 107 ms | 71 ms | 85 ms | 52 ms |
| burst, 1% slow | 116 ms | 65 ms | 106 ms | 55 ms |
| steady 100 req/s | 137 ms | 127 ms | 129 ms | 384 ms |

## Takeaways

**Pipelining beats batching even without pg_net's pause.** Section 2 removes pg_net's 1 s pause.
pg_rest is then about 2× faster when nothing is slow. With slow requests the gap is 9× at 0.1%
slow and 43× at 1% slow, because a pg_net batch commits only after its slowest request. pg_net
also keeps a transaction open for the whole 2 s of every batch that contains a slow request. pg_rest
never holds a transaction open for noticeable time.

**pg_net's pause is what makes it cheap at low steady rates.** Without the pause, pg_net's
steady-rate CPU (358 ms per 1,000 requests) is about the same as pg_rest's (389 ms). With the
1 s pause, both extensions drop to about 130 ms. Both workers cost roughly the same per
transaction, so CPU at low rates is set by how often they commit.

**With the same pause and the same batch size, pipelining still wins with slow requests.** This
is the "1 s, 200 in flight" row in section 3. It matches pg_net with no slow requests (51.5 s vs
50.1 s), and is 2.6–2.8× faster with 1–10% slow requests (53–57 s vs 150 s).

**A 50 ms pause does not reduce steady-rate CPU at 100 req/s.** The steady benchmark enqueues a
batch every 100 ms. A 50 ms pause is shorter than that gap, so pg_rest still commits about twice
per batch: once to claim it and once to retire its responses. A pause only merges work that
arrives within it. Its main effect here was a higher p50 on bursts (0.13 s → 0.43 s). To cut
steady-rate CPU, the pause has to be at least as long as the gap between enqueues, and that
costs up to that much latency. For example, a 1 s pause means up to about 1 s of extra latency.

