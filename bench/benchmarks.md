# pg_rest vs pg_net benchmarks

For pg_rest running in pg_net's own CI loadtest harness, see
[pg_net_loadtest/comparison.md](pg_net_loadtest/comparison.md).

## Setup

- **Machine:** one Apple Silicon laptop running macOS. The database, the workers, the benchmark
  client and the mock server all ran on it.
- **Postgres:** 18.6, the build from `cargo pgrx init`. It has assertions enabled, so absolute
  numbers are pessimistic; comparisons between runs are fair.
- **Extensions:** pg_rest as a release build, and pg_net 0.20.4 with default settings (batch size 200) unless stated otherwise. Both are preloaded in the same cluster and send requests to the
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

Run-to-run noise: pg_rest's no-slow-request throughput varied between about 32k and 48k req/s across runs. Sections 1–5 used the Python mock server. Replacing it with a Rust one barely changed the numbers (see "Mock server: Python vs Rust" below), so the ceiling is in pg_rest, not in the mock server.

## 1. Default configurations

pg_net as shipped (it pauses 1 s after every batch) against pg_rest's defaults.

| slow | ext     |   total |  req/s | fast req/s |    p50 |     p99 | fast p99 | max xact |
| ---- | ------- | ------: | -----: | ---------: | -----: | ------: | -------: | -------: |
| 0%   | pg_net  |  50.1 s |    200 |        200 | 25.6 s |  50.1 s |   50.1 s |   0.02 s |
| 0%   | pg_rest |  0.22 s | 45,325 |     48,050 | 0.13 s |  0.21 s |   0.21 s |        0 |
| 0.1% | pg_net  |  70.1 s |    143 |        143 | 35.6 s |  70.1 s |   70.1 s |   2.02 s |
| 0.1% | pg_rest |  2.28 s |  4,388 |     47,123 | 0.14 s |  0.21 s |   0.21 s |        0 |
| 1%   | pg_net  | 150.0 s |     67 |         66 | 77.5 s | 150.0 s |  150.0 s |   2.02 s |
| 1%   | pg_rest |  2.32 s |  4,312 |     36,454 | 0.19 s |  0.27 s |   0.27 s |        0 |
| 10%  | pg_net  | 150.0 s |     67 |         67 | 77.5 s | 149.9 s |  134.8 s |   2.02 s |
| 10%  | pg_rest |  4.13 s |  2,422 |      4,377 | 0.18 s |  2.29 s |   1.46 s |        0 |

CPU per 1,000 requests:

| scenario               | pg_net | pg_rest |
| ---------------------- | -----: | ------: |
| idle (share of a core) |   0.0% |    0.2% |
| burst, 0% slow         | 107 ms |   58 ms |
| burst, 1% slow         | 116 ms |   65 ms |
| steady 100 req/s       | 137 ms |  348 ms |

## 2. pg_net without its pause

pg_net patched so that its 1 s pause between batches is 0 s. It still processes interrupts
between batches, and still waits for a wake-up when the queue is empty. pg_rest runs its
unmodified defaults.

| slow | ext     |   total |  req/s | fast req/s |    p50 |     p99 | fast p99 | max xact |
| ---- | ------- | ------: | -----: | ---------: | -----: | ------: | -------: | -------: |
| 0%   | pg_net  |  0.59 s | 16,819 |     17,180 | 0.32 s |  0.58 s |   0.58 s |   0.01 s |
| 0%   | pg_rest |  0.32 s | 31,695 |     32,800 | 0.20 s |  0.30 s |   0.30 s |        0 |
| 0.1% | pg_net  |  20.7 s |    483 |        483 | 10.3 s |  20.7 s |   20.7 s |   2.01 s |
| 0.1% | pg_rest |  2.28 s |  4,392 |     45,983 | 0.13 s |  0.22 s |   0.22 s |        0 |
| 1%   | pg_net  | 101.0 s |     99 |         98 | 52.6 s | 101.0 s |  101.0 s |   2.02 s |
| 1%   | pg_rest |  2.33 s |  4,293 |     36,057 | 0.19 s |  0.27 s |   0.27 s |        0 |
| 10%  | pg_net  | 101.0 s |     99 |         99 | 52.5 s | 101.0 s |   90.9 s |   2.02 s |
| 10%  | pg_rest |  4.12 s |  2,425 |      4,299 | 0.18 s |  2.31 s |   1.44 s |        0 |

CPU per 1,000 requests:

| scenario               | pg_net (no pause) | pg_rest |
| ---------------------- | ----------------: | ------: |
| idle (share of a core) |              0.0% |    0.2% |
| burst, 0% slow         |             53 ms |   46 ms |
| burst, 1% slow         |            114 ms |   56 ms |
| steady 100 req/s       |            358 ms |  389 ms |

## 3. pg_rest with a pg_net-style pause

These runs used a temporary patch that is not part of pg_rest. The worker pauses for a fixed time
after every transaction, as pg_net does after every batch. Each transaction then commits every
response collected since the previous one, instead of a bucket of at most 100.

- **1 s, 1,000 in flight:** pg_net's pause with pg_rest's default pipeline depth.
- **1 s, 200 in flight:** pg_net's pause and pg_net's batch size. The remaining difference is
  pipelining versus batching.
- **50 ms, 1,000 in flight:** a much shorter pause.

pg_net is shown with its default settings (1 s pause, batch size 200).

| slow | variant                        |   total |  req/s | fast req/s |    p50 |     p99 | fast p99 | max xact |
| ---- | ------------------------------ | ------: | -----: | ---------: | -----: | ------: | -------: | -------: |
| 0%   | pg_net                         |  50.1 s |    200 |        200 | 25.6 s |  50.1 s |   50.1 s |   0.02 s |
| 0%   | pg_rest 1 s, 1,000 in flight   |  11.2 s |    892 |        893 | 7.11 s |  11.2 s |   11.2 s |   0.02 s |
| 0%   | pg_rest 1 s, 200 in flight     |  51.5 s |    194 |        194 | 27.3 s |  51.5 s |   51.5 s |   0.01 s |
| 0%   | pg_rest 50 ms, 1,000 in flight |  0.70 s | 14,356 |     14,672 | 0.43 s |  0.68 s |   0.68 s |   0.01 s |
| 0.1% | pg_net                         |  70.1 s |    143 |        143 | 35.6 s |  70.1 s |   70.1 s |   2.02 s |
| 0.1% | pg_rest 1 s, 1,000 in flight   |  12.8 s |    780 |        924 | 6.71 s |  10.8 s |   10.8 s |   0.02 s |
| 0.1% | pg_rest 1 s, 200 in flight     |  53.1 s |    189 |        196 | 26.8 s |  51.0 s |   51.0 s |   0.01 s |
| 0.1% | pg_rest 50 ms, 1,000 in flight |  2.75 s |  3,641 |     13,614 | 0.43 s |  0.69 s |   0.69 s |   0.01 s |
| 1%   | pg_net                         | 150.0 s |     67 |         66 | 77.5 s | 150.0 s |  150.0 s |   2.02 s |
| 1%   | pg_rest 1 s, 1,000 in flight   |  13.8 s |    725 |        841 | 6.69 s |  11.8 s |   10.8 s |   0.03 s |
| 1%   | pg_rest 1 s, 200 in flight     |  53.0 s |    189 |        194 | 26.8 s |  51.0 s |   51.0 s |   0.01 s |
| 1%   | pg_rest 50 ms, 1,000 in flight |  2.76 s |  3,628 |     13,345 | 0.41 s |  0.74 s |   0.74 s |   0.01 s |
| 10%  | pg_net                         | 150.0 s |     67 |         67 | 77.5 s | 149.9 s |  134.8 s |   2.02 s |
| 10%  | pg_rest 1 s, 1,000 in flight   |  13.8 s |    726 |        838 | 6.66 s |  11.8 s |   10.7 s |   0.03 s |
| 10%  | pg_rest 1 s, 200 in flight     |  57.0 s |    175 |        177 | 28.8 s |  56.0 s |   51.0 s |   0.01 s |
| 10%  | pg_rest 50 ms, 1,000 in flight |  4.18 s |  2,393 |      4,268 | 0.53 s |  3.32 s |   2.11 s |   0.01 s |

CPU per 1,000 requests:

| scenario               | pg_net | pg_rest 1 s, 1,000 in flight | pg_rest 1 s, 200 in flight | pg_rest 50 ms, 1,000 in flight |
| ---------------------- | -----: | ---------------------------: | -------------------------: | -----------------------------: |
| idle (share of a core) |   0.0% |                         0.1% |                       0.1% |                           0.2% |
| burst, 0% slow         | 107 ms |                        71 ms |                      85 ms |                          52 ms |
| burst, 1% slow         | 116 ms |                        65 ms |                     106 ms |                          55 ms |
| steady 100 req/s       | 137 ms |                       127 ms |                     129 ms |                         384 ms |

## 4. Logged vs unlogged tables

pg_rest's tables are now regular logged tables. Sections 1–3 used the earlier unlogged tables.
Both modes below ran the same build, back to back: first logged, then after
`ALTER TABLE … SET UNLOGGED` on both tables. WAL settings: `synchronous_commit = on`,
`wal_sync_method = open_datasync`, `full_page_writes = on`, `wal_level = replica`.

| slow | tables   |  total |  req/s | fast req/s |    p50 |    p99 | fast p99 | max xact |
| ---- | -------- | -----: | -----: | ---------: | -----: | -----: | -------: | -------: |
| 0%   | logged   | 0.32 s | 31,334 |     32,450 | 0.21 s | 0.31 s |   0.31 s |        0 |
| 0%   | unlogged | 0.24 s | 42,535 |     44,927 | 0.13 s | 0.22 s |   0.22 s |        0 |
| 0.1% | logged   | 2.29 s |  4,365 |     37,072 | 0.18 s | 0.27 s |   0.27 s |        0 |
| 0.1% | unlogged | 2.28 s |  4,383 |     38,226 | 0.12 s | 0.21 s |   0.21 s |        0 |
| 1%   | logged   | 2.24 s |  4,456 |     45,534 | 0.13 s | 0.22 s |   0.22 s |        0 |
| 1%   | unlogged | 2.24 s |  4,462 |     38,359 | 0.12 s | 0.26 s |   0.21 s |        0 |
| 10%  | logged   | 4.11 s |  2,434 |      4,431 | 0.12 s | 2.24 s |   1.37 s |        0 |
| 10%  | unlogged | 4.10 s |  2,438 |      4,440 | 0.12 s | 2.24 s |   1.38 s |        0 |

The same 10k burst with no slow requests, run once more per mode right after a checkpoint:

| tables   |  total |  req/s |       WAL written (enqueue + worker) |
| -------- | -----: | -----: | -----------------------------------: |
| logged   | 0.23 s | 43,384 | 8.4 MB (about 860 bytes per request) |
| unlogged | 0.22 s | 45,901 |                                11 kB |

CPU per 1,000 requests:

| scenario               | logged | unlogged |
| ---------------------- | -----: | -------: |
| idle (share of a core) |   0.0% |     0.1% |
| burst, 0% slow         |  47 ms |    45 ms |
| burst, 1% slow         |  50 ms |    48 ms |
| steady 100 req/s       | 334 ms |   363 ms |

**On this machine, WAL logging has no measurable effect on throughput, latency or worker CPU.**
The differences between the two modes are within run-to-run noise: the 0% row differs in the main
table, but the repeat run after a checkpoint shows 0.23 s vs 0.22 s. WAL is written and flushed
by Postgres processes other than the worker (the committing backend, the WAL writer), so the
worker CPU column doesn't include it.

This machine is a best case for WAL. The worker commits about once per 100 responses, so a burst
of 10k requests causes only about 100–200 WAL flushes, and a laptop SSD flushes fast. The cost
shows up elsewhere:

- **Slow flushes.** On disks with ~1–5 ms flushes (network block storage, for example), each
  worker commit waits that long with `synchronous_commit = on`. At 100–200 commits per 10k
  requests, that adds roughly 0.1–1 s per 10k requests.
- **WAL volume.** About 860 bytes per request, which replicas, backups and WAL archiving all
  have to handle.

Logged tables mean requests and responses survive a crash, and they are replicated to standbys.

**Planner bug found and fixed while running this.** The first unlogged run, right after
`SET UNLOGGED` had rewritten the tables with empty statistics, had one transaction open for
13.9 s. The claim query joined the queue to a `LIMIT … FOR UPDATE SKIP LOCKED` subquery. With a
1-row estimate, the planner chose a nested loop that re-ran the locking subquery for every queue
row: 14.2 s for a 10k-row queue, and more rows claimed than the limit allowed. The same can
happen in production when autovacuum records an empty queue just before a burst. The claim and
TTL queries now use `id = any(array(subquery))` and `ctid = any(array(subquery))`. The subquery
runs once, and the update is an index or TID lookup whatever the estimates: 4.3 ms on the same
data. The numbers in this section are from the fixed build; sections 1–3 used the old queries,
which had fresh statistics at the time.

## 5. Response bucket size: 100 vs 1

The default (`RESPONSE_BUCKET_SIZE = 100`, with a 50 ms deadline) is compared against a build
that commits every response in its own transaction (`RESPONSE_BUCKET_SIZE = 1`, a temporary
change, reverted afterwards). Everything else is at its default: 1,000 requests in flight and
logged tables. Both ran back to back on a freshly created extension.

| slow | bucket size |  total |  req/s | fast req/s |    p50 |    p99 | fast p99 | max xact |
| ---- | ----------: | -----: | -----: | ---------: | -----: | -----: | -------: | -------: |
| 0%   |         100 | 0.28 s | 35,899 |     37,588 | 0.18 s | 0.27 s |   0.27 s |        0 |
| 0%   |           1 | 3.93 s |  2,546 |      2,554 | 2.04 s | 3.89 s |   3.89 s |        0 |
| 0.1% |         100 | 2.27 s |  4,397 |     48,894 | 0.13 s | 0.20 s |   0.20 s |        0 |
| 0.1% |           1 | 5.83 s |  1,714 |      2,440 | 2.14 s | 4.07 s |   4.07 s |        0 |
| 1%   |         100 | 2.24 s |  4,471 |     49,384 | 0.12 s | 0.20 s |   0.20 s |        0 |
| 1%   |           1 | 5.93 s |  1,685 |      2,372 | 2.17 s | 4.17 s |   4.15 s |        0 |
| 10%  |         100 | 4.10 s |  2,437 |      4,424 | 0.13 s | 2.24 s |   1.40 s |        0 |
| 10%  |           1 | 6.04 s |  1,655 |      2,239 | 2.24 s | 5.57 s |   3.97 s |        0 |

One 10k burst with no slow requests, measured right after a checkpoint:

| bucket size |  total | commits in the database | WAL written |
| ----------: | -----: | ----------------------: | ----------: |
|         100 | 0.27 s |                     166 |      8.4 MB |
|           1 | 4.36 s |                  10,597 |     10.1 MB |

"Commits in the database" counts all backends, including the enqueueing session. The worker
accounts for almost all of them.

CPU per 1,000 requests:

| scenario               | bucket size 100 | bucket size 1 |
| ---------------------- | --------------: | ------------: |
| idle (share of a core) |            0.1% |          0.3% |
| burst, 0% slow         |           47 ms |        411 ms |
| burst, 1% slow         |           46 ms |        438 ms |
| steady 100 req/s       |          442 ms |        868 ms |

**Batching commits matters about as much as pipelining.** Committing each response on its own:

- raises the commit count 64× (166 → 10,597 per 10k requests);
- makes the worker 14× slower when nothing is slow (0.28 s → 3.93 s), and cuts fast-request
  throughput about 20× when there are slow requests;
- raises median latency from about 0.13 s to about 2 s;
- costs about 9× more CPU per request on bursts (47 → 411 ms per 1k), and 2× more at a steady
  100 req/s.

The worker's single Postgres thread becomes the bottleneck: it was busy 94% of the time during
the no-slow burst, doing little besides running one transaction per response. WAL grows by only
20% (8.4 → 10.1 MB), because most WAL is the row data itself rather than commit records. Per-row
commits cost CPU and latency, not WAL volume.

Even with a bucket of 1, pg_rest is 13× faster than pg_net with default settings on 10k fast
requests (3.9 s vs 50.1 s), and never holds a transaction open. Pipelining and the missing 1 s
pause still help. But a large share of the headroom comes from committing responses in batches.

The baseline's steady-rate CPU (442 ms per 1k) is higher than in earlier sections (334–389 ms).
The steady scenario varies noticeably between runs; compare the two columns of this table rather
than figures across sections.

## Mock server: Python vs Rust

`mock_server/` is a Rust port of `tests/mock_server.py`, with the same endpoints including the
deliberately malformed ones. The full pytest suite passes against it (81/81). Both servers ran
on `main`, back to back, on the same cluster and build.

| slow | mock server |  total |  req/s | fast req/s |    p50 | fast p99 |
| ---- | ----------- | -----: | -----: | ---------: | -----: | -------: |
| 0%   | Python      | 0.26 s | 38,614 |     40,574 | 0.17 s |   0.25 s |
| 0%   | Rust        | 0.23 s | 43,387 |     45,879 | 0.13 s |   0.22 s |
| 0.1% | Python      | 2.26 s |  4,432 |     50,328 | 0.12 s |   0.20 s |
| 0.1% | Rust        | 2.21 s |  4,518 |     48,831 | 0.13 s |   0.20 s |
| 1%   | Python      | 2.25 s |  4,448 |     48,371 | 0.13 s |   0.20 s |
| 1%   | Rust        | 2.25 s |  4,447 |     48,417 | 0.13 s |   0.20 s |
| 10%  | Python      | 4.09 s |  2,446 |      4,423 | 0.12 s |   1.44 s |
| 10%  | Rust        | 4.08 s |  2,448 |      4,420 | 0.12 s |   1.39 s |

pg_rest's worker CPU per 1k requests was the same with both servers: 43–46 ms on bursts and
406–439 ms at a steady 100 req/s.

pg_net against the Rust server was also unchanged:

| slow |   total | req/s | CPU per 1k requests |
| ---- | ------: | ----: | ------------------: |
| 0%   |  50.4 s |   199 |      119 ms (burst) |
| 0.1% |  70.3 s |   142 |                   – |
| 1%   | 150.7 s |    66 |      125 ms (burst) |
| 10%  | 150.7 s |    66 |                   – |

At a steady 100 req/s, pg_net used 142 ms per 1k requests. Its numbers are set by its 1 s pause
and its batching, not by the server.

**The mock server was not the bottleneck.** The Rust server made fast bursts about 12% faster
(0.26 → 0.23 s) and changed nothing else. During 40k-request bursts:

|  HTTP_WORKER_THREADS |   throughput | pg_rest worker CPU |         mock server CPU |
| -------------------: | -----------: | -----------------: | ----------------------: |
|          2 (default) | 50–53k req/s |    ~245% of a core | ~90% of one of 12 cores |
| 4 (temporary change) |   ~47k req/s |           280–310% |                125–140% |

- **The limit is inside pg_rest's request path.** With two tokio threads plus its main thread,
  the worker uses about 2.5 cores.
- **More HTTP threads don't help.** Twice the tokio threads used more CPU for slightly less
  throughput, so their number isn't the limit.
- **Likely candidates:** the worker's single main thread, which spawns every request and handles
  every response, or lock contention in reqwest/hyper's per-host connection pool. More threads
  making it slower fits the latter.
- **Postgres commits aren't the limit either:** the `in-memory-tables` branch, which commits nothing,
  hits about the same ceiling.
- **Not profiled.** Pinning it down would need a profiler (e.g. Instruments or `samply`) on the
  worker.

## Committing the bucket early when the pipeline is full

Sampling the queue during a 10%-slow run (see the `in-memory-tables` branch's section 7) showed a
stall.

- **The stall.** Once most of the 1,000 slots were held by slow requests, the few slots still
  cycling produced too few responses to fill a bucket of 100. Each round then waited out the
  50 ms `RESPONSE_BUCKET_MAX_WAIT` before committing and freeing its slots.
- **The fix.** The worker now also commits the bucket immediately when every slot is taken and
  requests are waiting to be claimed. In that state, the bucketed responses are exactly what's
  blocking new claims. When there are free slots, batching is unchanged.

Both builds are from `main`, run back to back with the Rust mock server. "Minimum" is the
fastest the pipeline could possibly drain: slow requests × 2 s ÷ 1,000 slots, or 2 s if that's
less.

| slow | build        |       total | fast req/s |    p50 |   fast p99 | minimum |
| ---- | ------------ | ----------: | ---------: | -----: | ---------: | ------: |
| 0%   | before       |      0.23 s |     45,530 | 0.14 s |     0.22 s |       – |
| 0%   | eager commit |      0.23 s |     45,432 | 0.13 s |     0.22 s |       – |
| 1%   | before       |      2.25 s |     49,531 | 0.12 s |     0.20 s |     2 s |
| 1%   | eager commit |      2.24 s |     47,325 | 0.12 s |     0.21 s |     2 s |
| 10%  | before       |      4.11 s |      4,413 | 0.12 s |     1.38 s |     2 s |
| 10%  | eager commit |  **2.26 s** | **38,559** | 0.12 s | **0.21 s** |     2 s |
| 20%  | before       |      5.86 s |      3,773 | 2.07 s |     2.12 s |     4 s |
| 20%  | eager commit |  **4.18 s** |      3,782 | 2.06 s |     2.12 s |     4 s |
| 50%  | before       |     11.04 s |      1,052 | 6.07 s |     4.37 s |    10 s |
| 50%  | eager commit | **10.19 s** |      1,218 | 6.03 s |     4.09 s |    10 s |

| CPU per 1k requests    | before | eager commit |
| ---------------------- | -----: | -----------: |
| burst, 0% slow         |  45 ms |        46 ms |
| burst, 1% slow         |  47 ms |        48 ms |
| burst, 10% slow        |  68 ms |        55 ms |
| steady 100 req/s       | 423 ms |       416 ms |
| idle (share of a core) |   0.3% |         0.3% |

**It works.**

- **10% slow:** 1.8× faster (4.11 → 2.26 s), and the fast requests stop waiting (p99
  1.38 → 0.21 s). That ties the `in-memory-tables` branch (2.21 s), so its 10%-slow advantage was
  entirely the bucket deadline, not the absence of tables.
- **20% and 50% slow:** `main` now runs within about 5% of the minimum.
- **Nothing else changes.** The fix only acts when the pipeline is full, so fast-only and
  steady-rate runs are unchanged. CPU on the 10%-slow burst went down, because the run is shorter.
- **Tests:** the pytest suite (81) and the `#[pg_test]`s pass.

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

**Committing responses in buckets matters as much as pipelining.** With a bucket of 1, one
transaction per response, pg_rest is 14× slower on fast bursts and uses 9× more CPU per request
(section 5).

**A 50 ms pause does not reduce steady-rate CPU at 100 req/s.** The steady benchmark enqueues a
batch every 100 ms. A 50 ms pause is shorter than that gap, so pg_rest still commits about twice
per batch: once to claim it and once to retire its responses. A pause only merges work that
arrives within it. Its main effect here was a higher p50 on bursts (0.13 s → 0.43 s). To cut
steady-rate CPU, the pause has to be at least as long as the gap between enqueues, and that
costs up to that much latency. For example, a 1 s pause means up to about 1 s of extra latency.
