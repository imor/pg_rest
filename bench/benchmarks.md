# pg_rest vs pg_net benchmarks

For pg_rest running in pg_net's own CI loadtest harness, see
[pg_net_loadtest/comparison.md](pg_net_loadtest/comparison.md).

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

## 4. Logged vs unlogged tables

pg_rest's tables are now regular logged tables. Sections 1–3 used the earlier unlogged tables.
Both modes below ran the same build, back to back: first logged, then after
`ALTER TABLE … SET UNLOGGED` on both tables. WAL settings: `synchronous_commit = on`,
`wal_sync_method = open_datasync`, `full_page_writes = on`, `wal_level = replica`.

| slow | tables | total | req/s | fast req/s | p50 | p99 | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 0% | logged | 0.32 s | 31,334 | 32,450 | 0.21 s | 0.31 s | 0.31 s | 0 |
| 0% | unlogged | 0.24 s | 42,535 | 44,927 | 0.13 s | 0.22 s | 0.22 s | 0 |
| 0.1% | logged | 2.29 s | 4,365 | 37,072 | 0.18 s | 0.27 s | 0.27 s | 0 |
| 0.1% | unlogged | 2.28 s | 4,383 | 38,226 | 0.12 s | 0.21 s | 0.21 s | 0 |
| 1% | logged | 2.24 s | 4,456 | 45,534 | 0.13 s | 0.22 s | 0.22 s | 0 |
| 1% | unlogged | 2.24 s | 4,462 | 38,359 | 0.12 s | 0.26 s | 0.21 s | 0 |
| 10% | logged | 4.11 s | 2,434 | 4,431 | 0.12 s | 2.24 s | 1.37 s | 0 |
| 10% | unlogged | 4.10 s | 2,438 | 4,440 | 0.12 s | 2.24 s | 1.38 s | 0 |

The same 10k burst with no slow requests, run once more per mode right after a checkpoint:

| tables | total | req/s | WAL written (enqueue + worker) |
|---|---:|---:|---:|
| logged | 0.23 s | 43,384 | 8.4 MB (about 860 bytes per request) |
| unlogged | 0.22 s | 45,901 | 11 kB |

CPU per 1,000 requests:

| scenario | logged | unlogged |
|---|---:|---:|
| idle (share of a core) | 0.0% | 0.1% |
| burst, 0% slow | 47 ms | 45 ms |
| burst, 1% slow | 50 ms | 48 ms |
| steady 100 req/s | 334 ms | 363 ms |

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

| slow | bucket size | total | req/s | fast req/s | p50 | p99 | fast p99 | max xact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 100 | 0.28 s | 35,899 | 37,588 | 0.18 s | 0.27 s | 0.27 s | 0 |
| 0% | 1 | 3.93 s | 2,546 | 2,554 | 2.04 s | 3.89 s | 3.89 s | 0 |
| 0.1% | 100 | 2.27 s | 4,397 | 48,894 | 0.13 s | 0.20 s | 0.20 s | 0 |
| 0.1% | 1 | 5.83 s | 1,714 | 2,440 | 2.14 s | 4.07 s | 4.07 s | 0 |
| 1% | 100 | 2.24 s | 4,471 | 49,384 | 0.12 s | 0.20 s | 0.20 s | 0 |
| 1% | 1 | 5.93 s | 1,685 | 2,372 | 2.17 s | 4.17 s | 4.15 s | 0 |
| 10% | 100 | 4.10 s | 2,437 | 4,424 | 0.13 s | 2.24 s | 1.40 s | 0 |
| 10% | 1 | 6.04 s | 1,655 | 2,239 | 2.24 s | 5.57 s | 3.97 s | 0 |

One 10k burst with no slow requests, measured right after a checkpoint:

| bucket size | total | commits in the database | WAL written |
|---:|---:|---:|---:|
| 100 | 0.27 s | 166 | 8.4 MB |
| 1 | 4.36 s | 10,597 | 10.1 MB |

"Commits in the database" counts all backends, including the enqueueing session. The worker
accounts for almost all of them.

CPU per 1,000 requests:

| scenario | bucket size 100 | bucket size 1 |
|---|---:|---:|
| idle (share of a core) | 0.1% | 0.3% |
| burst, 0% slow | 47 ms | 411 ms |
| burst, 1% slow | 46 ms | 438 ms |
| steady 100 req/s | 442 ms | 868 ms |

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

## 6. Subtransaction per bucket, with a row-by-row fallback

The design proposed in review, on branch `subxact-retire`:

1. Each bucket is stored in a subtransaction.
2. If storing it fails, for example because a user trigger on `_http_response` raises, the
   subtransaction is rolled back and the bucket's responses are stored one at a time, each in its
   own subtransaction.
3. A response that still fails on its own is dropped with a WARNING, and its request is deleted,
   so it isn't re-sent forever.
4. A query cancel (`pg_cancel_backend`) is re-raised rather than treated as a bad row, as
   PL/pgSQL's `WHEN OTHERS` does.

On `main`, any error while storing a bucket ends the worker, and the postmaster restarts it.

### Normal path (nothing fails)

| slow | build | total | fast req/s | p50 | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|
| 0% | main | 0.27 s | 39,544 | 0.17 s | 0.25 s | 0.01 s |
| 0% | subxact-retire | 0.27 s | 38,942 | 0.18 s | 0.26 s | 0.01 s |
| 1% | main | 2.25 s | 49,514 | 0.12 s | 0.20 s | 0 |
| 1% | subxact-retire | 2.25 s | 48,406 | 0.13 s | 0.20 s | 0 |
| 10% | main | 4.10 s | 4,422 | 0.12 s | 1.37 s | 0 |
| 10% | subxact-retire | 4.10 s | 4,443 | 0.12 s | 1.42 s | 0 |

| CPU per 1k requests | main | subxact-retire |
|---|---:|---:|
| burst, 0% slow | 44 ms | 44 ms |
| burst, 1% slow | 48 ms | 47 ms |

**The subtransaction costs nothing measurable on the normal path**: one extra subtransaction per
bucket of 100.

### Failure path

`bench/failure_bench.py` adds a `BEFORE INSERT` trigger on `rest._http_response` that raises for
every request id divisible by N. It then enqueues 10,000 requests and waits up to 60 s for the
queue to drain.

- **none:** no trigger.
- **0:** the trigger, which never raises. This isolates the trigger's own cost.

| failing rows | build | queue drained | time | responses stored | worker restarts | CPU per 1k |
|---|---|---|---:|---:|---:|---:|
| none | main | yes | 0.21 s | 10,000 | 0 | 46 ms |
| none | subxact-retire | yes | 0.20 s | 10,000 | 0 | 45 ms |
| trigger, 0 failing | main | yes | 0.23 s | 10,000 | 0 | 48 ms |
| trigger, 0 failing | subxact-retire | yes | 0.23 s | 10,000 | 0 | 50 ms |
| 1 in 1,000 (10 rows) | main | **no** (60 s) | – | 1,300 | **56** | – |
| 1 in 1,000 (10 rows) | subxact-retire | yes | 0.44 s | 9,990 | 0 | 69 ms |
| 1 in 100 (100 rows) | main | **no** (60 s) | – | 100 | **56** | – |
| 1 in 100 (100 rows) | subxact-retire | yes | 2.22 s | 9,900 | 0 | 248 ms |
| 1 in 10 (1,000 rows) | main | **no** (60 s) | – | 0 | **56** | – |
| 1 in 10 (1,000 rows) | subxact-retire | yes | 2.16 s | 9,000 | 0 | 240 ms |

**What `main` does is worse than "crash and replay".** The failing bucket aborts, the worker exits,
and the restarted worker re-sends every claimed request. A bucket containing a bad row fails again
every time, so the worker restarts about once a second and the queue never drains. In 60 s, at
most 1,300 of the 10,000 responses were stored, and every request was sent to the remote server
56 times.

**With the subtransaction, every good response is stored and the worker keeps running.** A
bucket that has to be retried costs about 20 ms: a 100-row bucket stored one row at a time.

- **1 bad row per 1,000:** 10 buckets were retried, and the run took 0.44 s instead of 0.23 s.
- **1 per 100, or 1 per 10:** every bucket was retried, and the run took about 2.2 s. That is
  about the cost of committing every response separately (section 5: 3.9 s), a little cheaper
  because subtransactions are lighter than transactions.

Things to know about this design:

- **Subtransaction overflow.** A bucket retried row by row runs up to about 100 subtransactions
  that write, inside one transaction. Postgres caches 64 subtransaction XIDs per backend
  (`PGPROC_MAX_CACHED_SUBXIDS`). Past that, the transaction's subtransaction list overflows, and
  until it commits, every other session's snapshot visibility checks have to consult
  `pg_subtrans`. That lasts only for one retry transaction of about 20 ms, but a table that makes
  every bucket fail causes it constantly.
  - Mitigations: retry by bisection instead of one row at a time. Split a failing bucket in
    half, recursively; one bad row in 100 then needs about 14 subtransactions instead of 100,
    and the retry gets cheaper too. Or cap the subtransactions per transaction below 64 by
    committing the retry in chunks.
- **What happens to a row that fails on its own is a policy choice.** Here it is dropped with a
  WARNING, and its request deleted. Alternatives: keep the request and retry later, which risks a
  loop if the failure is permanent, or move it to a dead-letter table.
- **Toolchain issue (unrelated to the design).** This machine's Xcode linker (ld-27037)
  sometimes produced a release dylib that macOS refused to load ("mis-aligned LINKEDIT string
  pool"). Which build was affected depended on the exact binary, not on the code. `main` was
  measured with the repo's release profile (fat LTO) and `subxact-retire` with thin LTO, the
  profiles that linked correctly for each. The normal-path numbers match `main`'s earlier fat-LTO
  runs (section 5's baseline), so the LTO difference doesn't show up here.

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

**Store each bucket in a subtransaction.** It costs nothing on the normal path. Without it, one
bad row (e.g. a raising trigger) puts the worker in a crash loop that never drains the queue and
re-sends every request about once a second. With it, only the bad rows are lost, and a retried
bucket costs about 20 ms (section 6).

**A 50 ms pause does not reduce steady-rate CPU at 100 req/s.** The steady benchmark enqueues a
batch every 100 ms. A 50 ms pause is shorter than that gap, so pg_rest still commits about twice
per batch: once to claim it and once to retire its responses. A pause only merges work that
arrives within it. Its main effect here was a higher p50 on bursts (0.13 s → 0.43 s). To cut
steady-rate CPU, the pause has to be at least as long as the gap between enqueues, and that
costs up to that much latency. For example, a 1 s pause means up to about 1 s of extra latency.

