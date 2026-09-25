# pg_rest vs pg_net in pg_net's CI loadtest

pg_net's CI runs a loadtest on every PR: `nix/loadtest.nix` (`net-loadtest`) in the `loadtest`
job of `.github/workflows/main.yml`. This document runs pg_rest in that same harness, next to
pg_net, with the CI's matrix.

## How it was run

`bench/pg_net_loadtest/run.sh` mirrors `net-loadtest`:

- **Environment:** pg_net's own `nix-shell --argstr pgVersion 17 --arg cassert false`, i.e. the
  PG 17.0 build without assertions that CI uses.
- **Cluster:** started by `xpg`, with nginx from `net-with-nginx` on `:8080` serving the
  requests.
- **Workload:** `call wait_for_many_gets(N)` from pg_net's `test/utils/loadtest.sql`. It enqueues
  N `http_get('http://localhost:8080')` in one transaction and measures from the last enqueue
  until the responses are in.
- **Sampling:** psrecord samples the worker's CPU and memory every second.
- **Matrix:** the CI's own. pg_net runs (10k, `pg_net.batch_size` 200), (20k, 400) and
  (40k, 800). pg_rest has no batch size and runs its defaults for the same request counts: 1,000
  requests in flight, buckets of 100 responses, and a 50 ms bucket deadline.

How the pg_rest runs differ:

- **Procedure.** pg_rest runs a copy of `wait_for_many_gets` against `rest.*`
  (`loadtest_rest.sql`). pg_net's version waits for the *last request id*, which is enough because
  pg_net commits responses in id order. pg_rest can complete requests out of order, so its version
  waits until *all* N responses are in. That is a stricter condition, and it is applied to
  pg_rest only.
- **Worker CPU.** The driver also records the worker's exact CPU time and RSS with `ps`, straight
  before and after the procedure. psrecord samples only once a second, so it barely catches
  pg_rest's runs, which finish in under a second. Those runs got 1–2 samples. pg_net's runs, at
  about 50 s, get about 50.
- **psrecord start.** The driver starts psrecord once the worker's pid is known, instead of
  `net-loadtest`'s fixed `sleep 2`. On this machine, 2 s was not enough for initdb plus startup,
  and the stock script failed.

The machine is an Apple Silicon laptop running macOS, with Nix; CI uses `ubuntu-latest`. Absolute
numbers will differ from CI's, but both extensions ran in the same harness on the same machine. CI
publishes its results only in each run's step summary, which isn't available through the GitHub
API, so there are no CI numbers here to compare against directly.

To reproduce:

```sh
# pg_rest built against the harness's PG 17 (the pg_config path is printed by `xpg`/`which pg_config` inside pg_net's nix-shell)
cargo pgrx package --pg-config <nix PG 17 pg_config> --no-default-features --features pg17 --out-dir /tmp/pg_rest_pkg17
export PG_REST_PKG=/tmp/pg_rest_pkg17
bench/pg_net_loadtest/run.sh <scratch copy of pg_net> pg_net 10000 200
bench/pg_net_loadtest/run.sh <scratch copy of pg_net> pg_rest 10000
# ... 20000/400, 40000/800 ...
python3 bench/pg_net_loadtest/summarize.py
```

`run.sh` temporarily swaps pg_rest's `init.conf`/`init.sql` into the pg_net checkout, restores
them on exit, and leaves pg_rest's files in `build-17/`. Point it at a scratch copy of pg_net.
The raw results are in `results/`.

## Results

"Loadtest results" is what pg_net's CI reports: time taken, successes, failures. Every run
succeeded for all requests.

| requests | extension | batch_size | time_taken | req/s | failures |
|---:|---|---:|---:|---:|---:|
| 10,000 | pg_net | 200 | 50.34 s | 199 | 0 |
| 10,000 | pg_rest | – | **0.27 s** | **36,714** | 0 |
| 20,000 | pg_net | 400 | 51.38 s | 389 | 0 |
| 20,000 | pg_rest | – | **0.47 s** | **42,118** | 0 |
| 40,000 | pg_net | 800 | 53.73 s | 744 | 0 |
| 40,000 | pg_rest | – | **0.95 s** | **42,212** | 0 |

pg_rest finishes 57–186× sooner.

- **pg_net.** Its time is almost constant at about 50 s, because the matrix scales the batch size
  with the request count. Every configuration therefore runs about 50 batches, and each batch is
  followed by pg_net's 1 s pause.
- **pg_rest.** It levels off at about 42k req/s. That is probably limited by nginx and the
  client-side enqueue, not by pg_rest.

### Worker CPU and memory

| requests | extension | worker CPU | CPU per 1k requests | worker RSS after the run | psrecord samples | psrecord max CPU | psrecord max real |
|---:|---|---:|---:|---:|---:|---:|---:|
| 10,000 | pg_net | 0.98 s | 98 ms | 24.3 MB | 50 | 2.6% | 24.2 MB |
| 10,000 | pg_rest | 0.83 s | 83 ms | 72.2 MB | 1 | 0.0% | 12.5 MB |
| 20,000 | pg_net | 1.86 s | 93 ms | 31.2 MB | 52 | 4.4% | 31.1 MB |
| 20,000 | pg_rest | 1.50 s | 75 ms | 99.0 MB | 1 | 0.0% | 12.5 MB |
| 40,000 | pg_net | 3.93 s | 98 ms | 48.9 MB | 54 | 10.9% | 48.7 MB |
| 40,000 | pg_rest | 3.39 s | 85 ms | 167.9 MB | 2 | 184.1% | 108.8 MB |

- **CPU.** pg_rest uses 13–19% less CPU per request than pg_net: 75–85 ms vs 93–98 ms per 1k
  requests. It uses it in a short burst rather than spread over 50 s. psrecord's single sample
  in the 40k run shows 184%, because pg_rest's worker thread and its tokio threads run in
  parallel.
- **psrecord columns for pg_rest.** Its runs end within about a second, so psrecord's
  once-a-second samples mostly land before or after the work. Use the `ps`-based columns for
  pg_rest.
- **Memory.** pg_rest's RSS is 3–3.5× pg_net's, and it grows with the run size. See below.

## Memory: what the higher RSS is

I checked this separately, with repeated 10k bursts against one long-lived pg_rest worker (the
PG 18 benchmark cluster):

- **Postgres memory contexts are flat.** `pg_log_backend_memory_contexts` reported the same
  2.0 MB grand total after 8 and after 14 bursts, byte for byte. Nothing leaks on the Postgres
  side.
- **RSS mostly plateaus.** It was 111 MB after the first burst, then 150 → 151 → 151 → 153 →
  161 MB over bursts 14–30. A steady per-request leak would add about 2 MB per burst. The
  remaining creep looks like allocator and buffer growth, but a small leak can't be fully ruled
  out.
- **The bulk is idle HTTP connections.** After the bursts, the worker held **2,136 idle
  keep-alive connections** to the one host, each with its own buffers and file descriptor.
  - A 1,000-request pipeline needs at most 1,000 at a time.
  - It overshoots because hyper starts a new connection whenever no pooled one is free. If an
    existing connection frees up first, the request uses that one, but the new connection still
    completes and joins the pool.
  - reqwest keeps idle connections for 90 s, with no per-host cap.

This is worth fixing before production use: on systems whose open-file limit is 1,024, a common
Linux default, the worker could run out of file descriptors. Capping idle connections per host
(reqwest's `pool_max_idle_per_host`) as a constant would bound both memory and descriptors. The
connections that are actually in use are already bounded by `MAX_IN_FLIGHT` and
`MAX_CONCURRENT_CONNECTS`.
