# pg_rest

Asynchronous HTTP requests from Postgres, written in Rust with [pgrx](https://github.com/pgcentralfoundation/pgrx),
[tokio](https://tokio.rs) and [reqwest](https://docs.rs/reqwest). pg_rest is a replacement for
[pg_net](https://github.com/supabase/pg_net) with the same request/response model. Its background worker
is pipelined, so one slow endpoint no longer holds up every other request.

```sql
select rest.http_get('https://example.com');                -- returns a request id
select * from rest._http_collect_response(1, async := false); -- waits for and returns the response
select * from rest._http_response where id = 1;              -- or read the response table directly
```

## Why

pg_net's worker opens a transaction, takes a batch of requests (200 by default) from the queue, sends
all of them, and waits for **every** response before it commits. Two things follow:

- A single slow response delays all the other responses in its batch, and delays the next batch too.
- The worker keeps a transaction open for as long as the slowest request takes.

pg_rest works like a CPU pipeline. It claims requests in a short transaction, and no transaction is
open while they are in flight. Responses are committed in small buckets as they arrive, and every
commit claims new requests into the slots that just freed up. A slow request only ever occupies its
own slot.

## Interface

The interface mirrors pg_net, in the `rest` schema so both extensions can be installed side by side:

| pg_net | pg_rest |
|---|---|
| `net.http_get(url, params, headers, timeout_milliseconds)` | `rest.http_get(...)` (same signature and defaults) |
| `net.http_post(url, body, params, headers, timeout_milliseconds)` | `rest.http_post(...)` |
| `net.http_delete(url, params, headers, timeout_milliseconds, body)` | `rest.http_delete(...)` |
| `net._http_collect_response(request_id, async)` | `rest._http_collect_response(...)` |
| `net.http_request_queue`, `net._http_response` | `rest.http_request_queue`, `rest._http_response` |
| `net.wake()`, `net.worker_restart()`, `net.wait_until_running()`, `net.check_worker_is_up()` | `rest.*` equivalents |

`rest.http_request_queue` has one extra column, `claimed_at`. It is set while a request is in flight,
and the row is deleted in the same statement that inserts its response.

## Installation

pg_rest has to be preloaded:

```
shared_preload_libraries = 'pg_rest'
```

```sh
cargo pgrx install --release --pg-config /path/to/pg_config
```

```sql
create extension pg_rest;
```

It supports Postgres 13 to 18, using the pgrx feature flags `pg13` … `pg18`. `pg19` builds against
19beta2.

## Configuration

pg_rest has no GUCs yet. Every tunable that could become one lives in [src/consts.rs](src/consts.rs):

| Constant | Default | Meaning |
|---|---|---|
| `DATABASE_NAME` / `USERNAME` | `postgres` / bootstrap superuser | where the worker connects (pg_net: `pg_net.database_name`, `pg_net.username`) |
| `MAX_IN_FLIGHT` | 1000 | pipeline depth: most requests claimed but not yet committed |
| `RESPONSE_BUCKET_SIZE` | 100 | a bucket is committed when it holds this many responses… |
| `RESPONSE_BUCKET_MAX_WAIT` | 50 ms | …or when its oldest response has waited this long |
| `RESPONSE_TTL` | 6 hours | responses older than this are deleted (pg_net: `pg_net.ttl`) |
| `MAX_TIMEOUT_MS` | 600000 | upper bound on `timeout_milliseconds` (pg_net: `pg_net.max_timeout_ms`) |
| `MAX_RESPONSE_BODY_BYTES` | 64 MiB | larger responses are recorded as errors |
| `MAX_CONCURRENT_REQUESTS_PER_HOST` | 200 | requests in flight to one host at a time; waiting for a slot counts against the request's timeout |
| `STALE_CONNECTION_RETRIES` | 1 | retries on a fresh connection when a pooled keep-alive connection died before any response (as curl does) |
| `HTTP_WORKER_THREADS` | 2 | tokio threads that do the HTTP work |

## Design

### Threads

The worker process has a main thread and a small tokio runtime.

- **The main thread is the only thread that ever calls into Postgres.** pgrx's thread check enforces
  this at runtime.
- **The tokio threads only send HTTP requests and receive responses.** They have every signal
  blocked, so Postgres' signal handlers can only run on the main thread.

The two sides communicate like this:

- **Main thread → tokio.** Each claimed request is spawned as a task (`Handle::spawn`), which never
  blocks.
- **Tokio → main thread.** Each task sends exactly one response on an unbounded channel, then writes
  a byte to a socket pair. The main thread waits on its latch and that socket together with
  `WaitLatchOrSocket`, so tokio threads never need `SetLatch`.
  - The channel never holds more than `MAX_IN_FLIGHT` responses, because at most that many requests
    are in flight.
  - A drop guard in each task makes sure a response is sent even if the task panics or is cancelled.

Tokio tasks never wait on the main thread, and the main thread only polls the channel. Neither side
can block the other, so they cannot deadlock.

### The main loop

Each iteration of the loop does the following:

1. Call `CHECK_FOR_INTERRUPTS()` and handle config reloads.
2. Move responses that have arrived into the bucket.
3. Run one short transaction if any of these is true:
   - the bucket is full, or its deadline has passed
   - there are free slots and the queue may have unclaimed requests
   - TTL cleanup is due

   The transaction does three things:
   - **retire:** `insert into _http_response … ; delete from http_request_queue where id = any(…)`,
     done in one statement for the whole bucket
   - **claim:** `update http_request_queue set claimed_at = now() … limit <free slots> for update skip locked returning …`
   - **expire:** delete up to `TTL_CLEANUP_BATCH` expired responses
4. After the commit, send the newly claimed requests.
5. Otherwise, wait on the latch and the wake socket until a wake, a signal, a response, or the next
   deadline.

The worker uses raw SPI with saved plans. pgrx's `Spi` assigns a transaction id before every
writable statement, and here a transaction id is only assigned when rows actually change.

### Failure handling

- **Restart or crash.** When the worker exits with requests in flight, those rows keep their
  `claimed_at`. The next worker clears every `claimed_at` at startup, so those requests are sent
  again. This is at-least-once delivery, the same as pg_net, where an aborted batch leaves its rows
  in the queue.
- **`worker_restart()`.** In-flight requests get `SHUTDOWN_GRACE` (500 ms) to finish, and
  everything that has arrived is committed before the worker exits.
- **Dropped or recreated extension.** The worker notices that the tables are missing, or that the
  queue's OID changed, and discards its in-flight requests. Their responses are ignored when they
  arrive.
- **Locked tables.** If another session holds a lock that conflicts with `AccessShareLock`, such as
  a `DROP EXTENSION` in progress, the worker retries after `LOCKED_RETRY_INTERVAL`.
- **Invalid timeouts.** A request with `timeout_milliseconds` outside `1..=MAX_TIMEOUT_MS` is not
  sent. It gets an error response instead.

### Observability

Both of these follow pg_net [#255](https://github.com/supabase/pg_net/pull/255) and
[#254](https://github.com/supabase/pg_net/pull/254):

- **pg_stat_activity.** The worker reports `active` while any request is in flight and `idle`
  otherwise. Because no transaction is open while requests are in flight, `xact_start` is only set
  during the short retire/claim transactions.
- **pgstat counters.** They are flushed with `pgstat_report_stat(false)` after every transaction,
  and with `pgstat_report_stat(true)` before going idle, so autovacuum sees the worker's writes.

## Differences from pg_net

- **Schema.** pg_rest uses the `rest` schema; pg_net uses `net`.
- **GUCs.** pg_rest has none yet; see [Configuration](#configuration).
- **URL-encoding helpers.** pg_rest has no `_urlencode_string` or `_encode_url_with_params_array`.
  Query parameters are encoded in Rust, with the same escaping as `curl_easy_escape`.
- **HTTP client.** pg_rest uses reqwest/hyper instead of libcurl. As a result:
  - Response header names are lowercase, e.g. `content-type`.
  - Error messages are reqwest's error chain, e.g. `error sending request for url (…): client error (Connect): tcp connect error: Connection refused (os error 61)`,
    instead of curl's strings.
  - Timeout messages don't break down DNS, TLS and HTTP time.
  - Malformed response headers that curl tolerates, such as spaces in header names, are errors.
  - Request header values containing CR/LF are rejected instead of being sent.
- **Null headers in `http_post`.** `http_post(..., headers := null)` sends
  `Content-Type: application/json`. pg_net drops all headers in this case.
- **Response bodies.** Invalid UTF-8 is replaced and NUL bytes are removed, so every body can be
  stored as `text`.

## Development

```sh
cargo pgrx test pg18      # #[pg_test]s: URL encoding, enqueueing, request validation
```

**pg_regress.** The tests in [tests/pg_regress](tests/pg_regress) check the SQL surface.
`cargo pgrx regress` needs `shared_preload_libraries = 'pg_rest'` in `~/.pgrx/data-NN/postgresql.conf`.
You can also run `pg_regress --use-existing` against any cluster that preloads pg_rest.

**End-to-end tests.** They live in [tests/](tests), ported from pg_net's pytest suite, and need a
running cluster with pg_rest preloaded plus the mock HTTP server:

```sh
python3 tests/mock_server.py &           # pg_net's nginx test endpoints on :8080 (and [::1]:8888)
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres \
  uv run --with pytest --with 'psycopg[binary]' --with sqlalchemy pytest tests
```

**Benchmark against pg_net.** This needs a cluster with `shared_preload_libraries = 'pg_net, pg_rest'`:

```sh
python3 tests/mock_server.py --port 8090 --ipv6-port 8891 &
uv run --with 'psycopg[binary]' python bench/bench.py --dsn 'host=127.0.0.1 port=5432 user=postgres dbname=postgres' -n 10000
uv run --with 'psycopg[binary]' --with psutil python bench/cpu_bench.py --dsn 'host=127.0.0.1 port=5432 user=postgres dbname=postgres'
```

## Benchmark

All runs used `bench/bench.py` on one laptop (Apple Silicon, macOS), with an assert-enabled
PG 18.6 build from `cargo pgrx init`, a release build of pg_rest and pg_net 0.20.4 with default
settings. Each run enqueued 10,000 GETs to the local mock server in one transaction. The
"slow" requests take 2 s each.

- **Latency** is the time from the enqueue commit until the response is visible in the response
  table.
- **fast p99** is the p99 latency of the earliest (N − slow) completions.
- **max xact** is the longest time the worker's transaction was open.

| slow requests | ext | total | throughput | p50 latency | p99 latency | fast p99 | max xact |
|---|---|---:|---:|---:|---:|---:|---:|
| 0% | pg_net | 50.1 s | 200 req/s | 25.6 s | 50.1 s | 50.1 s | 0.02 s |
| 0% | pg_rest | 0.21 s | 47,143 req/s | 0.12 s | 0.20 s | 0.20 s | 0 |
| 0.1% | pg_net | 70.0 s | 143 req/s | 35.5 s | 70.0 s | 70.0 s | 2.01 s |
| 0.1% | pg_rest | 2.27 s | 4,414 req/s | 0.12 s | 0.20 s | 0.20 s | 0 |
| 1% | pg_net | 150.0 s | 67 req/s | 77.5 s | 150.0 s | 150.0 s | 2.02 s |
| 1% | pg_rest | 2.25 s | 4,445 req/s | 0.13 s | 0.27 s | 0.20 s | 0 |

How to read the table:

- **pg_net, no slow requests.** Throughput is limited by its batch size (200) and the 1 s pause
  between batches.
- **pg_net, with slow requests.** Every batch that contains a slow request waits for it before
  committing, and the transaction stays open for the whole 2 s.
- **pg_rest, with slow requests.** The total time is about the time of one slow request. Fast
  responses are unaffected (fast p99 ≈ 0.2 s), and the worker never holds a transaction open for
  noticeable time.

### CPU usage

`bench/cpu_bench.py` measures the CPU time (user + system) of each extension's worker process.
For pg_rest this includes its tokio threads. It does not count the cost of enqueueing, which is
the same for both extensions. The setup is the same as above.

| scenario | ext | wall | CPU | CPU % | CPU per 1k requests |
|---|---|---:|---:|---:|---:|
| idle, 30 s | pg_net | 30.0 s | 0.00 s | 0.0 | – |
| idle, 30 s | pg_rest | 30.0 s | 0.03 s | 0.1 | – |
| burst, 10k, 0% slow | pg_net | 50.6 s | 0.99 s | 2.0 | 99 ms |
| burst, 10k, 0% slow | pg_rest | 0.71 s | 0.45 s | 62.9 | 45 ms |
| burst, 10k, 1% slow | pg_net | 150.4 s | 1.00 s | 0.7 | 100 ms |
| burst, 10k, 1% slow | pg_rest | 2.66 s | 0.45 s | 17.0 | 45 ms |
| steady, 100 req/s for 30 s | pg_net | 30.5 s | 0.37 s | 1.2 | 123 ms |
| steady, 100 req/s for 30 s | pg_rest | 30.0 s | 0.93 s | 3.1 | 312 ms |

- **Bursts.** pg_rest uses about half the CPU per request. Its CPU % is higher only because it
  finishes 20–70× sooner.
- **Idle.** pg_rest wakes every second for TTL cleanup, which costs about 0.1% of a core.
- **Low steady rates.** pg_rest costs about 2.5× more per request. pg_net handles everything that
  arrived in the last second in one transaction. pg_rest commits a claim soon after each wake and
  commits each partial bucket when its 50 ms deadline passes, so it runs many more small
  transactions. That is the price of low latency. Raising `RESPONSE_BUCKET_MAX_WAIT` trades
  latency for fewer transactions.
