"""
Throughput/latency benchmark: pg_rest vs pg_net.

Both extensions are installed in the same database and send requests to the same mock server
(tests/mock_server.py). For each scenario, N requests are enqueued in one transaction, of which a
fraction go to a slow endpoint. The benchmark then polls the response table and records when
each response becomes visible, i.e. when the worker committed it. Latency is measured from the
enqueue commit to visibility, which is what a caller polling for its response observes.

It also samples the worker's pg_stat_activity row to report the longest time the worker held a
transaction open.

Requirements: a cluster with `shared_preload_libraries = 'pg_net, pg_rest'` and both extensions
creatable in the target database, and the mock server running.

Usage:
  uv run --with 'psycopg[binary]' python bench/bench.py \
      --dsn 'host=127.0.0.1 port=28819 user=postgres dbname=postgres' \
      --url http://127.0.0.1:8090 -n 10000
"""

import argparse
import statistics
import threading
import time

import psycopg

EXTENSIONS = {
    "pg_net": {
        "schema": "net",
        "responses": "net._http_response",
        "queue": "net.http_request_queue",
        "backend": "%pg_net%",
    },
    "pg_rest": {
        "schema": "rest",
        "responses": "rest._http_response",
        "queue": "rest.http_request_queue",
        "backend": "%pg_rest%",
    },
}


def percentile(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[k]


class XactSampler(threading.Thread):
    """Samples how long the worker has had a transaction open."""

    def __init__(self, dsn, backend):
        super().__init__(daemon=True)
        self.dsn = dsn
        self.backend = backend
        self.max_xact_s = 0.0
        self.stop = threading.Event()

    def run(self):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            while not self.stop.is_set():
                row = conn.execute(
                    "select extract(epoch from now() - xact_start) from pg_stat_activity "
                    "where backend_type ilike %s and xact_start is not null",
                    (self.backend,),
                ).fetchone()
                if row and row[0] is not None:
                    self.max_xact_s = max(self.max_xact_s, float(row[0]))
                time.sleep(0.02)


def run_scenario(dsn, ext, url, n, slow_fraction, slow_delay, timeout_s):
    meta = EXTENSIONS[ext]
    schema = meta["schema"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"truncate {meta['responses']}")
        conn.execute(f"delete from {meta['queue']}")

        slow_every = round(1 / slow_fraction) if slow_fraction > 0 else 0
        sampler = XactSampler(dsn, meta["backend"])
        sampler.start()

        # Enqueue everything in one transaction; the commit wakes the worker.
        with conn.transaction():
            conn.execute(
                f"""
                select {schema}.http_get(
                    case when %(slow_every)s > 0 and i %% %(slow_every)s = 0
                         then %(url)s || '/pathological?delay=' || %(delay)s
                         else %(url)s || '/' end,
                    timeout_milliseconds := 30000)
                from generate_series(1, %(n)s) i
                """,
                {"slow_every": slow_every, "url": url, "delay": slow_delay, "n": n},
            )
        t0 = time.monotonic()

        # Poll visibility: record the time at which each response count was first seen.
        seen = 0
        arrivals = []
        deadline = t0 + timeout_s
        while seen < n and time.monotonic() < deadline:
            count = conn.execute(f"select count(*) from {meta['responses']}").fetchone()[0]
            now = time.monotonic() - t0
            if count > seen:
                arrivals.extend([now] * (count - seen))
                seen = count
            time.sleep(0.01)
        total = time.monotonic() - t0

        sampler.stop.set()
        sampler.join()

        errors = conn.execute(
            f"select count(*) from {meta['responses']} where status_code is distinct from 200"
        ).fetchone()[0]
        error_samples = conn.execute(
            f"select coalesce(error_msg, 'status ' || status_code), count(*) "
            f"from {meta['responses']} where status_code is distinct from 200 "
            f"group by 1 order by 2 desc limit 3"
        ).fetchall()

    # The earliest (n - slow) completions: approximates latency of the fast requests.
    fast = arrivals[: n - (n // slow_every if slow_every else 0)]
    return {
        "ext": ext,
        "completed": seen,
        "errors": errors,
        "total_s": total,
        "throughput": seen / total if total > 0 else 0,
        "p50_s": percentile(arrivals, 50),
        "p99_s": percentile(arrivals, 99),
        "fast_p99_s": percentile(fast, 99),
        "max_xact_s": sampler.max_xact_s,
        "error_samples": error_samples,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="host=127.0.0.1 port=28819 user=postgres dbname=postgres")
    parser.add_argument("--url", default="http://127.0.0.1:8090")
    parser.add_argument("-n", type=int, default=10000)
    parser.add_argument("--slow-delay", type=float, default=2.0)
    parser.add_argument("--slow-fractions", default="0,0.001,0.01")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--extensions", default="pg_net,pg_rest")
    args = parser.parse_args()

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        for ext in args.extensions.split(","):
            conn.execute(f"create extension if not exists {ext}")
        # Let the workers see the extensions before starting.
        time.sleep(2)

    print(
        f"{'scenario':<22} {'ext':<8} {'done':>6} {'errors':>6} {'total s':>8} {'req/s':>8} "
        f"{'p50 s':>7} {'p99 s':>7} {'fast p99 s':>10} {'max xact s':>10}"
    )
    for fraction in [float(f) for f in args.slow_fractions.split(",")]:
        scenario = f"n={args.n} slow={fraction:.1%}"
        for ext in args.extensions.split(","):
            r = run_scenario(args.dsn, ext, args.url, args.n, fraction, args.slow_delay, args.timeout)
            print(
                f"{scenario:<22} {r['ext']:<8} {r['completed']:>6} {r['errors']:>6} "
                f"{r['total_s']:>8.2f} {r['throughput']:>8.0f} {r['p50_s']:>7.2f} "
                f"{r['p99_s']:>7.2f} {r['fast_p99_s']:>10.2f} {r['max_xact_s']:>10.2f}",
                flush=True,
            )
            for msg, count in r["error_samples"]:
                print(f"    {count} x {msg}", flush=True)


if __name__ == "__main__":
    main()
