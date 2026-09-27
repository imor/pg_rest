"""
CPU usage benchmark: pg_rest vs pg_net.

Measures the CPU time (user + system) consumed by each extension's background worker process.
For pg_rest this includes its tokio threads, since they belong to the same process. Only the
worker is measured: the cost of enqueueing (the `http_get` calls) is paid by the client backend
and is the same for both extensions.

Scenarios, run for each extension in turn:

  idle    both extensions installed, nothing to do, for --idle-seconds.
  burst   -n requests enqueued in one transaction (--slow-fraction of them slow). CPU is
          measured from the enqueue commit until every response is visible.
  steady  --rate requests/s enqueued in small transactions (every 100 ms) for --steady-seconds,
          measured until every response is visible. Choose a rate both extensions can sustain
          (pg_net's default throughput is ~200 req/s) so that the CPU cost is compared at equal
          throughput.

Requirements: a cluster with `shared_preload_libraries = 'pg_net, pg_rest'`, and the mock server
(tests/mock_server.py) running.

Usage:
  uv run --with 'psycopg[binary]' --with psutil python bench/cpu_bench.py \
      --dsn 'host=127.0.0.1 port=28819 user=postgres dbname=postgres' \
      --url http://127.0.0.1:8090
"""

import argparse
import time

import psutil
import psycopg

from bench import EXTENSIONS, clear, count_responses


def worker_process(conn, ext):
    row = conn.execute(
        "select pid from pg_stat_activity where backend_type ilike %s",
        (EXTENSIONS[ext]["backend"],),
    ).fetchone()
    if row is None:
        raise SystemExit(f"the {ext} worker is not running")
    return psutil.Process(row[0])


def cpu_seconds(proc):
    times = proc.cpu_times()
    return times.user, times.system


def reset(conn, ext):
    clear(conn, EXTENSIONS[ext])


def response_count(conn, ext):
    return count_responses(conn, EXTENSIONS[ext])


def wait_for_responses(conn, ext, n, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if response_count(conn, ext) >= n:
            return True
        time.sleep(0.01)
    return False


def enqueue(conn, ext, url, n, slow_every, slow_delay):
    schema = EXTENSIONS[ext]["schema"]
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


def measure(proc, fn):
    """Runs `fn` and returns (result, user s, system s, wall s) for the worker process."""
    user0, sys0 = cpu_seconds(proc)
    t0 = time.monotonic()
    result = fn()
    wall = time.monotonic() - t0
    user1, sys1 = cpu_seconds(proc)
    return result, user1 - user0, sys1 - sys0, wall


def scenario_idle(conn, ext, args):
    proc = worker_process(conn, ext)
    _, user, system, wall = measure(proc, lambda: time.sleep(args.idle_seconds))
    return {"requests": 0, "completed": 0, "user": user, "sys": system, "wall": wall}


def scenario_burst(conn, ext, args):
    reset(conn, ext)
    proc = worker_process(conn, ext)
    slow_every = round(1 / args.slow_fraction) if args.slow_fraction > 0 else 0

    def run():
        enqueue(conn, ext, args.url, args.n, slow_every, args.slow_delay)
        wait_for_responses(conn, ext, args.n, args.timeout)
        return response_count(conn, ext)

    completed, user, system, wall = measure(proc, run)
    return {"requests": args.n, "completed": completed, "user": user, "sys": system, "wall": wall}


def scenario_steady(conn, ext, args):
    reset(conn, ext)
    proc = worker_process(conn, ext)
    per_tick = max(1, round(args.rate / 10))
    ticks = round(args.steady_seconds * 10)
    total = per_tick * ticks

    def run():
        start = time.monotonic()
        for tick in range(ticks):
            enqueue(conn, ext, args.url, per_tick, 0, 0)
            # Keep a fixed schedule regardless of how long the enqueue took.
            next_tick = start + (tick + 1) * 0.1
            time.sleep(max(0.0, next_tick - time.monotonic()))
        wait_for_responses(conn, ext, total, args.timeout)
        return response_count(conn, ext)

    completed, user, system, wall = measure(proc, run)
    return {"requests": total, "completed": completed, "user": user, "sys": system, "wall": wall}


SCENARIOS = {"idle": scenario_idle, "burst": scenario_burst, "steady": scenario_steady}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="host=127.0.0.1 port=28819 user=postgres dbname=postgres")
    parser.add_argument("--url", default="http://127.0.0.1:8090")
    parser.add_argument("--extensions", default="pg_net,pg_rest")
    parser.add_argument("--scenarios", default="idle,burst,steady")
    parser.add_argument("-n", type=int, default=10000, help="requests in the burst scenario")
    parser.add_argument("--slow-fraction", type=float, default=0.0)
    parser.add_argument("--slow-delay", type=float, default=2.0)
    parser.add_argument("--rate", type=float, default=100, help="req/s in the steady scenario")
    parser.add_argument("--steady-seconds", type=float, default=30)
    parser.add_argument("--idle-seconds", type=float, default=30)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()

    extensions = args.extensions.split(",")
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        for ext in extensions:
            conn.execute(f"create extension if not exists {ext}")
        time.sleep(2)  # let the workers notice the extensions

        print(
            f"{'scenario':<34} {'ext':<8} {'done':>6} {'wall s':>7} {'user s':>7} {'sys s':>7} "
            f"{'cpu s':>7} {'cpu %':>6} {'cpu ms/1k req':>13}"
        )
        for name in args.scenarios.split(","):
            if name == "burst":
                label = f"burst n={args.n} slow={args.slow_fraction:.1%}"
            elif name == "steady":
                label = f"steady {args.rate:g} req/s for {args.steady_seconds:g}s"
            else:
                label = f"idle {args.idle_seconds:g}s"
            for ext in extensions:
                # Let the previous scenario's work (e.g. responses still being committed) settle.
                time.sleep(2)
                r = SCENARIOS[name](conn, ext, args)
                cpu = r["user"] + r["sys"]
                per_1k = f"{cpu * 1000 / r['completed'] * 1000:.1f}" if r["completed"] else "-"
                print(
                    f"{label:<34} {ext:<8} {r['completed']:>6} {r['wall']:>7.2f} "
                    f"{r['user']:>7.2f} {r['sys']:>7.2f} {cpu:>7.2f} "
                    f"{cpu / r['wall'] * 100:>6.1f} {per_1k:>13}",
                    flush=True,
                )


if __name__ == "__main__":
    main()
