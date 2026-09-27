"""
Failure-path benchmark: what happens when storing some responses fails.

A BEFORE INSERT trigger on rest._http_response raises for every request whose id is divisible
by --fail-every. With 10,000 requests and buckets of 100:
  1000  10 bad rows, so about 1 bucket in 10 contains one
  100   100 bad rows, about one in every bucket
  10    1,000 bad rows, about ten in every bucket

For each scenario it enqueues -n requests in one transaction and waits until the queue is empty,
or until --timeout. It reports the elapsed time, responses stored, responses missing, worker CPU
and worker restarts. The "none" scenario has no trigger at all; "0" has the trigger, but it never
raises, which isolates the trigger's own cost.

Usage:
  uv run --with 'psycopg[binary]' --with psutil python bench/failure_bench.py \\
      --dsn 'host=127.0.0.1 port=28819 user=postgres dbname=postgres' --url http://127.0.0.1:8090
"""

import argparse
import time

import psutil
import psycopg

TRIGGER_FN = """
create or replace function public.pg_rest_fail_some() returns trigger language plpgsql as $$
begin
  if {every} > 0 and new.id % {every} = 0 then
    raise exception 'simulated failure for request %', new.id;
  end if;
  return new;
end
$$
"""


def worker_pid(conn):
    row = conn.execute(
        "select pid from pg_stat_activity where backend_type ilike '%pg_rest%'"
    ).fetchone()
    return row[0] if row else None


def cpu_seconds(pid):
    try:
        t = psutil.Process(pid).cpu_times()
        return t.user + t.system
    except psutil.NoSuchProcess:
        return None


def run(conn, url, n, every, timeout):
    conn.execute("drop trigger if exists pg_rest_fail_some on rest._http_response")
    if every != "none":
        conn.execute(TRIGGER_FN.format(every=int(every)))
        conn.execute(
            "create trigger pg_rest_fail_some before insert on rest._http_response "
            "for each row execute function public.pg_rest_fail_some()"
        )
    conn.execute("truncate rest._http_response")
    conn.execute("delete from rest.http_request_queue")
    time.sleep(1)

    pids = [worker_pid(conn)]
    cpu0 = cpu_seconds(pids[0])

    with conn.transaction():
        conn.execute(
            "select rest.http_get(%s, timeout_milliseconds := 30000) from generate_series(1, %s)",
            (url + "/", n),
        )
    t0 = time.monotonic()

    done = False
    while time.monotonic() - t0 < timeout:
        pid = worker_pid(conn)
        if pid is not None and pid != pids[-1]:
            pids.append(pid)
        if conn.execute("select count(*) from rest.http_request_queue").fetchone()[0] == 0:
            done = True
            break
        time.sleep(0.02)
    elapsed = time.monotonic() - t0

    stored = conn.execute("select count(*) from rest._http_response").fetchone()[0]
    cpu1 = cpu_seconds(pids[-1]) if len(pids) == 1 else None
    conn.execute("drop trigger if exists pg_rest_fail_some on rest._http_response")
    return {
        "done": done,
        "elapsed": elapsed,
        "stored": stored,
        "missing": n - stored,
        "restarts": len(pids) - 1,
        "cpu": (cpu1 - cpu0) if cpu0 is not None and cpu1 is not None else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default="host=127.0.0.1 port=28819 user=postgres dbname=postgres")
    parser.add_argument("--url", default="http://127.0.0.1:8090")
    parser.add_argument("-n", type=int, default=10000)
    parser.add_argument("--fail-every", default="none,0,1000,100,10")
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()

    print(
        f"{'fail every':>10} {'finished':>8} {'time s':>7} {'stored':>7} {'missing':>7} "
        f"{'restarts':>8} {'cpu s':>6} {'cpu ms/1k':>9}"
    )
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        for every in args.fail_every.split(","):
            r = run(conn, args.url, args.n, every, args.timeout)
            cpu = f"{r['cpu']:.2f}" if r["cpu"] is not None else "-"
            per_1k = f"{r['cpu'] * 1e6 / args.n:.0f}" if r["cpu"] is not None else "-"
            print(
                f"{every:>10} {str(r['done']):>8} {r['elapsed']:>7.2f} {r['stored']:>7} "
                f"{r['missing']:>7} {r['restarts']:>8} {cpu:>6} {per_1k:>9}",
                flush=True,
            )
            # Let a crash-looping worker settle before the next scenario.
            time.sleep(3)


if __name__ == "__main__":
    main()
