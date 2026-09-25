"""
Summarizes the results of bench/pg_net_loadtest/run.sh as a markdown table.

For each run (`<ext>-<requests>`) it reads:
  <tag>.query.csv       the `run` table written by wait_for_many_gets (what pg_net's CI reports)
  <tag>.cpu_before/after `ps -o time=,rss=` of the worker just before and after the run
  <tag>.psrecord.log    psrecord samples, 1 s apart (what pg_net's CI reports as CPU/MEM)

Usage: python3 bench/pg_net_loadtest/summarize.py [results dir]
"""

import csv
import re
import sys
from pathlib import Path


def ps_seconds(value):
    """Parses `ps -o time=` output: [[dd-]hh:]mm:ss[.xx]."""
    days = 0
    if "-" in value:
        d, value = value.split("-", 1)
        days = int(d)
    parts = [float(p) for p in value.split(":")]
    seconds = 0.0
    for p in parts:
        seconds = seconds * 60 + p
    return days * 86400 + seconds


def interval_seconds(value):
    """Parses a Postgres interval like 00:00:50.656171."""
    h, m, s = value.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


def read_ps(path):
    time, rss = path.read_text().split()
    return ps_seconds(time), int(rss) / 1024


def read_psrecord(path):
    samples = []
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                _, cpu, real, _ = line.split()
                samples.append((float(cpu), float(real)))
    return samples


def main():
    results = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parent / "results")
    rows = []
    for query in sorted(results.glob("*.query.csv")):
        tag = query.name.removesuffix(".query.csv")
        ext, reqs = re.match(r"(.+)-(\d+)$", tag).groups()
        with query.open() as f:
            run = next(csv.DictReader(f))
        taken = interval_seconds(run["time_taken"])
        cpu0, _ = read_ps(results / f"{tag}.cpu_before")
        cpu1, rss1 = read_ps(results / f"{tag}.cpu_after")
        cpu = cpu1 - cpu0
        samples = read_psrecord(results / f"{tag}.psrecord.log")
        rows.append(
            {
                "ext": ext,
                "requests": int(reqs),
                "batch_size": run["batch_size"] or "–",
                "time_taken": taken,
                "req_per_s": int(reqs) / taken if taken else 0,
                "successes": run["request_successes"],
                "failures": run["request_failures"],
                "cpu_s": cpu,
                "cpu_ms_per_1k": cpu * 1000 / int(reqs) * 1000,
                "rss_mb": rss1,
                "samples": len(samples),
                "max_cpu_pct": max((c for c, _ in samples), default=None),
                "max_real_mb": max((r for _, r in samples), default=None),
            }
        )

    rows.sort(key=lambda r: (r["requests"], r["ext"]))
    print(
        "| requests | extension | batch_size | time_taken | req/s | successes | failures "
        "| worker CPU | CPU per 1k requests | worker RSS after | psrecord samples "
        "| psrecord max CPU | psrecord max real |"
    )
    print("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        max_cpu = f"{r['max_cpu_pct']:.1f}%" if r["max_cpu_pct"] is not None else "–"
        max_real = f"{r['max_real_mb']:.1f} MB" if r["max_real_mb"] is not None else "–"
        print(
            f"| {r['requests']:,} | {r['ext']} | {r['batch_size']} | {r['time_taken']:.2f} s "
            f"| {r['req_per_s']:,.0f} | {r['successes']} | {r['failures']} | {r['cpu_s']:.2f} s "
            f"| {r['cpu_ms_per_1k']:.0f} ms | {r['rss_mb']:.1f} MB | {r['samples']} "
            f"| {max_cpu} | {max_real} |"
        )


if __name__ == "__main__":
    main()
