#!/usr/bin/env bash
# Runs pg_net's CI loadtest against pg_net or pg_rest.
#
# This mirrors pg_net's `net-loadtest` (nix/loadtest.nix), which pg_net's CI runs on every PR:
# a PG 17 cluster without assertions started by `xpg`, nginx from `net-with-nginx` on :8080,
# `call wait_for_many_gets(N)` from test/utils/loadtest.sql, and psrecord sampling the worker's
# CPU and memory once a second. Two differences:
#   - It waits until the worker's pid is known before starting psrecord, rather than sleeping
#     2 s (which isn't enough for initdb + startup on every machine).
#   - It records the worker's exact CPU time and RSS (`ps`) just before and after the run, since
#     psrecord's 1 s samples miss most of a run that lasts about a second.
#   - It can run pg_rest: it swaps in pg_rest versions of test/init.conf, test/init.sql and
#     loadtest.sql (restored on exit), and copies pg_rest's build into build-17/, which is
#     where xpg loads extensions from.
#
# Usage:
#   bench/pg_net_loadtest/run.sh <pg_net checkout> <pg_net|pg_rest> <requests> [pg_net batch_size]
#
# Environment:
#   PG_REST_PKG  output dir of `cargo pgrx package --pg-config <the harness's pg_config>`
#                (required for pg_rest)
#   OUT_DIR      where results go (default: bench/pg_net_loadtest/results)
#
# The pg_net checkout is modified while a pg_rest run is in progress (and build-17/ keeps the
# pg_rest files), so point it at a scratch copy rather than a working tree you care about.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

if [ "${1:-}" != "--inner" ]; then
  net_dir="$(cd "$1" && pwd)"
  ext="$2"
  reqs="$3"
  batch="${4:-}"
  out_dir="${OUT_DIR:-$here/results}"
  mkdir -p "$out_dir"
  out_dir="$(cd "$out_dir" && pwd)"
  cd "$net_dir"
  exec nix-shell --argstr pgVersion 17 --arg cassert false \
    --run "bash '$here/run.sh' --inner '$ext' '$reqs' '$batch' '$out_dir' '${PG_REST_PKG:-}'"
fi

# Inside pg_net's nix-shell, in the pg_net checkout.
shift
ext="$1"
reqs="$2"
batch="$3"
out_dir="$4"
pkg="$5"

tag="$ext-$reqs"
pidfile="$PWD/build-17/worker.pid"
query_csv="$out_dir/$tag.query.csv"
record_log="$out_dir/$tag.psrecord.log"
cpu_before="$out_dir/$tag.cpu_before"
cpu_after="$out_dir/$tag.cpu_after"

# psrecord isn't on PATH in pg_net's shell; net-loadtest refers to it by store path.
psrecord="$(grep -oE '/nix/store/[^ ]*-psrecord-[^/ ]*/bin/psrecord' "$(command -v net-loadtest)" | head -1)"

# Build first, so the cluster starts quickly once the run begins.
xpg build > /dev/null 2>&1

case "$ext" in
  pg_net)
    schema=net
    opts="-c log_min_messages=WARNING${batch:+ -c pg_net.batch_size=$batch}"
    ;;
  pg_rest)
    [ -n "$pkg" ] || { echo "PG_REST_PKG is required for pg_rest" >&2; exit 1; }
    schema=rest
    opts="-c log_min_messages=WARNING"
    cp test/init.conf test/init.conf.orig
    cp test/init.sql test/init.sql.orig
    trap 'mv test/init.conf.orig test/init.conf; mv test/init.sql.orig test/init.sql; rm -f test/utils/loadtest_rest.sql' EXIT
    cp "$here/init_rest.conf" test/init.conf
    cp "$here/init_rest.sql" test/init.sql
    cp "$here/loadtest_rest.sql" test/utils/loadtest_rest.sql
    cp "$(find "$pkg" -name 'pg_rest.dylib' -o -name 'pg_rest.so' | head -1)" build-17/
    find "$pkg" \( -name 'pg_rest.control' -o -name 'pg_rest--*.sql' \) -exec cp {} build-17/extension/ \;
    ;;
  *)
    echo "unknown extension: $ext" >&2
    exit 1
    ;;
esac

rm -f "$pidfile"

net-with-nginx xpg --options "$opts" psql -X \
  -c "select $schema.wait_until_running()" \
  -c "\pset tuples_only on" -c "\pset format unaligned" -c "\o $pidfile" \
  -c "select pid from pg_stat_activity where backend_type ilike '%$ext%worker%'" \
  -c "\o" -c "\pset tuples_only off" \
  -c "\! ps -o time=,rss= -p \$(cat $pidfile) > $cpu_before" \
  -c "call wait_for_many_gets($reqs)" \
  -c "\! ps -o time=,rss= -p \$(cat $pidfile) > $cpu_after" \
  -c "\pset format csv" -c "\o $query_csv" -c "select * from run" > /dev/null &
xpg_pid=$!

for _ in $(seq 1 600); do
  grep -qE '^[0-9]+$' "$pidfile" 2> /dev/null && break
  sleep 0.2
done

# Samples until the worker exits, which happens when xpg stops the cluster after the run. Like
# pg_net's CI it samples once a second, so a run that finishes within a second or two (pg_rest's
# usually do) gets few or no samples; the exact worker CPU time comes from the `ps` snapshots
# taken just before and after wait_for_many_gets instead.
"$psrecord" "$(cat "$pidfile")" --interval 1 --log "$record_log" > /dev/null 2>&1 || true
wait "$xpg_pid"

echo "== $tag"
cat "$query_csv"
