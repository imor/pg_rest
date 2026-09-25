"""
pg_net counts the worker's queries with pg_stat_statements. pg_stat_statements isn't preloaded in
pg_rest's test cluster, so these tests count the worker's scans of rest.http_request_queue in
pg_stat_user_tables instead (the worker flushes its table stats, see
test_worker_writes_increment_pgstat_counters).

Differs from pg_net: pg_rest's idle worker still runs a query every TTL_CLEANUP_INTERVAL (1
second), to delete expired responses from rest._http_response. It doesn't touch the request queue
unless it is woken, which is what these tests check.
"""

import time

from sqlalchemy import text

from common import http_requests, wait_for_queue_drain, wait_for_response_count
from common import wait_for_worker_state, wait_until


def queue_scans(autocommit_sess):
    return autocommit_sess.execute(
        text(
            """
        select coalesce(seq_scan, 0) + coalesce(idx_scan, 0)
        from pg_stat_user_tables
        where relid = 'rest.http_request_queue'::regclass
    """
        )
    ).scalar_one()


def wait_for_stable_queue_scans(autocommit_sess):
    """Waits until the worker has flushed its stats, i.e. the scan count stops changing"""

    last = [None]

    def fetch():
        # pgstat flushes are rate limited to once a second (PGSTAT_MIN_INTERVAL)
        time.sleep(1.1)
        current = queue_scans(autocommit_sess)
        stable = current == last[0]
        last[0] = current
        return (stable, current)

    return wait_until(fetch, lambda r: r[0], timeout=15, description="stable queue scan count")[1]


def test_idle_worker_does_not_query_the_queue(sess, autocommit_sess):
    """
    Check that the background worker doesn't query the queue
    when no new requests arrive
    """

    wait_for_worker_state(autocommit_sess, "idle")
    old_scans = wait_for_stable_queue_scans(autocommit_sess)

    # sleep for some time to see if new queries arrive
    time.sleep(3)

    assert queue_scans(autocommit_sess) == old_scans


def test_wakes_at_commit_time(sess, autocommit_sess):
    """
    Check that the background worker is only woken at commit time,
    and not at all when the requests are rolled back
    """

    wait_for_worker_state(autocommit_sess, "idle")
    initial_scans = wait_for_stable_queue_scans(autocommit_sess)

    http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,100);
    """
        ),
    )

    wait_for_response_count(autocommit_sess, 100)
    wait_for_queue_drain(autocommit_sess)
    wait_for_worker_state(autocommit_sess, "idle")

    commit_scans = wait_for_stable_queue_scans(autocommit_sess)
    assert commit_scans > initial_scans

    # if the new requests are rollbacked/aborted, then no new queries will be made by the bg worker
    sess.execute(
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,100);
    """
        )
    )

    sess.rollback()

    # wait for requests
    time.sleep(2)

    assert queue_scans(autocommit_sess) == commit_scans
    (count,) = autocommit_sess.execute(text("select count(*) from rest._http_response")).fetchone()
    assert count == 100
