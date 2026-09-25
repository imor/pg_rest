"""
pg_rest-specific tests of the pipelined worker: a slow request only occupies its own pipeline
slot, no transaction is open while requests are in flight, and in-flight requests survive a
worker restart.
"""

import time

from sqlalchemy import text

from common import get_queue_length, restart_worker, wait_for_queue_drain
from common import wait_for_response_count, wait_for_worker_state, wait_until


# Longest a worker transaction may have been open when sampled. The worker's own transactions
# (claim/retire, TTL cleanup) take milliseconds.
BRIEF_XACT_S = 0.5


def worker_activity(autocommit_sess):
    """The worker's state and how long its current transaction has been open (None if none)."""
    return autocommit_sess.execute(
        text(
            """
        select state, extract(epoch from clock_timestamp() - xact_start)::float8
        from pg_stat_activity
        where backend_type ilike '%pg_rest%'
    """
        )
    ).one()


def test_slow_request_does_not_hold_back_fast_ones(sess, autocommit_sess):
    """
    With pg_net, a batch's responses are committed together, so one slow
    request delays every request of its batch. With pg_rest the fast
    responses are committed while the slow one is still in flight, and no
    transaction is open while it is.
    """

    wait_for_worker_state(autocommit_sess, "idle")

    (slow_id,) = sess.execute(
        text("select rest.http_get('http://localhost:8080/pathological?delay=3', timeout_milliseconds := 10000)")
    ).fetchone()
    fast_ids = [
        row[0]
        for row in sess.execute(
            text("select rest.http_get('http://localhost:8080/') from generate_series(1, 500)")
        ).fetchall()
    ]
    sess.commit()
    committed_at = time.time()

    def fetch():
        (fast, slow) = autocommit_sess.execute(
            text(
                """
            select
                count(*) filter (where id <> :slow_id and status_code = 200),
                count(*) filter (where id = :slow_id)
            from rest._http_response
        """
            ),
            {"slow_id": slow_id},
        ).one()
        return (fast, slow)

    (fast, slow) = wait_until(
        fetch, lambda r: r[0] == 500 or r[1] > 0, timeout=10, sleep_interval=0.05,
        description="the fast responses",
    )
    elapsed = time.time() - committed_at
    assert (fast, slow) == (500, 0), f"after {elapsed:.2f}s"
    assert elapsed < 2, f"the fast responses took {elapsed:.2f}s"

    # Only the slow request is left in the queue, in flight
    (ids,) = autocommit_sess.execute(
        text("select array_agg(id) from rest.http_request_queue where claimed_at is not null")
    ).one()
    assert ids == [slow_id]
    assert get_queue_length(autocommit_sess)() == 1
    assert max(fast_ids) > slow_id

    # While the slow request is in flight the worker is active, but holds no transaction open
    # for it. A sample can still land inside the short TTL-cleanup transaction the worker runs
    # every second, so a transaction that has only just started is allowed.
    # Sample it a few times over the remaining ~1s the slow request has left.
    deadline = committed_at + 2.5
    samples = 0
    while time.time() < deadline:
        (state, xact_age) = worker_activity(autocommit_sess)
        assert state == "active"
        assert xact_age is None or xact_age < BRIEF_XACT_S, f"transaction open for {xact_age}s"
        samples += 1
        time.sleep(0.05)
    assert samples > 0
    assert fetch()[1] == 0, "the slow response arrived too early to observe the worker"

    # Then the slow response arrives and the worker goes back to idle
    wait_for_response_count(autocommit_sess, 501)
    wait_for_queue_drain(autocommit_sess)
    wait_for_worker_state(autocommit_sess, "idle")

    (status_code,) = autocommit_sess.execute(
        text("select status_code from rest._http_response where id = :id"), {"id": slow_id}
    ).one()
    assert status_code == 200
    (state, xact_age) = worker_activity(autocommit_sess)
    assert state == "idle"
    assert xact_age is None or xact_age < BRIEF_XACT_S, f"transaction open for {xact_age}s"


def test_in_flight_requests_survive_worker_restart(sess, autocommit_sess):
    """
    rest.worker_restart() while requests are in flight: the old worker
    gives them a short grace period, then exits leaving them claimed, and the
    new worker sends them again. All of them get exactly one response.
    """

    ids = [
        row[0]
        for row in sess.execute(
            text(
                "select rest.http_get('http://localhost:8080/pathological?status=200&delay=2') from generate_series(1, 5)"
            )
        ).fetchall()
    ]
    sess.commit()

    wait_until(
        lambda: autocommit_sess.execute(
            text("select count(*) from rest.http_request_queue where claimed_at is not null")
        ).scalar_one(),
        lambda count: count == 5,
        description="the requests to be in flight",
    )

    restart_worker(autocommit_sess)

    wait_for_response_count(autocommit_sess, 5)
    wait_for_queue_drain(autocommit_sess)

    rows = autocommit_sess.execute(
        text("select id, status_code, error_msg from rest._http_response order by id")
    ).fetchall()
    assert [tuple(r) for r in rows] == [(i, 200, None) for i in ids]
