import time
import re
import psycopg
import pytest
from sqlalchemy import text
from common import pg_restart_worker, pg_wait_for_responses, wait_until
from common import http_request, pg_collect_response, pg_http_request

# pg_rest has no pg_net.max_timeout_ms GUC: the bound is MAX_TIMEOUT_MS (600000) in src/consts.rs,
# and the error message doesn't mention a GUC.
REJECTED = (
    "timeout_milliseconds must be between 1 and 600000, got {}"
)


def wait_for_responses(conn, ids, deadline_s=20):
    """
    Poll rest._http_response until every id has a row, return {id: (status_code, error_msg, timed_out)}

    `conn` is unused, a fresh autocommit connection is used so that every poll sees a new snapshot.
    """
    return pg_wait_for_responses(ids, deadline_s)


def test_http_get_timeout_reached(sess):
    """Test rest.http_get with timeout errs on a slow reply"""

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(url := 'http://localhost:8080/pathological?status=200&delay=6');
    """
        ),
    )

    # wait for timeout
    wait_for_responses(None, [request_id])

    (content_type, content, response, timed_out) = sess.execute(
        text(
            """
        select content_type, content, error_msg, timed_out from rest._http_response where id = :request_id;
    """
        ),
        {"request_id": request_id},
    ).fetchone()

    assert content_type is None
    assert content is None
    assert timed_out
    assert response.startswith("Timeout of 5000 ms reached")


def test_http_detailed_timeout(sess):
    """
    Test the timeout shows a detailed error msg.

    pg_net reports curl's per phase timings (DNS, TCP/SSL handshake,
    HTTP request/response). reqwest doesn't expose those, so pg_rest reports
    the phase the timeout happened in and the total time.
    """

    pattern = r"""
        ^Timeout\sof\s1000\sms\sreached\s
        while\ssending\sthe\srequest\sor\sreceiving\sthe\sresponse\.\s
        Total\stime:\s(?P<A>[0-9]*\.?[0-9]+)\sms$
    """

    regex = re.compile(pattern, re.VERBOSE)

    # Timeout at the HTTP step
    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(url := 'http://localhost:8080/pathological?delay=2', timeout_milliseconds := 1000)
    """
        ),
    )

    # wait for timeout
    wait_for_responses(None, [request_id])

    (content_type, content, response, timed_out) = sess.execute(
        text(
            """
        select content_type, content, error_msg, timed_out from rest._http_response where id = :request_id;
    """
        ),
        {"request_id": request_id},
    ).fetchone()

    match = regex.search(response)
    assert match, response

    total_time = float(match.group("A"))

    assert content_type is None
    assert content is None
    assert timed_out
    assert 1000 <= total_time < 2000


def test_http_get_succeed_with_gt_timeout(sess):
    """
    Test rest.http_get with timeout succeeds when the timeout
    is greater than the slow reply response time
    """

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(url := 'http://localhost:8080?status=200&delay=3', timeout_milliseconds := 3500);
    """
        ),
    )

    wait_for_responses(None, [request_id])

    (status_code,) = sess.execute(
        text(
            """
        select status_code from rest._http_response where id = :request_id;
    """
        ),
        {"request_id": request_id},
    ).fetchone()

    assert status_code == 200


def test_many_slow_mixed_with_fast(sess):
    """
    Test many fast responses finish despite being mixed with slow responses,
    the fast responses will wait the timeout duration
    """

    sess.execute(
        text(
            """
      select
        rest.http_get(url := 'http://localhost:8080/pathological?status=200')
      , rest.http_get(url := 'http://localhost:8080/pathological?status=200&delay=2', timeout_milliseconds := 1000)
      , rest.http_get(url := 'http://localhost:8080/pathological?status=200')
      , rest.http_get(url := 'http://localhost:8080/pathological?status=200&delay=2', timeout_milliseconds := 1000)
      from generate_series(1,25) _;
    """
        )
    )

    sess.commit()

    # wait for timeouts
    wait_until(
        lambda: sess.execute(text("select count(*) from rest._http_response")).scalar_one(),
        lambda count: count == 100,
        timeout=10,
        description="all 100 responses",
    )
    sess.commit()

    (request_successes, request_timeouts) = sess.execute(
        text(
            """
      select
        count(*) filter (where error_msg is null and status_code = 200) as request_successes,
        count(*) filter (where error_msg is not null and error_msg like 'Timeout of 1000 ms reached%') as request_timeouts
      from rest._http_response;
    """
        )
    ).fetchone()

    assert request_successes == 50
    assert request_timeouts == 50


@pytest.mark.parametrize("timeout", [0, -1, 2147483647])
def test_out_of_range_timeouts_are_rejected(conn, timeout):
    """A 0, negative or oversized timeout is not sent, the request gets an error response instead"""

    request_id = pg_http_request(
        conn,
        "select rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := %s)",
        (timeout,),
    )

    response = pg_collect_response(conn, request_id)

    assert response["status"] == "ERROR"
    assert (
        response["message"]
        == REJECTED.format(timeout)
    )


# pg_net's test_worker_honours_max_timeout_ms and test_max_timeout_ms_is_superuser_only are not
# ported: they test the pg_net.max_timeout_ms GUC, which pg_rest doesn't have.


def test_rejected_requests_do_not_affect_the_rest_of_the_batch(conn):
    """Rejected and valid requests consumed in the same batch are all answered"""

    ids = list(
        conn.execute(
            """
        select
            rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200'),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := -1),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200')
        """
        ).fetchone()
    )
    conn.commit()

    deadline = time.time() + 15
    responses = {}
    while len(responses) < 4 and time.time() < deadline:
        for request_id, status_code, error_msg in conn.execute(
            "select id, status_code, error_msg from rest._http_response where id = any(%s)",
            (ids,),
        ).fetchall():
            responses[request_id] = (status_code, error_msg)
        conn.rollback()
        time.sleep(0.2)

    assert [responses.get(i) for i in ids] == [
        (
            None,
            REJECTED.format(0),
        ),
        (200, None),
        (
            None,
            REJECTED.format(-1),
        ),
        (200, None),
    ]


@pytest.mark.parametrize("timeout", [1, 600000])
def test_timeout_bounds_are_accepted(conn, timeout):
    """The bounds themselves are valid, the request is sent"""

    request_id = pg_http_request(
        conn,
        "select rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := %s)",
        (timeout,),
    )

    (status_code, error_msg, timed_out) = wait_for_responses(conn, [request_id])[
        request_id
    ]

    # 1ms may legitimately time out, but the request must have been sent, not rejected
    assert status_code == 200 or timed_out
    assert error_msg != REJECTED.format(timeout)


def test_batch_with_only_rejected_requests_keeps_the_worker_alive(conn):
    """A batch where every request is rejected is answered and the worker carries on"""

    ids = list(
        conn.execute(
            """
        select
            rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0)
        """
        ).fetchone()
    )
    conn.commit()

    responses = wait_for_responses(conn, ids)
    assert [responses[i][1] for i in ids] == [REJECTED.format(0), REJECTED.format(0)]

    follow_up = pg_http_request(
        conn,
        "select rest.http_get(url := 'http://localhost:8080/pathological?status=200')",
    )
    assert wait_for_responses(conn, [follow_up])[follow_up][0] == 200


def test_rejected_request_last_in_batch(conn):
    """A rejected request at the end of a batch does not disturb the ones before it"""

    ids = list(
        conn.execute(
            """
        select
            rest.http_get(url := 'http://localhost:8080/pathological?status=200'),
            rest.http_post(url := 'http://localhost:8080/pathological?status=200', body := '{}'),
            rest.http_delete(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0)
        """
        ).fetchone()
    )
    conn.commit()

    responses = wait_for_responses(conn, ids)
    assert [responses[i][:2] for i in ids] == [
        (200, None),
        (200, None),
        (None, REJECTED.format(0)),
    ]


def test_direct_insert_out_of_range_is_rejected(conn):
    """Rows inserted directly into the queue go through the same check"""

    request_id = pg_http_request(
        conn,
        """
        insert into rest.http_request_queue(method, url, timeout_milliseconds)
        values ('GET', 'http://localhost:8080/pathological?status=200', -7)
        returning id
    """,
    )
    conn.execute("select rest.wake()")
    conn.commit()

    assert wait_for_responses(conn, [request_id])[request_id][1] == REJECTED.format(-7)


def test_rejected_alongside_slow_and_fast_requests(conn):
    """Rejection, a real timeout and a success in one batch are all recorded correctly"""

    ids = list(
        conn.execute(
            """
        select
            rest.http_get(url := 'http://localhost:8080/pathological?status=200&delay=6', timeout_milliseconds := 1500),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0),
            rest.http_get(url := 'http://localhost:8080/pathological?status=200')
        """
        ).fetchone()
    )
    conn.commit()

    responses = wait_for_responses(conn, ids)
    slow, rejected, fast = (responses[i] for i in ids)
    assert slow[2] and slow[1].startswith("Timeout of 1500 ms reached")
    assert rejected == (None, REJECTED.format(0), False)
    assert fast == (200, None, False)


def test_large_batch_mixing_rejected_and_valid_requests(conn):
    """More requests than batch_size, every other one rejected, all answered across batches"""

    ids = [
        row[0]
        for row in conn.execute(
            """
        select rest.http_get(
            url := 'http://localhost:8080/pathological?status=200',
            timeout_milliseconds := case when mod(n, 2) = 0 then 0 else 5000 end)
        from generate_series(1, 250) n
        """
        ).fetchall()
    ]
    conn.commit()

    responses = wait_for_responses(conn, ids, deadline_s=60)
    assert len(responses) == 250
    assert all(
        responses[i][1] == REJECTED.format(0)
        for n, i in enumerate(ids, 1)
        if n % 2 == 0
    )
    assert all(responses[i][0] == 200 for n, i in enumerate(ids, 1) if n % 2 == 1)


def test_worker_restart_after_rejections(conn):
    """Rejections leave nothing behind that breaks a worker restart"""

    request_id = pg_http_request(
        conn,
        "select rest.http_get(url := 'http://localhost:8080/pathological?status=200', timeout_milliseconds := 0)",
    )
    assert wait_for_responses(conn, [request_id])[request_id][1] == REJECTED.format(0)

    conn.commit()
    pg_restart_worker(conn)

    follow_up = pg_http_request(
        conn,
        "select rest.http_get(url := 'http://localhost:8080/pathological?status=200')",
    )
    assert wait_for_responses(conn, [follow_up])[follow_up][0] == 200
