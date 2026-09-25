"""
pg_net's versions of these tests lower the pg_net.ttl and pg_net.batch_size GUCs. pg_rest has no
GUCs: the ttl is RESPONSE_TTL ("6 hours") and at most TTL_CLEANUP_BATCH (1000) expired responses
are deleted every TTL_CLEANUP_INTERVAL (1 second), see src/consts.rs. So instead of lowering the
ttl, these tests make responses expire by moving their `created` timestamp into the past.

Unlike pg_net, pg_rest's worker deletes expired responses periodically even when it isn't woken.
"""

import time

from sqlalchemy import text

from common import collect_response_sync, get_response_count, http_request, http_requests
from common import restart_worker, wait_for_response_count, wait_until

TTL = "6 hours"


def expire_responses(sess, where="true"):
    sess.execute(
        text(
            f"update rest._http_response set created = now() - '{TTL}'::interval - '1 second'::interval where {where}"
        )
    )
    sess.commit()


def insert_expired_responses(sess, count):
    sess.execute(
        text(
            f"""
        insert into rest._http_response(id, status_code, content, created)
        select n, 200, 'expired', now() - '{TTL}'::interval - '1 minute'::interval
        from generate_series(1, :count) n
    """
        ),
        {"count": count},
    )
    sess.commit()


def test_http_responses_deleted_after_ttl(sess, autocommit_sess):
    """
    Check that http responses will be deleted when they reach their ttl,
    and that responses that haven't reached it are kept
    """

    old_id = http_request(sess, text("select rest.http_get('http://localhost:8080/anything');"))
    response = collect_response_sync(sess, old_id)
    assert response is not None
    assert response["status"] == "SUCCESS"

    new_id = http_request(sess, text("select rest.http_get('http://localhost:8080/anything');"))
    response = collect_response_sync(sess, new_id)
    assert response["status"] == "SUCCESS"
    sess.commit()

    expire_responses(sess, f"id = {old_id}")

    # The worker deletes the expired response without being woken up
    wait_for_response_count(autocommit_sess, 1)

    (remaining,) = autocommit_sess.execute(text("select id from rest._http_response")).fetchone()
    assert remaining == new_id

    # And keeps the one that hasn't expired, even after a few more cleanups
    time.sleep(2.5)
    assert get_response_count(autocommit_sess)() == 1


def test_http_responses_will_complete_deletion(sess, autocommit_sess):
    """
    Check that http responses will keep being deleted until completion
    despite no new requests coming, even when there are more expired
    responses than a single cleanup deletes
    """

    insert_expired_responses(sess, 2500)

    request_id = http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,4) offset 3;
    """
        ),
    )
    response = collect_response_sync(sess, request_id)
    assert response["status"] == "SUCCESS"
    sess.commit()

    # Each cleanup deletes at most 1000 responses, so it takes several of them. Wait until at
    # least one cleanup is seen partway through, to check that deletion is batched.
    wait_until(
        get_response_count(autocommit_sess),
        lambda count: count < 2504,
        description="expired responses to start being deleted",
    )

    # The 4 fresh responses are kept
    wait_for_response_count(autocommit_sess, 4)


def test_http_responses_will_delete_despite_restart(sess, autocommit_sess):
    """
    Check that http responses will keep being deleted despite no
    new requests coming and despite worker restart
    """

    request_id = http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,4) offset 3;
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "SUCCESS"

    wait_for_response_count(autocommit_sess, 4)

    insert_expired_responses(sess, 1500)
    expire_responses(sess)

    restart_worker(autocommit_sess)

    wait_for_response_count(autocommit_sess, 0)
