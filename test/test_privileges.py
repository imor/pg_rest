import pytest
from sqlalchemy import text
from common import collect_response_sync, http_request


def test_rest_on_postgres_role(sess):
    """Check that the postgres role can use the rest schema by default"""

    role = sess.execute(text("select current_user;")).fetchone()
    assert role[0] == "postgres"

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(
            'http://localhost:8080/anything'
        );
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "SUCCESS"


def test_rest_on_pre_existing_role(sess):
    """Check that a pre existing role can use the rest schema"""

    role = sess.execute(text("select current_user;")).fetchone()
    assert role[0] == "postgres"

    sess.execute(text("set local role to pre_existing;"))
    (request_id, current_user) = sess.execute(
        text(
            """
        select rest.http_get(
            'http://localhost:8080/anything'
        ), current_user;
    """
        )
    ).fetchone()
    assert request_id == 1
    assert current_user == "pre_existing"

    # Commit to wakeup background worker
    sess.commit()

    # Confirm that the request was retrievable
    sess.execute(text("set local role to pre_existing;"))
    response = collect_response_sync(sess, request_id)
    current_user = sess.execute(text("select current_user;")).scalar()
    assert response["status"] == "SUCCESS"
    assert current_user == "pre_existing"


@pytest.fixture
def another_role(sess):
    sess.execute(text("create role another;"))
    sess.commit()
    yield "another"
    sess.rollback()
    sess.execute(text("drop role if exists another;"))
    sess.commit()


def test_rest_on_new_role(sess, another_role):
    """Check that a newly created role can use the rest schema"""

    role = sess.execute(text("select current_user;")).fetchone()
    assert role[0] == "postgres"

    sess.execute(text("set local role to another;"))

    (request_id, current_user) = sess.execute(
        text(
            """
        select rest.http_get(
            'http://localhost:8080/anything'
        ), current_user;
    """
        )
    ).fetchone()
    assert request_id == 1
    assert current_user == "another"

    # Commit to wakeup background worker
    sess.commit()

    # Confirm that the request was retrievable
    sess.execute(text("set local role to another;"))
    response = collect_response_sync(sess, request_id)
    current_user = sess.execute(text("select current_user;")).scalar()
    assert response["status"] == "SUCCESS"
    assert current_user == "another"
    sess.commit()


def test_worker_restart_as_new_role(sess, another_role):
    """Check that a newly created role can use the rest.worker_restart function"""

    sess.execute(text("set local role to another;"))
    (res, current_user) = sess.execute(
        text(
            """
        select rest.worker_restart(), current_user;
    """
        )
    ).fetchone()
    assert res
    assert current_user == "another"

    sess.execute(text("select rest.wait_until_running();"))
