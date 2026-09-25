import pytest
from sqlalchemy import text
from common import collect_response_sync, http_request, http_requests

wrong_port = 6666


def test_get_bad_url(sess):
    """Test rest.http_get returns a descriptive errors for bad urls"""

    with pytest.raises(Exception) as execinfo:
        sess.execute(
            text(
                f"""
            select rest.http_get('localhost:{wrong_port}');
        """
            )
        )

    assert "Unsupported URL scheme" in str(execinfo.value)


def test_http_get_rejects_relative_url(sess):
    """Test rest.http_get with a correct error when given a relative url"""

    with pytest.raises(Exception) as execinfo:
        sess.execute(
            text(
                """
            select rest.http_get('/malformed_url');
        """
            )
        )

    assert 'invalid URL "/malformed_url"' in str(execinfo.value)


def test_bad_post(sess):
    """Test rest.http_post with an empty url + body returns an error"""

    with pytest.raises(Exception) as execinfo:
        sess.execute(
            text(
                """
            select rest.http_post(null, '{"hello": "world"}');
        """
            )
        )
    assert 'null value in column "url"' in str(execinfo)


def test_bad_get(sess):
    """Test rest.http_get with an empty url + body returns an error"""

    with pytest.raises(Exception) as execinfo:
        sess.execute(
            text(
                """
            select rest.http_get(null);
        """
            )
        )
    assert 'null value in column "url"' in str(execinfo)


def test_bad_delete(sess):
    """Test rest.http_delete with an empty url + body returns an error"""

    with pytest.raises(Exception) as execinfo:
        sess.execute(
            text(
                """
            select rest.http_delete(null);
        """
            )
        )
    assert 'null value in column "url"' in str(execinfo)


# pg_net's test_bad_utils is not ported: pg_rest has no rest._urlencode_string or
# rest._encode_url_with_params_array, URLs are encoded in Rust inside rest.http_get and friends.


def test_it_keeps_working_after_many_connection_refused(sess):
    """
    Test the worker doesn't crash on many failed responses
    with connection refused
    """

    request_id = http_requests(
        sess,
        text(
            f"""
        select rest.http_get('http://localhost:{wrong_port}') from generate_series(1,10) offset 9;
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "ERROR"

    (error_msg, count) = sess.execute(
        text(
            """
        select error_msg, count(*) from rest._http_response where status_code is null group by error_msg;
    """
        )
    ).fetchone()

    # pg_rest reports reqwest's error chain instead of curl's "Couldn't connect to server"
    # (the os error number is platform specific, e.g. 61 on macOS, 111 on Linux)
    assert error_msg.startswith(
        f"error sending request for url (http://localhost:{wrong_port}/): "
        "client error (Connect): tcp connect error: Connection refused (os error "
    )
    assert count == 10

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200');
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response["status"] == "SUCCESS"
    assert response["message"] == "ok"
    assert response["status_code"] == 200


def test_it_keeps_working_after_server_returns_nothing(sess):
    """
    Test the worker doesn't crash on many failed responses
    with server returned nothing
    """

    request_id = http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?disconnect=true') from generate_series(1,10) offset 9;
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "ERROR"

    (error_msg, count) = sess.execute(
        text(
            """
        select error_msg, count(*) from rest._http_response where status_code is null group by error_msg;
    """
        )
    ).fetchone()

    # pg_rest reports hyper's error instead of curl's "Server returned nothing (no headers, no data)"
    assert error_msg == (
        "error sending request for url (http://localhost:8080/pathological?disconnect=true): "
        "client error (SendRequest): connection closed before message completed"
    )
    assert count == 10

    request_id = http_requests(
        sess,
        text(
            """
        select rest.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10) offset 9;
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response["status"] == "SUCCESS"

    (status_code, count) = sess.execute(
        text(
            """
        select status_code, count(*) from rest._http_response where status_code = 200 group by status_code;
    """
        )
    ).fetchone()

    assert status_code == 200
    assert count == 10
