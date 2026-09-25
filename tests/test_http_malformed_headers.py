from sqlalchemy import text
from common import collect_response_sync, http_request


def test_http_header_missing_value(sess):
    """Check that a `MissingValue: ` header is processed correctly"""

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(
            url:='http://localhost:8080/pathological?malformed-header=missing-value'
        );
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "SUCCESS"
    # header names are lowercase in pg_rest (reqwest/hyper), curl keeps the server's case
    assert response["headers"]["missingvalue"] == ""


def test_http_header_injection(sess):
    """
    Check that a `HeaderInjection Injected-Header: This header
    contains an injection` header fails without crashing
    """

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(
            url:='http://localhost:8080/pathological?malformed-header=header-injection'
        );
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "ERROR"
    # hyper's error instead of curl's "Weird server reply"
    assert response["message"].endswith("client error (SendRequest): invalid HTTP header parsed")


def test_http_header_spaces(sess):
    """
    Check that a `Spaces In Header Name: This header name contains spaces`
    header fails without crashing.

    Differs from pg_net: curl tolerates the invalid header name and the
    request succeeds, hyper rejects the response instead.
    """

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(
            url:='http://localhost:8080/pathological?malformed-header=spaces-in-header-name'
        );
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "ERROR"
    assert response["message"].endswith("client error (SendRequest): invalid HTTP header parsed")


def test_http_header_non_printable_chars(sess):
    """
    Check that a `NonPrintableChars: NonPrintableChars\\u0001\\u0002`
    header fails without crashing.

    Differs from pg_net: curl accepts the control characters in the header
    value and the request succeeds, hyper rejects the response instead.
    """

    request_id = http_request(
        sess,
        text(
            """
        select rest.http_get(
            url:='http://localhost:8080/pathological?malformed-header=non-printable-chars'
        );
    """
        ),
    )

    response = collect_response_sync(sess, request_id)

    assert response is not None
    assert response["status"] == "ERROR"
    assert response["message"].endswith("client error (SendRequest): invalid HTTP header parsed")
