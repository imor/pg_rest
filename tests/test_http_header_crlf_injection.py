import socket
import threading

from sqlalchemy import text

from common import collect_response_sync


def _start_raw_capture_server(
    response=b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    accept_timeout=10,
):
    """
    Starts a bare TCP server (no HTTP parsing at all) that accepts exactly one
    connection, records every raw byte it receives, then writes back a
    minimal valid HTTP response so the client's request completes normally.

    Returns (port, captured, thread). `captured` is a dict that will hold the
    raw request bytes under the "raw" key once `thread` finishes.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    captured = {"raw": b""}

    def _run():
        srv.settimeout(accept_timeout)
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            srv.close()
            return
        conn.settimeout(0.5)
        buf = b""
        try:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                buf += chunk
        except socket.timeout:
            # No more bytes arriving -- the client is done sending and is now
            # waiting on our reply, which is exactly what we want to capture.
            pass
        captured["raw"] = buf
        try:
            conn.sendall(response)
        finally:
            conn.close()
            srv.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return port, captured, thread


def _post_with_header(sess, port, header_value):
    (request_id,) = sess.execute(
        text(
            """
        select rest.http_post(
            url := :url,
            body := jsonb_build_object('marker', 'expected-json-body'),
            headers := jsonb_build_object('x-injected-secret', cast(:value as text))
        );
    """
        ),
        {"url": f"http://127.0.0.1:{port}/", "value": header_value},
    ).fetchone()
    sess.commit()
    return collect_response_sync(sess, request_id)


def test_header_value_crlf_injection_is_rejected(sess):
    """
    pg_net bug (docs/bugs.md item 7 in pg_net): a header VALUE containing an
    embedded CRLFCRLF sequence was passed verbatim to curl, which split the
    request so that the receiver saw the rest of the headers as body.

    pg_rest (reqwest/hyper) rejects header values containing CR/LF: the
    request is never sent and it gets an error response instead. pg_net's
    version of this test asserted that the value is sent without splitting
    the request; here we assert that nothing reaches the wire at all.
    """
    port, captured, thread = _start_raw_capture_server(accept_timeout=3)

    # Mimics a header value that itself contains a raw CRLFCRLF, splicing in
    # a fake extra header along the way -- e.g. what a tainted secret value
    # from a credential table could look like.
    injected_value = "innocuous-secret\r\n\r\nX-Smuggled-Header: injected-by-bug"

    response = _post_with_header(sess, port, injected_value)

    assert response["status"] == "ERROR"
    assert response["message"].startswith("builder error"), response["message"]

    thread.join(timeout=15)
    assert captured["raw"] == b"", (
        f"a request with a CR/LF header value reached the wire:\n{captured['raw']!r}"
    )


def test_header_value_without_crlf_is_sent_unsplit(sess):
    """Control for the test above: a normal header value is sent as one well-formed request"""

    port, captured, thread = _start_raw_capture_server()

    response = _post_with_header(sess, port, "innocuous-secret")

    assert response["status"] == "SUCCESS"
    thread.join(timeout=15)

    raw = captured["raw"]
    assert raw, "the test TCP server never received a request from pg_rest"
    assert raw.count(b"\r\n\r\n") == 1, raw

    head, body = raw.split(b"\r\n\r\n", 1)
    assert b"\r\nx-injected-secret: innocuous-secret\r\n" in head + b"\r\n"
    assert body == b'{"marker": "expected-json-body"}'
