"""
A small HTTP server used by the tests and the benchmark.

It mirrors the endpoints of pg_net's nginx test server (nix/nginx/conf/custom.conf in pg_net) on
port 8080, plus an IPv6-only server on port 8888. It is written against raw asyncio streams so
that it can also misbehave on purpose (malformed headers, dropped connections).

Usage: python3 tests/mock_server.py [--port 8080] [--ipv6-port 8888]
"""

import argparse
import asyncio
from urllib.parse import parse_qs, urlsplit

REASONS = {
    200: "OK",
    201: "Created",
    204: "No Content",
    301: "Moved Permanently",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    406: "Not Acceptable",
    500: "Internal Server Error",
    503: "Service Unavailable",
}


async def read_request(reader):
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    method, target, _version = lines[0].split(" ", 2)
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()

    body = b""
    if "content-length" in headers:
        body = await reader.readexactly(int(headers["content-length"]))
    elif headers.get("transfer-encoding", "").lower() == "chunked":
        while True:
            size = int((await reader.readuntil(b"\r\n")).strip(), 16)
            if size == 0:
                await reader.readuntil(b"\r\n")
                break
            body += await reader.readexactly(size)
            await reader.readexactly(2)
    return method, target, headers, body, head


def response(status, body=b"", headers=None, raw_headers=b""):
    if isinstance(body, str):
        body = body.encode()
    out = f"HTTP/1.1 {status} {REASONS.get(status, 'Unknown')}\r\n".encode()
    all_headers = {"Content-Type": "text/plain", "Connection": "keep-alive"}
    all_headers.update(headers or {})
    all_headers["Content-Length"] = str(len(body))
    for name, value in all_headers.items():
        out += f"{name}: {value}\r\n".encode()
    return out + raw_headers + b"\r\n" + body


MALFORMED_HEADERS = {
    "missing-value": b"MissingValue: \r\n",
    "header-injection": b"HeaderInjection Injected-Header: This header contains an injection\r\n",
    "spaces-in-header-name": b"Spaces In Header Name: This header name contains spaces\r\n",
    "non-printable-chars": b"NonPrintableChars: NonPrintableChars\x01\x02\r\n",
}


async def route(method, target, headers, body, raw_head):
    """Returns the bytes to write, or None to drop the connection."""
    url = urlsplit(target)
    path = url.path
    query = parse_qs(url.query)
    is_args = "?" if url.query else ""

    if path == "/":
        return response(200, "Hello world\n")
    if path == "/slow-reply":
        await asyncio.sleep(2)
        return response(200, "this text will come in response body with HTTP 200 after 2 seconds\n")
    if path == "/really-slow-reply":
        await asyncio.sleep(30)
        return response(200, "this text will come in response body with HTTP 200 after 30 seconds\n")
    if path == "/echo-method":
        return response(200, f"{method}\n")
    if path == "/anything":
        return response(200, f"{is_args}{url.query}\n")
    if path == "/headers":
        return response(200, raw_head)
    if path == "/post":
        if method != "POST":
            return response(405)
        if headers.get("content-type") != "application/json":
            return response(406)
        return response(200, body + b"\n", {"Content-Type": "application/json"})
    if path == "/delete":
        if method != "DELETE":
            return response(405)
        if headers.get("content-type"):
            return response(400)
        return response(200, raw_head + f"{is_args}{url.query}".encode() + b"\n")
    if path == "/delete_w_body":
        if method != "DELETE":
            return response(405)
        return response(200, body + b"\n")
    if path == "/redirect_me":
        return response(301, "", {"Location": "/to_here", "X-Redirect-Hop": "first"})
    if path == "/to_here":
        return response(200, "I got redirected\n", {"X-Final-Hop": "last"})
    if path == "/pathological":
        delay = float(query.get("delay", ["0"])[0])
        if delay:
            await asyncio.sleep(delay)
        if query.get("disconnect", ["false"])[0] == "true":
            return None
        status = int(query.get("status", ["200"])[0])
        malformed = query.get("malformed-header", [None])[0]
        raw = MALFORMED_HEADERS.get(malformed, b"")
        return response(status, "", raw_headers=raw)
    return response(404, "not found\n")


async def handle(reader, writer):
    try:
        while True:
            try:
                method, target, headers, body, raw_head = await read_request(reader)
            except (asyncio.IncompleteReadError, ConnectionError, ValueError):
                break
            out = await route(method, target, headers, body, raw_head)
            if out is None:
                break  # "Server returned nothing"
            writer.write(out)
            await writer.drain()
            if headers.get("connection", "").lower() == "close":
                break
    except ConnectionError:
        pass
    finally:
        writer.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--ipv6-port", type=int, default=8888)
    args = parser.parse_args()

    server = await asyncio.start_server(handle, "127.0.0.1", args.port, backlog=4096)

    async def ipv6_handle(reader, writer):
        try:
            await read_request(reader)
            writer.write(response(200, "Hello ipv6 only\n"))
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()

    servers = [server]
    try:
        servers.append(await asyncio.start_server(ipv6_handle, "::1", args.ipv6_port))
    except OSError:
        pass  # no IPv6 on this machine

    print(f"mock server listening on 127.0.0.1:{args.port} and [::1]:{args.ipv6_port}", flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    asyncio.run(main())
