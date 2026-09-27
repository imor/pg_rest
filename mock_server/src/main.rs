//! HTTP server used by pg_rest's tests and benchmarks.
//!
//! A port of `tests/mock_server.py` (itself a replica of pg_net's nginx test server) that is
//! fast enough not to be the bottleneck in benchmarks. It serves the same endpoints on port 8080,
//! plus an IPv6-only server on port 8888. HTTP is handled over raw TCP (parsed with `httparse`)
//! so that it can also misbehave on purpose: malformed headers, dropped connections.
//!
//! Usage: mock_server [--port 8080] [--ipv6-port 8888] [--threads N]

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

const MAX_HEADERS: usize = 64;
const LISTEN_BACKLOG: u32 = 4096;

struct Request {
    method: String,
    path: String,
    query: String,
    /// Header names are lowercased.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    /// The request line and headers exactly as received, including the final CRLF CRLF.
    raw_head: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k) == name).then(|| percent_decode(v))
        })
    }

    fn is_args(&self) -> &'static str {
        if self.query.is_empty() {
            ""
        } else {
            "?"
        }
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(b)) => {
                out.push(b);
                i += 3;
            }
            (b'+', _) => {
                out.push(b' ');
                i += 1;
            }
            (b, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Reads more bytes into `buf`. Returns false at end of stream.
async fn fill(stream: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<bool> {
    let mut chunk = [0u8; 16 * 1024];
    let n = stream.read(&mut chunk).await?;
    buf.extend_from_slice(&chunk[..n]);
    Ok(n > 0)
}

/// Reads one request. `Ok(None)` when the client closed the connection (or sent garbage).
async fn read_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<Option<Request>> {
    // Request line and headers.
    let (head_len, mut request) = loop {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Request::new(&mut headers);
        match parsed.parse(buf) {
            Ok(httparse::Status::Complete(n)) => {
                let target = parsed.path.unwrap_or("/");
                let (path, query) = target.split_once('?').unwrap_or((target, ""));
                let request = Request {
                    method: parsed.method.unwrap_or("GET").to_owned(),
                    path: path.to_owned(),
                    query: query.to_owned(),
                    headers: parsed
                        .headers
                        .iter()
                        .map(|h| {
                            (
                                h.name.to_ascii_lowercase(),
                                String::from_utf8_lossy(h.value).trim().to_owned(),
                            )
                        })
                        .collect(),
                    body: Vec::new(),
                    raw_head: buf[..n].to_vec(),
                };
                break (n, request);
            }
            Ok(httparse::Status::Partial) => {
                if !fill(stream, buf).await? {
                    return Ok(None);
                }
            }
            Err(_) => return Ok(None),
        }
    };
    buf.drain(..head_len);

    // Body: Content-Length or chunked.
    if let Some(len) = request
        .header("content-length")
        .and_then(|v| v.parse::<usize>().ok())
    {
        while buf.len() < len {
            if !fill(stream, buf).await? {
                return Ok(None);
            }
        }
        request.body = buf.drain(..len).collect();
    } else if request
        .header("transfer-encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        loop {
            let line_end = loop {
                if let Some(i) = buf.windows(2).position(|w| w == b"\r\n") {
                    break i;
                }
                if !fill(stream, buf).await? {
                    return Ok(None);
                }
            };
            let size_str = String::from_utf8_lossy(&buf[..line_end]).trim().to_owned();
            let size =
                usize::from_str_radix(size_str.split(';').next().unwrap_or(""), 16).unwrap_or(0);
            buf.drain(..line_end + 2);
            while buf.len() < size + 2 {
                if !fill(stream, buf).await? {
                    return Ok(None);
                }
            }
            if size == 0 {
                buf.drain(..2);
                break;
            }
            request.body.extend(buf.drain(..size));
            buf.drain(..2);
        }
    }
    Ok(Some(request))
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

/// Builds a response the way the Python server does: Content-Type (text/plain unless
/// overridden) and Connection first, then any extra headers, then Content-Length, then any raw
/// (possibly malformed) header lines.
fn response(status: u16, body: &[u8], extra: &[(&str, &str)], raw_headers: &[u8]) -> Vec<u8> {
    let content_type = extra
        .iter()
        .find(|(n, _)| *n == "Content-Type")
        .map_or("text/plain", |(_, v)| *v);
    let mut out = Vec::with_capacity(128 + body.len());
    out.extend_from_slice(format!("HTTP/1.1 {status} {}\r\n", reason(status)).as_bytes());
    out.extend_from_slice(
        format!("Content-Type: {content_type}\r\nConnection: keep-alive\r\n").as_bytes(),
    );
    for (name, value) in extra.iter().filter(|(n, _)| *n != "Content-Type") {
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    out.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    out.extend_from_slice(raw_headers);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}

fn malformed_header(kind: &str) -> &'static [u8] {
    match kind {
        "missing-value" => b"MissingValue: \r\n",
        "header-injection" => {
            b"HeaderInjection Injected-Header: This header contains an injection\r\n"
        }
        "spaces-in-header-name" => b"Spaces In Header Name: This header name contains spaces\r\n",
        "non-printable-chars" => b"NonPrintableChars: NonPrintableChars\x01\x02\r\n",
        _ => b"",
    }
}

/// Returns the bytes to write, or `None` to drop the connection without answering.
async fn route(r: &Request) -> Option<Vec<u8>> {
    let with_newline = |mut b: Vec<u8>| {
        b.push(b'\n');
        b
    };
    Some(match r.path.as_str() {
        "/" => response(200, b"Hello world\n", &[], b""),
        "/slow-reply" => {
            tokio::time::sleep(Duration::from_secs(2)).await;
            response(
                200,
                b"this text will come in response body with HTTP 200 after 2 seconds\n",
                &[],
                b"",
            )
        }
        "/really-slow-reply" => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            response(
                200,
                b"this text will come in response body with HTTP 200 after 30 seconds\n",
                &[],
                b"",
            )
        }
        "/echo-method" => response(200, format!("{}\n", r.method).as_bytes(), &[], b""),
        "/anything" => response(
            200,
            format!("{}{}\n", r.is_args(), r.query).as_bytes(),
            &[],
            b"",
        ),
        "/headers" => response(200, &r.raw_head, &[], b""),
        "/post" => {
            if r.method != "POST" {
                response(405, b"", &[], b"")
            } else if r.header("content-type") != Some("application/json") {
                response(406, b"", &[], b"")
            } else {
                response(
                    200,
                    &with_newline(r.body.clone()),
                    &[("Content-Type", "application/json")],
                    b"",
                )
            }
        }
        "/delete" => {
            if r.method != "DELETE" {
                response(405, b"", &[], b"")
            } else if r.header("content-type").is_some_and(|v| !v.is_empty()) {
                response(400, b"", &[], b"")
            } else {
                let mut body = r.raw_head.clone();
                body.extend_from_slice(format!("{}{}\n", r.is_args(), r.query).as_bytes());
                response(200, &body, &[], b"")
            }
        }
        "/delete_w_body" => {
            if r.method != "DELETE" {
                response(405, b"", &[], b"")
            } else {
                response(200, &with_newline(r.body.clone()), &[], b"")
            }
        }
        "/redirect_me" => response(
            301,
            b"",
            &[("Location", "/to_here"), ("X-Redirect-Hop", "first")],
            b"",
        ),
        "/to_here" => response(200, b"I got redirected\n", &[("X-Final-Hop", "last")], b""),
        "/pathological" => {
            let delay: f64 = r.param("delay").and_then(|d| d.parse().ok()).unwrap_or(0.0);
            if delay > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(delay)).await;
            }
            if r.param("disconnect").as_deref() == Some("true") {
                return None; // "Server returned nothing"
            }
            let status = r
                .param("status")
                .and_then(|s| s.parse().ok())
                .unwrap_or(200);
            let raw = r
                .param("malformed-header")
                .map_or(&b""[..], |m| malformed_header(&m));
            response(status, b"", &[], raw)
        }
        _ => response(404, b"not found\n", &[], b""),
    })
}

async fn handle(mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let mut buf = Vec::with_capacity(16 * 1024);
    loop {
        let request = match read_request(&mut stream, &mut buf).await {
            Ok(Some(request)) => request,
            _ => return,
        };
        let Some(out) = route(&request).await else {
            return;
        };
        if stream.write_all(&out).await.is_err() {
            return;
        }
        if request
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"))
        {
            return;
        }
    }
}

/// The IPv6-only server answers one request per connection.
async fn handle_ipv6(mut stream: TcpStream) {
    let mut buf = Vec::new();
    if let Ok(Some(_)) = read_request(&mut stream, &mut buf).await {
        let _ = stream
            .write_all(&response(200, b"Hello ipv6 only\n", &[], b""))
            .await;
    }
}

fn listen(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(LISTEN_BACKLOG)
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = arg(&args, "--port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let ipv6_port: u16 = arg(&args, "--ipv6-port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(8888);
    let threads: Option<usize> = arg(&args, "--threads").and_then(|t| t.parse().ok());

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = threads {
        builder.worker_threads(threads);
    }
    builder.build()?.block_on(async move {
        let v4 = listen(SocketAddr::from(([127, 0, 0, 1], port)))?;
        // No IPv6 on this machine is fine.
        let v6 = listen(SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, ipv6_port))).ok();
        println!("mock server listening on 127.0.0.1:{port} and [::1]:{ipv6_port}");

        if let Some(v6) = v6 {
            tokio::spawn(async move {
                while let Ok((stream, _)) = v6.accept().await {
                    tokio::spawn(handle_ipv6(stream));
                }
            });
        }
        loop {
            let (stream, _) = v4.accept().await?;
            tokio::spawn(handle(stream));
        }
    })
}
