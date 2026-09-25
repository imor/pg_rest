//! The tokio/reqwest side of the worker.
//!
//! Nothing in this module may call into Postgres: everything here runs on tokio threads, except
//! `build_runtime` and `Http::send`, which only spawn tasks and are called from the main thread.

use std::error::Error as _;
use std::io::ErrorKind;
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::mpsc::UnboundedSender;

use crate::consts::{
    HTTP_WORKER_THREADS, MAX_CONCURRENT_CONNECTS, MAX_RESPONSE_BODY_BYTES, STALE_CONNECTION_RETRIES,
};
use crate::worker::notify::WakeSender;
use crate::worker::types::{HttpRequest, HttpResponse, Method, Outcome};

const USER_AGENT: &str = concat!("pg_rest/", env!("CARGO_PKG_VERSION"));

/// Blocks every signal in the calling thread and returns the previous mask.
fn block_all_signals() -> libc::sigset_t {
    unsafe {
        let mut all: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::pthread_sigmask(libc::SIG_BLOCK, &all, &mut old);
        old
    }
}

fn restore_signal_mask(mask: &libc::sigset_t) {
    unsafe {
        libc::pthread_sigmask(libc::SIG_SETMASK, mask, std::ptr::null_mut());
    }
}

/// Builds the runtime whose threads send requests.
///
/// Postgres' signal handlers touch backend-global state and must only run on the main thread.
/// A process-directed signal is delivered to any thread that doesn't block it, so every tokio
/// thread blocks all signals. They are blocked in the main thread while the runtime spawns its
/// threads (new threads inherit the mask), and again in `on_thread_start`, which also covers
/// threads the blocking pool spawns later (e.g. for DNS resolution).
pub fn build_runtime() -> std::io::Result<Runtime> {
    let old_mask = block_all_signals();
    let runtime = Builder::new_multi_thread()
        .worker_threads(HTTP_WORKER_THREADS)
        .thread_name("pg_rest http")
        .on_thread_start(|| {
            block_all_signals();
        })
        .enable_all()
        .build();
    restore_signal_mask(&old_mask);
    runtime
}

pub fn build_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        // reqwest only speaks http and https, which matches pg_net's CURLOPT_PROTOCOLS_STR.
        .redirect(reqwest::redirect::Policy::limited(10))
        // Limit connection attempts (TCP connect and TLS handshake), not requests: a full
        // pipeline would otherwise open up to MAX_IN_FLIGHT connections to one server at once
        // and overflow its listen backlog. Requests waiting for a connection reuse pooled ones
        // as they become free, and the wait counts against their timeout.
        .connector_layer(tower::limit::ConcurrencyLimitLayer::new(
            MAX_CONCURRENT_CONNECTS,
        ))
        .build()
}

/// Sends requests on the runtime and routes their responses back to the main thread.
pub struct Http {
    handle: Handle,
    client: reqwest::Client,
    tx: UnboundedSender<HttpResponse>,
    waker: WakeSender,
}

impl Http {
    pub fn new(
        handle: Handle,
        client: reqwest::Client,
        tx: UnboundedSender<HttpResponse>,
        waker: WakeSender,
    ) -> Self {
        Self {
            handle,
            client,
            tx,
            waker,
        }
    }

    /// Starts sending `request`. Never blocks. Exactly one response for it is eventually sent to
    /// the main thread, even if the task panics or is cancelled.
    pub fn send(&self, request: HttpRequest, generation: u64) {
        let guard = ResponseGuard {
            id: request.id,
            generation,
            tx: self.tx.clone(),
            waker: self.waker.clone(),
            sent: false,
        };
        let client = self.client.clone();
        self.handle.spawn(async move {
            let outcome = perform(&client, request).await;
            guard.send(outcome);
        });
    }
}

/// Makes sure every request produces exactly one response, which keeps the main thread's
/// in-flight count exact.
struct ResponseGuard {
    id: i64,
    generation: u64,
    tx: UnboundedSender<HttpResponse>,
    waker: WakeSender,
    sent: bool,
}

impl ResponseGuard {
    fn send(mut self, outcome: Outcome) {
        self.deliver(outcome);
    }

    fn deliver(&mut self, outcome: Outcome) {
        self.sent = true;
        let response = HttpResponse {
            id: self.id,
            generation: self.generation,
            outcome,
        };
        // The receiver only goes away when the worker is exiting.
        if self.tx.send(response).is_ok() {
            self.waker.wake();
        }
    }
}

impl Drop for ResponseGuard {
    fn drop(&mut self) {
        if !self.sent {
            self.deliver(Outcome::Failure {
                timed_out: false,
                error_msg: "request was aborted before it completed".into(),
            });
        }
    }
}

async fn perform(client: &reqwest::Client, request: HttpRequest) -> Outcome {
    let timeout = request.timeout;
    let started = Instant::now();

    let method = match request.method {
        Method::Get => reqwest::Method::GET,
        Method::Post => reqwest::Method::POST,
        Method::Delete => reqwest::Method::DELETE,
    };

    let mut builder = client.request(method, &request.url).timeout(timeout);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    match request.body {
        Some(body) => builder = builder.body(body),
        // Like pg_net, a POST without a body still sends `Content-Length: 0`.
        None if request.method == Method::Post => builder = builder.body(Vec::new()),
        None => {}
    }

    let mut retries_left = STALE_CONNECTION_RETRIES;
    let mut response = loop {
        let retry = if retries_left > 0 {
            builder.try_clone()
        } else {
            None
        };
        match builder.send().await {
            Ok(response) => break response,
            Err(e) => {
                let remaining = timeout.saturating_sub(started.elapsed());
                match retry {
                    // A pooled keep-alive connection can be closed by the server just as it is
                    // reused. curl (and so pg_net) retries on a fresh connection in that case;
                    // do the same, within the request's original timeout.
                    Some(next) if is_dead_connection(&e) && !remaining.is_zero() => {
                        retries_left -= 1;
                        builder = next.timeout(remaining);
                    }
                    _ => return failure(&e, timeout, started),
                }
            }
        }
    };

    let status_code = i32::from(response.status().as_u16());

    let mut headers = serde_json::Map::new();
    for (name, value) in response.headers() {
        headers.insert(
            name.as_str().to_owned(),
            serde_json::Value::String(clean_text(value.as_bytes())),
        );
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|v| clean_text(v.as_bytes()));

    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > MAX_RESPONSE_BODY_BYTES {
                    return Outcome::Failure {
                        timed_out: false,
                        error_msg: format!(
                            "response body exceeds the maximum of {MAX_RESPONSE_BODY_BYTES} bytes"
                        ),
                    };
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => return failure(&e, timeout, started),
        }
    }

    Outcome::Success {
        status_code,
        headers,
        content_type,
        // Like pg_net, an empty body is stored as NULL.
        body: (!body.is_empty()).then(|| clean_text(&body)),
    }
}

fn failure(e: &reqwest::Error, timeout: Duration, started: Instant) -> Outcome {
    if e.is_timeout() {
        let phase = if e.is_connect() {
            "while connecting"
        } else {
            "while sending the request or receiving the response"
        };
        Outcome::Failure {
            timed_out: true,
            error_msg: format!(
                "Timeout of {} ms reached {phase}. Total time: {:.3} ms",
                timeout.as_millis(),
                started.elapsed().as_secs_f64() * 1000.0
            ),
        }
    } else {
        Outcome::Failure {
            timed_out: false,
            error_msg: error_chain(e),
        }
    }
}

/// Whether `e` means the connection died before any response arrived (reset, aborted, or closed
/// by the peer), as opposed to e.g. a connect failure or a timeout.
fn is_dead_connection(e: &reqwest::Error) -> bool {
    if e.is_timeout() || e.is_connect() {
        return false;
    }
    let mut source = e.source();
    while let Some(s) = source {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }
        }
        if let Some(hyper) = s.downcast_ref::<hyper::Error>() {
            if hyper.is_incomplete_message() || hyper.is_canceled() {
                return true;
            }
        }
        source = s.source();
    }
    false
}

/// Joins an error and all of its sources, e.g.
/// "error sending request for url (...): client error (Connect): tcp connect error: Connection
/// refused (os error 61)".
fn error_chain(e: &reqwest::Error) -> String {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    msg
}

/// Converts bytes to text Postgres will accept: invalid UTF-8 is replaced and NUL bytes, which
/// `text` can't contain, are removed.
fn clean_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.contains('\0') {
        text.replace('\0', "")
    } else {
        text.into_owned()
    }
}
