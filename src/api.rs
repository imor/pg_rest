//! User-facing SQL functions. These run in user backends, never in the background worker.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};

use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::Value;

use crate::shmem::{self, WorkerStatus};

const ENQUEUE_SQL: &str = "
    insert into rest.http_request_queue(method, url, headers, body, timeout_milliseconds)
    values ($1, $2, $3, convert_to($4::text, 'UTF8'), $5)
    returning id";

/// Whether `wake_at_commit` has been registered in this backend.
static WAKE_CALLBACK_REGISTERED: AtomicBool = AtomicBool::new(false);
/// Whether the current transaction should wake the worker when it commits.
static WAKE_AT_COMMIT: AtomicBool = AtomicBool::new(false);

/// Wakes the worker only at commit time, so that e.g.
/// `select rest.http_get(...) from generate_series(1, 100000)` wakes it once, and only after
/// the requests are visible to it.
#[pg_guard]
unsafe extern "C-unwind" fn wake_at_commit(event: pg_sys::XactEvent::Type, _arg: *mut c_void) {
    use pg_sys::XactEvent::*;
    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => {
            if WAKE_AT_COMMIT.swap(false, Ordering::Relaxed) {
                let state = shmem::state();
                // Only the first of many concurrent wakes sets the latch.
                if state
                    .should_wake
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    state.set_latch();
                }
            }
        }
        // `PREPARE TRANSACTION` / `COMMIT PREPARED` don't wake the worker automatically; call
        // `rest.wake()` after them, like pg_net.
        XACT_EVENT_PREPARE | XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => {
            WAKE_AT_COMMIT.store(false, Ordering::Relaxed);
        }
        _ => {}
    }
}

/// The functions live in a pgrx schema module rather than using `schema = rest` in the control
/// file, so that the `rest` schema is created by, and dropped with, the extension.
#[pg_schema]
mod rest {
    use super::*;

    /// Wakes the background worker when the current transaction commits.
    #[pg_extern]
    fn wake() {
        // RegisterXactCallback never deduplicates, so register at most once per backend.
        if !WAKE_CALLBACK_REGISTERED.swap(true, Ordering::Relaxed) {
            unsafe { pg_sys::RegisterXactCallback(Some(wake_at_commit), std::ptr::null_mut()) };
        }
        WAKE_AT_COMMIT.store(true, Ordering::Relaxed);
    }

    /// Restarts the background worker. Always returns true; the bool is kept for compatibility with
    /// pg_net, whose version also reloads the configuration (pg_rest has no settings to reload).
    #[pg_extern]
    fn worker_restart() -> bool {
        let state = shmem::state();
        state.got_restart.store(true, Ordering::Release);
        state.set_latch();
        true
    }

    /// Blocks until the background worker is running.
    #[pg_extern]
    fn wait_until_running() {
        let state = shmem::state();
        while state.status() != WorkerStatus::Running {
            unsafe {
                pg_sys::WaitLatch(
                    pg_sys::MyLatch,
                    (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH)
                        as i32,
                    10,
                    pg_sys::PG_WAIT_EXTENSION,
                );
                pg_sys::ResetLatch(pg_sys::MyLatch);
            }
            check_for_interrupts!();
        }
    }

    /// Interface to make an async GET request. Returns the request id.
    #[pg_extern(volatile, parallel_unsafe)]
    fn http_get(
        // url for the request
        url: Option<&str>,
        // key/value pairs to be url encoded and appended to the `url`
        params: default!(Option<JsonB>, "'{}'::jsonb"),
        // key/values to be included in request headers
        headers: default!(Option<JsonB>, "'{}'::jsonb"),
        // the maximum number of milliseconds the request may take before being cancelled
        timeout_milliseconds: default!(Option<i32>, 5000),
    ) -> i64 {
        enqueue("GET", url, params, headers, None, timeout_milliseconds)
    }

    /// Interface to make an async POST request. Returns the request id.
    #[pg_extern(volatile, parallel_unsafe)]
    fn http_post(
        // url for the request
        url: Option<&str>,
        // body of the POST request
        body: default!(Option<JsonB>, "'{}'::jsonb"),
        // key/value pairs to be url encoded and appended to the `url`
        params: default!(Option<JsonB>, "'{}'::jsonb"),
        // key/values to be included in request headers
        headers: default!(
            Option<JsonB>,
            r#"'{"Content-Type": "application/json"}'::jsonb"#
        ),
        // the maximum number of milliseconds the request may take before being cancelled
        timeout_milliseconds: default!(Option<i32>, 5000),
    ) -> i64 {
        let mut headers = match headers {
            Some(JsonB(Value::Object(map))) => map,
            _ => serde_json::Map::new(),
        };

        let content_type = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| jsonb_text(value));

        match content_type {
            // If the user provided headers but omitted the content type, add it back in.
            None | Some(None) => {
                headers.insert(
                    "Content-Type".into(),
                    Value::String("application/json".into()),
                );
            }
            Some(Some(content_type)) if content_type != "application/json" => {
                error!("Content-Type header must be \"application/json\"");
            }
            Some(Some(_)) => {}
        }

        enqueue(
            "POST",
            url,
            params,
            Some(JsonB(Value::Object(headers))),
            body,
            timeout_milliseconds,
        )
    }

    /// Interface to make an async DELETE request. Returns the request id.
    #[pg_extern(volatile, parallel_unsafe)]
    fn http_delete(
        // url for the request
        url: Option<&str>,
        // key/value pairs to be url encoded and appended to the `url`
        params: default!(Option<JsonB>, "'{}'::jsonb"),
        // key/values to be included in request headers
        headers: default!(Option<JsonB>, "'{}'::jsonb"),
        // the maximum number of milliseconds the request may take before being cancelled
        timeout_milliseconds: default!(Option<i32>, 5000),
        // optional body of the request
        body: default!(Option<JsonB>, "NULL"),
    ) -> i64 {
        enqueue("DELETE", url, params, headers, body, timeout_milliseconds)
    }

    fn enqueue(
        method: &str,
        url: Option<&str>,
        params: Option<JsonB>,
        headers: Option<JsonB>,
        body: Option<JsonB>,
        timeout_milliseconds: Option<i32>,
    ) -> i64 {
        // A NULL url is left to the table's not-null constraint, like pg_net.
        let url = url.map(|url| encode_url_with_params(url, params.as_ref().map(|p| &p.0)));

        let id = Spi::get_one_with_args::<i64>(
            ENQUEUE_SQL,
            &[
                method.into(),
                url.into(),
                headers.into(),
                body.into(),
                timeout_milliseconds.into(),
            ],
        )
        .unwrap_or_else(|e| error!("failed to enqueue request: {e}"))
        .expect("insert ... returning id returned no id");

        wake();

        id
    }
}

/// Validates `url` and appends `params` to its query string, encoded like pg_net does with
/// `curl_easy_escape`. Errors match pg_net's so that callers see the same messages.
pub(crate) fn encode_url_with_params(url: &str, params: Option<&Value>) -> String {
    let mut parsed = match url::Url::parse(url) {
        Ok(parsed) => parsed,
        Err(e) => error!("invalid URL \"{url}\": {e}"),
    };
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        error!("invalid URL \"{url}\": Unsupported URL scheme");
    }

    if let Some(Value::Object(params)) = params {
        // Postgres orders jsonb object keys by length first, then bytewise. Match it so that the
        // query string is identical to pg_net's.
        let mut params: Vec<_> = params.iter().collect();
        params.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));

        let encoded: Vec<String> = params
            .into_iter()
            // NULL values are skipped, like `key || '=' || NULL` in pg_net.
            .filter_map(|(key, value)| {
                jsonb_text(value).map(|value| format!("{}={}", escape(key), escape(&value)))
            })
            .collect();

        if !encoded.is_empty() {
            let encoded = encoded.join("&");
            let query = match parsed.query() {
                Some(existing) if !existing.is_empty() => format!("{existing}&{encoded}"),
                _ => encoded,
            };
            parsed.set_query(Some(&query));
        }
    }

    parsed.into()
}

/// The text form of a jsonb value, as returned by `jsonb_each_text`. `None` for JSON null.
fn jsonb_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Percent-encodes everything except RFC 3986 unreserved characters, like `curl_easy_escape`.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_encode_url_without_params() {
        assert_eq!(
            encode_url_with_params("http://localhost:8080", None),
            "http://localhost:8080/"
        );
    }

    #[pg_test]
    fn test_encode_url_with_params() {
        let params = serde_json::json!({"hello": "world", "a b": "c&d", "n": 1, "skip": null});
        assert_eq!(
            encode_url_with_params("http://localhost:8080/anything?x=1#frag", Some(&params)),
            "http://localhost:8080/anything?x=1&n=1&a%20b=c%26d&hello=world#frag"
        );
    }

    #[pg_test(error = "invalid URL \"/malformed_url\": relative URL without a base")]
    fn test_encode_relative_url() {
        encode_url_with_params("/malformed_url", None);
    }

    #[pg_test(error = "invalid URL \"localhost:8080\": Unsupported URL scheme")]
    fn test_encode_unsupported_scheme() {
        encode_url_with_params("localhost:8080", None);
    }

    fn queued(id: i64) -> (String, String, Option<JsonB>, Option<String>, i32) {
        Spi::connect(|client| {
            let row = client
                .select(
                    "select method::text, url, headers, convert_from(body, 'UTF8'), timeout_milliseconds
                     from rest.http_request_queue where id = $1 and claimed_at is null",
                    None,
                    &[id.into()],
                )?
                .first();
            Ok::<_, pgrx::spi::Error>((
                row.get::<String>(1)?.unwrap(),
                row.get::<String>(2)?.unwrap(),
                row.get::<JsonB>(3)?,
                row.get::<String>(4)?,
                row.get::<i32>(5)?.unwrap(),
            ))
        })
        .unwrap()
    }

    #[pg_test]
    fn test_http_get_enqueues() {
        let id = Spi::get_one::<i64>(
            r#"select rest.http_get('http://localhost:8080/anything', '{"a": "1"}', '{"X-Test": "yes"}', 1234)"#,
        )
        .unwrap()
        .unwrap();
        let (method, url, headers, body, timeout) = queued(id);
        assert_eq!(method, "GET");
        assert_eq!(url, "http://localhost:8080/anything?a=1");
        assert_eq!(headers.unwrap().0, serde_json::json!({"X-Test": "yes"}));
        assert_eq!(body, None);
        assert_eq!(timeout, 1234);
    }

    #[pg_test]
    fn test_http_post_enqueues_with_default_content_type() {
        let id = Spi::get_one::<i64>(
            r#"select rest.http_post('http://localhost:8080/post', '{"hello": "world"}', headers := '{"X-Test": "yes"}')"#,
        )
        .unwrap()
        .unwrap();
        let (method, _, headers, body, timeout) = queued(id);
        assert_eq!(method, "POST");
        assert_eq!(
            headers.unwrap().0,
            serde_json::json!({"X-Test": "yes", "Content-Type": "application/json"})
        );
        // Stored as the jsonb's text form, like pg_net.
        assert_eq!(body.as_deref(), Some(r#"{"hello": "world"}"#));
        assert_eq!(timeout, 5000);
    }

    #[pg_test(error = "Content-Type header must be \"application/json\"")]
    fn test_http_post_rejects_other_content_types() {
        Spi::run(
            r#"select rest.http_post('http://localhost:8080/post', headers := '{"content-type": "text/plain"}')"#,
        )
        .unwrap();
    }

    #[pg_test]
    fn test_http_delete_enqueues() {
        let id = Spi::get_one::<i64>("select rest.http_delete('http://localhost:8080/delete')")
            .unwrap()
            .unwrap();
        let (method, _, _, body, _) = queued(id);
        assert_eq!(method, "DELETE");
        assert_eq!(body, None);

        let id = Spi::get_one::<i64>(
            r#"select rest.http_delete('http://localhost:8080/delete_w_body', body := '{"a": 1}')"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(queued(id).3.as_deref(), Some(r#"{"a": 1}"#));
    }

    #[pg_test(
        error = "null value in column \"url\" of relation \"http_request_queue\" violates not-null constraint"
    )]
    fn test_null_url_is_rejected() {
        Spi::run("select rest.http_get(null)").unwrap();
    }

    #[pg_test]
    fn test_worker_is_running() {
        Spi::run("select rest.wait_until_running()").unwrap();
        Spi::run("select rest.check_worker_is_up()").unwrap();
        let state = Spi::get_one::<String>(
            "select application_name from pg_stat_activity where backend_type ilike '%pg_rest%'",
        )
        .unwrap();
        assert_eq!(
            state.as_deref(),
            Some(concat!("pg_rest ", env!("CARGO_PKG_VERSION")))
        );
    }
}
