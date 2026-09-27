//! Validation of claimed requests. Only called from the worker's main thread.
//!
//! On this branch there are no tables: requests and responses live in shared memory (`mem`),
//! so the worker runs no SQL at all.

use std::time::Duration;

use pgrx::JsonB;
use serde_json::Value;

use crate::consts::MAX_TIMEOUT_MS;
use crate::worker::types::{HttpRequest, Method, Outcome};

/// What a claimed row turned into.
pub enum Claimed {
    /// Ready to send.
    Send(HttpRequest),
    /// Must not be sent; its (error) response is ready.
    Rejected(i64, Outcome),
}

pub fn to_request(
    id: i64,
    method: &str,
    url: String,
    timeout_ms: i32,
    headers: Option<JsonB>,
    body: Option<Vec<u8>>,
) -> Claimed {
    let reject = |error_msg: String| {
        Claimed::Rejected(
            id,
            Outcome::Failure {
                timed_out: false,
                error_msg,
            },
        )
    };

    // A request that never finishes would hold its pipeline slot forever, so timeouts are
    // bounded. Requests outside the bound are not sent.
    if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
        return reject(format!(
            "timeout_milliseconds must be between 1 and {MAX_TIMEOUT_MS}, got {timeout_ms}"
        ));
    }

    let Some(method) = Method::parse(method) else {
        return reject(format!("Unsupported request method {method}"));
    };

    let headers = match headers.map(|h| h.0) {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Object(map)) => map
            .into_iter()
            .filter_map(|(name, value)| match value {
                // Like pg_net, headers with a null value are skipped.
                Value::Null => None,
                Value::String(s) => Some((name, s)),
                other => Some((name, other.to_string())),
            })
            .collect(),
        Some(_) => return reject("headers must be a JSON object".into()),
    };

    Claimed::Send(HttpRequest {
        id,
        method,
        url,
        headers,
        body,
        timeout: Duration::from_millis(timeout_ms as u64),
    })
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::prelude::*;

    fn rejection(claimed: Claimed) -> String {
        match claimed {
            Claimed::Rejected(_, Outcome::Failure { error_msg, .. }) => error_msg,
            Claimed::Rejected(..) => panic!("rejected without an error"),
            Claimed::Send(request) => panic!("expected a rejection, got {request:?}"),
        }
    }

    #[pg_test]
    fn test_to_request() {
        let headers = serde_json::json!({"a": "1", "b": 2, "skipped": null});
        let Claimed::Send(request) = to_request(
            7,
            "post",
            "http://x/".into(),
            100,
            Some(JsonB(headers)),
            Some(b"hi".to_vec()),
        ) else {
            panic!("expected a request");
        };
        assert_eq!(request.id, 7);
        assert_eq!(request.method, Method::Post);
        assert_eq!(
            request.headers,
            vec![("a".into(), "1".into()), ("b".into(), "2".into())]
        );
        assert_eq!(request.body.as_deref(), Some(&b"hi"[..]));
        assert_eq!(request.timeout, Duration::from_millis(100));
    }

    #[pg_test]
    fn test_to_request_rejects_bad_timeouts() {
        for timeout in [0, -1, MAX_TIMEOUT_MS + 1] {
            assert_eq!(
                rejection(to_request(
                    1,
                    "GET",
                    "http://x/".into(),
                    timeout,
                    None,
                    None
                )),
                format!(
                    "timeout_milliseconds must be between 1 and {MAX_TIMEOUT_MS}, got {timeout}"
                )
            );
        }
    }

    #[pg_test]
    fn test_to_request_rejects_non_object_headers() {
        let headers = Some(JsonB(serde_json::json!(["a"])));
        assert_eq!(
            rejection(to_request(1, "GET", "http://x/".into(), 10, headers, None)),
            "headers must be a JSON object"
        );
    }
}
