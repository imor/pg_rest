//! Owned messages passed between the worker's main thread and the tokio threads. They contain no
//! Postgres data (Datums, palloc'd memory), so they are safe to move across threads.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Delete,
}

impl Method {
    /// Parses a `rest.http_method` value, which the domain restricts to get/post/delete in any
    /// case.
    pub fn parse(method: &str) -> Option<Method> {
        if method.eq_ignore_ascii_case("get") {
            Some(Method::Get)
        } else if method.eq_ignore_ascii_case("post") {
            Some(Method::Post)
        } else if method.eq_ignore_ascii_case("delete") {
            Some(Method::Delete)
        } else {
            None
        }
    }
}

/// A claimed request, ready to be sent.
#[derive(Debug)]
pub struct HttpRequest {
    pub id: i64,
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
}

/// The outcome of a request, ready to be inserted into the response table.
#[derive(Debug)]
pub struct HttpResponse {
    pub id: i64,
    /// Which generation of claims this response belongs to. See `Worker::generation`.
    pub generation: u64,
    pub outcome: Outcome,
}

#[derive(Debug)]
pub enum Outcome {
    Success {
        status_code: i32,
        headers: serde_json::Map<String, serde_json::Value>,
        content_type: Option<String>,
        body: Option<String>,
    },
    Failure {
        timed_out: bool,
        error_msg: String,
    },
}
