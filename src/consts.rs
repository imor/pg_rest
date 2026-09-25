//! Tunables for pg_rest.
//!
//! No GUCs are defined yet. Every value here is a candidate for becoming one, and is kept in
//! this module so that promoting it later is a local change.

use std::time::Duration;

/// Database the background worker connects to. pg_net equivalent: `pg_net.database_name`.
pub const DATABASE_NAME: &str = "postgres";

/// Role the background worker connects as (`None` means the bootstrap superuser).
/// pg_net equivalent: `pg_net.username`.
pub const USERNAME: Option<&str> = None;

/// Maximum number of requests that are claimed but not yet retired (response committed).
/// This is the depth of the pipeline and also the upper bound on the claim batch size.
pub const MAX_IN_FLIGHT: usize = 1000;

/// Number of responses collected before they are committed to the response table.
pub const RESPONSE_BUCKET_SIZE: usize = 100;

/// Maximum time the oldest response in a partially filled bucket waits before the bucket is
/// committed anyway.
pub const RESPONSE_BUCKET_MAX_WAIT: Duration = Duration::from_millis(50);

/// How long responses are kept before they are deleted. Must be a valid Postgres interval.
/// pg_net equivalent: `pg_net.ttl`.
pub const RESPONSE_TTL: &str = "6 hours";

/// How often expired responses are deleted.
pub const TTL_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum number of expired responses deleted per cleanup.
pub const TTL_CLEANUP_BATCH: i32 = 1000;

/// Upper bound on a request's `timeout_milliseconds`. Requests outside `1..=MAX_TIMEOUT_MS` are
/// not sent and get an error response instead. pg_net equivalent: `pg_net.max_timeout_ms`.
pub const MAX_TIMEOUT_MS: i32 = 600_000;

/// Maximum size of a response body. Responses are held in memory until their bucket is
/// committed, so this bounds the worker's memory use to roughly
/// `MAX_IN_FLIGHT * MAX_RESPONSE_BODY_BYTES` in the worst case.
pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Number of tokio worker threads used to send requests and receive responses.
pub const HTTP_WORKER_THREADS: usize = 2;

/// Maximum number of requests in flight to one host (host and port) at a time. Bounds the burst of
/// new connections a full pipeline would otherwise open to a single server. Time spent waiting
/// for a slot counts against the request's timeout. pg_net is implicitly bounded by its batch
/// size (200 by default).
pub const MAX_CONCURRENT_REQUESTS_PER_HOST: usize = 200;

/// Number of per-host limiters kept before unused ones are forgotten.
pub const HOST_LIMITS_PRUNE_AT: usize = 1024;

/// How many times a request is retried on a fresh connection when the connection it was sent on
/// died before any response arrived (typically a pooled keep-alive connection the server had just
/// closed). Retries happen within the request's timeout.
pub const STALE_CONNECTION_RETRIES: u32 = 1;

/// Longest time the worker sleeps without waking up, even with nothing to do. Bounds how long
/// interrupts, config reloads and TTL cleanup can be delayed.
pub const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long to wait before retrying when the extension's tables are locked by another session.
pub const LOCKED_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// On `worker_restart()`, how long in-flight requests get to finish before the worker exits.
/// Requests that don't finish are sent again by the next worker.
pub const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// How long the tokio runtime gets to shut down when the worker process exits.
pub const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// Seconds the postmaster waits before restarting the worker after it exits.
pub const WORKER_RESTART_TIME: Duration = Duration::from_secs(1);
