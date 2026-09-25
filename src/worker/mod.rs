//! The background worker.
//!
//! # Threads
//!
//! The worker process has one main thread, which runs everything in this module and is the only
//! thread that calls into Postgres, and a few tokio threads that only send HTTP requests and
//! receive their responses (see `http`). They communicate like this:
//!
//! * main to tokio: requests are spawned as tasks on the runtime (`Handle::spawn`, which never
//!   blocks).
//! * tokio to main: each task sends exactly one response on an unbounded channel and then wakes
//!   the main thread through a socket (see `notify`). The channel never holds more than
//!   `MAX_IN_FLIGHT` responses, because at most that many requests are in flight.
//!
//! Tokio threads never wait on the main thread and the main thread never waits on a tokio thread
//! (it only polls the channel), so neither side can deadlock the other.
//!
//! # Pipeline
//!
//! Requests are claimed (`claimed_at` is set) in a short transaction and sent after it commits.
//! No transaction is open while they are in flight. Responses collect in a bucket, which is
//! committed when it has `RESPONSE_BUCKET_SIZE` responses or when its oldest response has waited
//! `RESPONSE_BUCKET_MAX_WAIT`. Committing a bucket retires its requests (deletes them from the
//! queue in the same statement that inserts their responses), which frees pipeline slots, and the
//! same transaction claims as many new requests as there are free slots. A slow response only
//! occupies its own slot; it doesn't hold back anything else.

mod db;
mod http;
mod notify;
mod types;

use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime};
use pgrx::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::consts::*;
use crate::shmem::{self, WorkerStatus};
use db::{Claimed, ExtensionTables, Plans, TickWork};
use http::Http;
use notify::WakeReceiver;
use types::HttpResponse;

const WORKER_NAME: &str = concat!("pg_rest ", env!("CARGO_PKG_VERSION"), " worker");
const APP_NAME: &str = concat!("pg_rest ", env!("CARGO_PKG_VERSION"));

/// The tokio runtime. Owned here rather than by `Worker` so that it is never dropped by
/// unwinding (dropping a runtime can block on its blocking pool), and so that `on_exit` can shut
/// it down before the process exits, whichever way it exits.
static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);

pub fn register() {
    BackgroundWorkerBuilder::new(WORKER_NAME)
        .set_type(WORKER_NAME)
        .set_library("pg_rest")
        .set_function("pg_rest_worker")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(WORKER_RESTART_TIME))
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_rest_worker(_arg: pg_sys::Datum) {
    unsafe {
        // SIGTERM (e.g. pg_terminate_backend) sets ProcDiePending and the latch; the next
        // CHECK_FOR_INTERRUPTS ends the worker. SIGINT (pg_cancel_backend) and SIGUSR1 keep the
        // handlers Postgres installs for database-connected workers.
        set_signal_handler(pg_sys::SIGTERM as i32, Some(signals::die));
        set_signal_handler(
            pg_sys::SIGHUP as i32,
            Some(signals::SignalHandlerForConfigReload),
        );
        pg_sys::BackgroundWorkerUnblockSignals();
    }

    BackgroundWorker::connect_worker_to_spi(Some(DATABASE_NAME), USERNAME);
    let app_name = std::ffi::CString::new(APP_NAME).expect("app name has no NUL");
    unsafe { pg_sys::pgstat_report_appname(app_name.as_ptr()) };

    let state = shmem::state();
    unsafe {
        state
            .latch
            .store(pg_sys::MyLatch as usize, Ordering::Release);
        pg_sys::on_proc_exit(Some(on_exit), pg_sys::Datum::from(0));
    }

    log!(
        "pg_rest worker started with max_in_flight={MAX_IN_FLIGHT}, \
         response_bucket_size={RESPONSE_BUCKET_SIZE}, \
         response_bucket_max_wait={RESPONSE_BUCKET_MAX_WAIT:?}, ttl={RESPONSE_TTL}, \
         database_name={DATABASE_NAME}"
    );

    let mut worker = Worker::new();

    state.set_status(WorkerStatus::Running);
    unsafe { pg_sys::pgstat_report_activity(pg_sys::BackendState::STATE_IDLE, std::ptr::null()) };

    worker.run();
    worker.shutdown();

    state.set_status(WorkerStatus::Exited);
    // Exiting with a failure code makes the postmaster restart the worker.
    unsafe { pg_sys::proc_exit(1) };
}

/// Postgres' own signal handlers. pgrx only exposes guarded Rust wrappers of these, which can't be
/// installed as C signal handlers, so they are declared here.
mod signals {
    #[cfg(not(feature = "pg19"))]
    extern "C-unwind" {
        pub fn die(signo: std::ffi::c_int);
        pub fn SignalHandlerForConfigReload(signo: std::ffi::c_int);
    }
    #[cfg(feature = "pg19")]
    extern "C-unwind" {
        pub fn die(signo: std::ffi::c_int, info: *const pgrx::pg_sys::pg_signal_info);
        pub fn SignalHandlerForConfigReload(
            signo: std::ffi::c_int,
            info: *const pgrx::pg_sys::pg_signal_info,
        );
    }
}

unsafe fn set_signal_handler(signo: i32, handler: pg_sys::pqsigfunc) {
    #[cfg(any(
        feature = "pg13",
        feature = "pg14",
        feature = "pg15",
        feature = "pg16",
        feature = "pg17"
    ))]
    pg_sys::pqsignal(signo, handler);
    #[cfg(any(feature = "pg18", feature = "pg19"))]
    pg_sys::pqsignal_be(signo, handler);
}

/// Runs on every kind of exit (normal, FATAL from CHECK_FOR_INTERRUPTS, ERROR). Stops the tokio
/// threads before the process exits so that none of them is running while exit handlers tear
/// down process state.
#[pg_guard]
unsafe extern "C-unwind" fn on_exit(_code: i32, _arg: pg_sys::Datum) {
    let state = shmem::state();
    state.latch.store(0, Ordering::Release);
    // Requests left in the queue are picked up by the next worker.
    state.should_wake.store(true, Ordering::Release);
    if state.status() == WorkerStatus::Running {
        state.set_status(WorkerStatus::Exited);
    }

    let runtime = RUNTIME.lock().ok().and_then(|mut rt| rt.take());
    if let Some(runtime) = runtime {
        runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    }
}

struct Worker {
    http: Http,
    responses: UnboundedReceiver<HttpResponse>,
    wake: WakeReceiver,
    plans: Option<Plans>,

    /// Requests claimed but not yet retired. Includes responses sitting in the bucket.
    in_flight: usize,
    /// Responses waiting to be committed.
    bucket: Vec<HttpResponse>,
    /// When the bucket must be committed even if it isn't full. Set when the first response
    /// enters an empty bucket.
    bucket_deadline: Option<Instant>,

    /// Whether there may be unclaimed requests in the queue. Set by wakes; cleared when a claim
    /// returns fewer rows than asked for.
    queue_maybe_nonempty: bool,
    /// Whether claims left behind by a previous worker still need to be reset.
    needs_claim_reset: bool,
    /// OID of the queue table the in-flight requests were claimed from.
    queue_oid: Option<pg_sys::Oid>,
    /// Bumped when in-flight requests are abandoned (the extension was dropped or recreated), so
    /// that their responses are ignored when they arrive.
    generation: u64,

    last_ttl_cleanup: Option<Instant>,
    /// Set when the extension's tables were locked; nothing is attempted before then.
    retry_at: Option<Instant>,

    /// What was last reported to pg_stat_activity.
    reported_running: bool,
    /// Whether table stats may be pending since the last forced flush.
    stats_pending: bool,
}

impl Worker {
    fn new() -> Worker {
        let runtime = http::build_runtime()
            .unwrap_or_else(|e| error!("pg_rest worker: could not start the tokio runtime: {e}"));
        let client = http::build_client()
            .unwrap_or_else(|e| error!("pg_rest worker: could not build the HTTP client: {e}"));
        let (waker, wake) = notify::wake_pair()
            .unwrap_or_else(|e| error!("pg_rest worker: could not create the wake socket: {e}"));
        let (tx, responses) = mpsc::unbounded_channel();

        let http = Http::new(runtime.handle().clone(), client, tx, waker);
        *RUNTIME.lock().expect("runtime mutex poisoned") = Some(runtime);

        Worker {
            http,
            responses,
            wake,
            plans: None,
            in_flight: 0,
            bucket: Vec::with_capacity(RESPONSE_BUCKET_SIZE),
            bucket_deadline: None,
            queue_maybe_nonempty: true,
            needs_claim_reset: true,
            queue_oid: None,
            generation: 0,
            last_ttl_cleanup: None,
            retry_at: None,
            reported_running: false,
            stats_pending: false,
        }
    }

    /// The main loop. Returns when a restart was requested.
    fn run(&mut self) {
        let state = shmem::state();
        loop {
            self.process_interrupts();

            if state.got_restart.swap(false, Ordering::AcqRel) {
                return;
            }
            if state.should_wake.swap(false, Ordering::AcqRel) {
                self.queue_maybe_nonempty = true;
            }

            self.collect_responses();

            let now = Instant::now();
            if self.retry_at.is_some_and(|t| now >= t) {
                self.retry_at = None;
            }

            let bucket_ready = self.bucket.len() >= RESPONSE_BUCKET_SIZE
                || self.bucket_deadline.is_some_and(|d| now >= d);
            let can_claim = self.queue_maybe_nonempty && self.in_flight < MAX_IN_FLIGHT;
            let ttl_due = self.ttl_due(now);

            if self.retry_at.is_none()
                && (bucket_ready || can_claim || ttl_due || self.needs_claim_reset)
            {
                if bucket_ready || can_claim {
                    self.report_activity(true);
                }
                self.tick(now, ttl_due);
                // Look again right away: more responses may have arrived, or the claim may have
                // been limited by free slots.
                continue;
            }

            self.report_activity(self.in_flight > 0);
            if self.in_flight == 0 && self.stats_pending {
                // Going idle: make sure no stats stay pending while nothing else would flush
                // them (PR #254 in pg_net).
                unsafe { pg_sys::pgstat_report_stat(true) };
                self.stats_pending = false;
            }

            self.wait(self.next_timeout(now));
        }
    }

    /// Processes interrupts and config reloads. Must be called regularly.
    fn process_interrupts(&self) {
        check_for_interrupts!();
        unsafe {
            if (&raw const pg_sys::ConfigReloadPending).read_volatile() != 0 {
                (&raw mut pg_sys::ConfigReloadPending).write_volatile(0);
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
    }

    /// Moves responses that have arrived into the bucket.
    fn collect_responses(&mut self) {
        // Drain the wake socket before the channel, so that a response sent after the drain
        // below triggers a new wake instead of being missed until the next timeout.
        self.wake.drain();
        while let Ok(response) = self.responses.try_recv() {
            if response.generation != self.generation {
                continue; // claimed from a table that no longer exists
            }
            self.add_to_bucket(response);
        }
    }

    fn add_to_bucket(&mut self, response: HttpResponse) {
        if self.bucket.is_empty() {
            self.bucket_deadline = Some(Instant::now() + RESPONSE_BUCKET_MAX_WAIT);
        }
        self.bucket.push(response);
    }

    fn ttl_due(&self, now: Instant) -> bool {
        self.last_ttl_cleanup
            .is_none_or(|t| now.duration_since(t) >= TTL_CLEANUP_INTERVAL)
    }

    /// One short transaction: retire up to a bucket of responses, claim requests for the free
    /// slots, and occasionally delete expired responses.
    fn tick(&mut self, now: Instant, ttl_due: bool) {
        let retire_count = self.bucket.len().min(RESPONSE_BUCKET_SIZE);

        let result = db::transaction(|| match db::lock_extension_tables() {
            ExtensionTables::Missing => TickResult::Missing,
            ExtensionTables::Locked => TickResult::Locked,
            ExtensionTables::Present { queue } => {
                let recreated = self.queue_oid.is_some_and(|oid| oid != queue);
                if recreated {
                    // The in-flight requests were claimed from a table that is gone. Their ids
                    // mean nothing in the new table.
                    self.abandon_in_flight();
                }
                self.queue_oid = Some(queue);

                let retire: &[HttpResponse] = if recreated {
                    &[]
                } else {
                    &self.bucket[..retire_count]
                };
                let free_slots = MAX_IN_FLIGHT - (self.in_flight - retire.len());
                let claim = if self.queue_maybe_nonempty {
                    free_slots
                } else {
                    0
                };

                let claimed = db::run_tick(
                    &mut self.plans,
                    TickWork {
                        // A new table has no stale claims.
                        reset_claims: self.needs_claim_reset && !recreated,
                        retire,
                        claim,
                        delete_expired: ttl_due,
                    },
                );
                TickResult::Done {
                    retired: retire.len(),
                    asked: claim,
                    claimed,
                }
            }
        });

        match result {
            TickResult::Missing => {
                if self.in_flight > 0 {
                    warning!(
                        "pg_rest worker: extension was dropped, discarding {} in-flight requests",
                        self.in_flight
                    );
                }
                self.abandon_in_flight();
                self.queue_oid = None;
                self.needs_claim_reset = false;
                self.queue_maybe_nonempty = false;
                self.last_ttl_cleanup = Some(now);
            }
            TickResult::Locked => {
                debug1!("pg_rest worker: extension tables are locked, retrying");
                self.retry_at = Some(now + LOCKED_RETRY_INTERVAL);
            }
            TickResult::Done {
                retired,
                asked,
                claimed,
            } => {
                self.needs_claim_reset = false;
                if ttl_due {
                    self.last_ttl_cleanup = Some(now);
                }

                self.bucket.drain(..retired);
                self.in_flight -= retired;
                if self.bucket.is_empty() {
                    self.bucket_deadline = None;
                }

                if claimed.len() < asked {
                    self.queue_maybe_nonempty = false;
                }

                // Send only after the claims are committed.
                self.in_flight += claimed.len();
                for c in claimed {
                    match c {
                        Claimed::Send(request) => self.http.send(request, self.generation),
                        Claimed::Rejected(id, outcome) => self.add_to_bucket(HttpResponse {
                            id,
                            generation: self.generation,
                            outcome,
                        }),
                    }
                }

                if retired > 0 || asked > 0 || ttl_due {
                    // Background workers must flush their own table stats (PR #254 in pg_net).
                    // Rate-limited internally when not forced.
                    unsafe { pg_sys::pgstat_report_stat(false) };
                    self.stats_pending = true;
                }
            }
        }
    }

    /// Forgets all in-flight requests. Responses already in flight are ignored when they arrive.
    fn abandon_in_flight(&mut self) {
        self.generation += 1;
        self.in_flight = 0;
        self.bucket.clear();
        self.bucket_deadline = None;
    }

    fn next_timeout(&self, now: Instant) -> Duration {
        let mut timeout = IDLE_POLL_INTERVAL;
        if let Some(deadline) = self.bucket_deadline {
            timeout = timeout.min(deadline.saturating_duration_since(now));
        }
        if let Some(retry_at) = self.retry_at {
            timeout = timeout.min(retry_at.saturating_duration_since(now));
        }
        if let Some(last) = self.last_ttl_cleanup {
            timeout = timeout.min((last + TTL_CLEANUP_INTERVAL).saturating_duration_since(now));
        }
        timeout
    }

    /// Sleeps until the latch is set (wake, signal), a response arrives, or `timeout` passes.
    fn wait(&self, timeout: Duration) {
        // Round up so that a sub-millisecond deadline doesn't turn into a busy loop.
        let timeout_ms = timeout.as_micros().div_ceil(1000).min(i64::MAX as u128) as _;
        unsafe {
            pg_sys::WaitLatchOrSocket(
                pg_sys::MyLatch,
                (pg_sys::WL_LATCH_SET
                    | pg_sys::WL_SOCKET_READABLE
                    | pg_sys::WL_TIMEOUT
                    | pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
                self.wake.fd(),
                timeout_ms,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);
        }
    }

    /// Reports the worker's state to pg_stat_activity (PR #255 in pg_net): active while any
    /// request is in flight or being committed, idle otherwise. Only transitions are reported.
    fn report_activity(&mut self, running: bool) {
        if running != self.reported_running {
            let state = if running {
                pg_sys::BackendState::STATE_RUNNING
            } else {
                pg_sys::BackendState::STATE_IDLE
            };
            unsafe { pg_sys::pgstat_report_activity(state, std::ptr::null()) };
            self.reported_running = running;
        }
    }

    /// Graceful exit after a restart request: gives in-flight requests up to `SHUTDOWN_GRACE`
    /// to finish and commits every response that has arrived. Requests still in flight after
    /// that are sent again by the next worker.
    fn shutdown(&mut self) {
        let deadline = Instant::now() + SHUTDOWN_GRACE;
        loop {
            self.process_interrupts();
            self.collect_responses();
            let now = Instant::now();
            if self.bucket.len() >= self.in_flight || now >= deadline {
                break;
            }
            self.wait(deadline - now);
        }

        while !self.bucket.is_empty() && self.retry_at.is_none() {
            let before = self.bucket.len();
            self.queue_maybe_nonempty = false; // don't claim anything new
            self.tick(Instant::now(), false);
            if self.bucket.len() == before {
                break;
            }
        }
        unsafe { pg_sys::pgstat_report_stat(true) };
    }
}

enum TickResult {
    Missing,
    Locked,
    Done {
        retired: usize,
        asked: usize,
        claimed: Vec<Claimed>,
    },
}
