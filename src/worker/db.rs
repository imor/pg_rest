//! Database side of the worker. Only called from the worker's main thread.
//!
//! This uses raw SPI rather than `pgrx::spi::Spi`: pgrx assigns a transaction id before every
//! writable statement, which would burn an XID on every tick even when nothing changes. Raw SPI
//! only assigns one when rows are actually modified, like pg_net.

use std::ffi::CStr;
use std::ptr::NonNull;
use std::time::Duration;

use pgrx::prelude::*;
use pgrx::{FromDatum, IntoDatum, JsonB};
use serde_json::Value;

use crate::consts::{MAX_TIMEOUT_MS, RESPONSE_TTL, TTL_CLEANUP_BATCH};
use crate::worker::types::{HttpRequest, HttpResponse, Method, Outcome};

const RESET_CLAIMS_SQL: &CStr = c"
    update rest.http_request_queue set claimed_at = null where claimed_at is not null";

/// Inserts responses and deletes their requests in one statement, so a request is never both
/// answered and claimable.
const RETIRE_SQL: &CStr = c"
    with ins as (
        insert into rest._http_response
            (id, status_code, content, headers, content_type, timed_out, error_msg)
        select * from unnest($1::int8[], $2::int4[], $3::text[], $4::jsonb[], $5::text[],
                             $6::bool[], $7::text[])
    )
    delete from rest.http_request_queue where id = any($1)";

/// `skip locked` so that the worker never waits on a row a user session has locked.
///
/// The ids are collected with `= any(array(...))` rather than a join: the subquery then runs
/// exactly once (as an InitPlan) and the update is a primary key lookup, whatever the planner's
/// row estimates. With a join, stale statistics (e.g. `reltuples = 0` after autovacuum ran on an
/// empty queue) can produce a nested loop that re-runs the locking subquery for every queue row.
const CLAIM_SQL: &CStr = c"
    update rest.http_request_queue
    set claimed_at = now()
    where id = any(array(
        select id
        from rest.http_request_queue
        where claimed_at is null
        order by id
        limit $1
        for update skip locked
    ))
    returning id, method::text, url, timeout_milliseconds, headers, body";

/// `= any(array(...))` for the same reason as `CLAIM_SQL`; it runs as a TID scan.
const DELETE_EXPIRED_SQL: &CStr = c"
    delete from rest._http_response
    where ctid = any(array(
        select ctid
        from rest._http_response
        where created < now() - $1::interval
        order by created
        limit $2
    ))";

/// Runs `f` in its own transaction and commits it. Errors raised by Postgres propagate as
/// panics; they end the worker, and the postmaster restarts it.
pub fn transaction<R>(f: impl FnOnce() -> R) -> R {
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }
    let result = f();
    unsafe {
        pg_sys::PopActiveSnapshot();
        pg_sys::CommitTransactionCommand();
    }
    result
}

pub enum ExtensionTables {
    /// The extension is installed and its tables are locked (`AccessShareLock`) until the end of
    /// the transaction.
    Present { queue: pg_sys::Oid },
    /// The extension isn't installed in the worker's database.
    Missing,
    /// Another session holds a conflicting lock, e.g. `DROP EXTENSION` is in progress.
    Locked,
}

/// Checks that the extension is installed and locks its tables so they can't be dropped while
/// the transaction runs. Must be called inside a transaction.
pub fn lock_extension_tables() -> ExtensionTables {
    unsafe {
        if pg_sys::get_extension_oid(c"pg_rest".as_ptr(), true) == pg_sys::InvalidOid {
            return ExtensionTables::Missing;
        }
        let schema = pg_sys::get_namespace_oid(c"rest".as_ptr(), true);
        if schema == pg_sys::InvalidOid {
            return ExtensionTables::Missing;
        }
        let queue = pg_sys::get_relname_relid(c"http_request_queue".as_ptr(), schema);
        let response = pg_sys::get_relname_relid(c"_http_response".as_ptr(), schema);
        if queue == pg_sys::InvalidOid || response == pg_sys::InvalidOid {
            return ExtensionTables::Missing;
        }
        let lockmode = pg_sys::AccessShareLock as pg_sys::LOCKMODE;
        if pg_sys::ConditionalLockRelationOid(queue, lockmode)
            && pg_sys::ConditionalLockRelationOid(response, lockmode)
        {
            ExtensionTables::Present { queue }
        } else {
            // Locks acquired so far are released at the end of the transaction.
            ExtensionTables::Locked
        }
    }
}

/// A plan saved with `SPI_keepplan`. Plans are kept for the life of the worker; the plan cache
/// revalidates them if the extension is dropped and recreated.
struct Plan(NonNull<pg_sys::_SPI_plan>);

impl Plan {
    /// Must be called while connected to SPI.
    fn prepare(sql: &CStr, argtypes: &[pg_sys::Oid]) -> Plan {
        let mut argtypes = argtypes.to_vec();
        unsafe {
            let plan =
                pg_sys::SPI_prepare(sql.as_ptr(), argtypes.len() as i32, argtypes.as_mut_ptr());
            let Some(plan) = NonNull::new(plan) else {
                let code = pg_sys::SPI_result;
                error!("SPI_prepare failed: {code}");
            };
            if pg_sys::SPI_keepplan(plan.as_ptr()) != 0 {
                error!("SPI_keepplan failed");
            }
            Plan(plan)
        }
    }

    /// Executes the plan and returns the number of rows processed. Must be called while
    /// connected to SPI. Argument datums must stay valid until this returns.
    fn execute(&self, args: &[Option<pg_sys::Datum>], expected: u32) -> u64 {
        let mut values: Vec<pg_sys::Datum> = args
            .iter()
            .map(|a| a.unwrap_or(pg_sys::Datum::from(0)))
            .collect();
        let nulls: Vec<std::ffi::c_char> = args
            .iter()
            .map(|a| if a.is_some() { b' ' } else { b'n' } as std::ffi::c_char)
            .collect();
        unsafe {
            let ret = pg_sys::SPI_execute_plan(
                self.0.as_ptr(),
                values.as_mut_ptr(),
                nulls.as_ptr(),
                false,
                0,
            );
            if ret != expected as i32 {
                error!("pg_rest worker query failed: SPI_execute_plan returned {ret}");
            }
            pg_sys::SPI_processed
        }
    }
}

pub struct Plans {
    reset_claims: Plan,
    retire: Plan,
    claim: Plan,
    delete_expired: Plan,
}

/// SPI connection for the duration of `f`. Everything palloc'd while connected is freed when
/// it returns, so results must be copied into Rust values inside `f`.
fn with_spi<R>(f: impl FnOnce() -> R) -> R {
    unsafe { pg_sys::SPI_connect() };
    let result = f();
    unsafe { pg_sys::SPI_finish() };
    result
}

/// What a claimed row turned into.
pub enum Claimed {
    /// Ready to send.
    Send(HttpRequest),
    /// Must not be sent; its (error) response is ready.
    Rejected(i64, Outcome),
}

/// The work of one tick, done inside a single transaction.
pub struct TickWork<'a> {
    pub reset_claims: bool,
    pub retire: &'a [HttpResponse],
    pub claim: usize,
    pub delete_expired: bool,
}

/// Runs a tick's statements. Must be called inside a transaction, after
/// `lock_extension_tables` returned `Present`.
pub fn run_tick(plans: &mut Option<Plans>, work: TickWork) -> Vec<Claimed> {
    with_spi(|| {
        let plans = plans.get_or_insert_with(|| Plans {
            reset_claims: Plan::prepare(RESET_CLAIMS_SQL, &[]),
            retire: Plan::prepare(
                RETIRE_SQL,
                &[
                    pg_sys::INT8ARRAYOID,
                    pg_sys::INT4ARRAYOID,
                    pg_sys::TEXTARRAYOID,
                    pg_sys::JSONBARRAYOID,
                    pg_sys::TEXTARRAYOID,
                    pg_sys::BOOLARRAYOID,
                    pg_sys::TEXTARRAYOID,
                ],
            ),
            claim: Plan::prepare(CLAIM_SQL, &[pg_sys::INT4OID]),
            delete_expired: Plan::prepare(DELETE_EXPIRED_SQL, &[pg_sys::TEXTOID, pg_sys::INT4OID]),
        });

        if work.reset_claims {
            let n = plans.reset_claims.execute(&[], pg_sys::SPI_OK_UPDATE);
            if n > 0 {
                log!("pg_rest worker: re-queued {n} requests claimed by a previous worker");
            }
        }

        if !work.retire.is_empty() {
            retire(&plans.retire, work.retire);
        }

        let claimed = if work.claim > 0 {
            claim(&plans.claim, work.claim)
        } else {
            Vec::new()
        };

        if work.delete_expired {
            let n = plans.delete_expired.execute(
                &[RESPONSE_TTL.into_datum(), TTL_CLEANUP_BATCH.into_datum()],
                pg_sys::SPI_OK_DELETE,
            );
            debug1!("pg_rest worker: deleted {n} expired responses");
        }

        claimed
    })
}

fn retire(plan: &Plan, responses: &[HttpResponse]) {
    let n = responses.len();
    let mut ids = Vec::with_capacity(n);
    let mut status_codes = Vec::with_capacity(n);
    let mut contents = Vec::with_capacity(n);
    let mut headers = Vec::with_capacity(n);
    let mut content_types = Vec::with_capacity(n);
    let mut timed_outs = Vec::with_capacity(n);
    let mut error_msgs = Vec::with_capacity(n);

    for response in responses {
        ids.push(response.id);
        match &response.outcome {
            Outcome::Success {
                status_code,
                headers: h,
                content_type,
                body,
            } => {
                status_codes.push(Some(*status_code));
                contents.push(body.clone());
                headers.push(Some(JsonB(Value::Object(h.clone()))));
                content_types.push(content_type.clone());
                timed_outs.push(Some(false));
                error_msgs.push(None::<String>);
            }
            Outcome::Failure {
                timed_out,
                error_msg,
            } => {
                status_codes.push(None);
                contents.push(None);
                headers.push(None);
                content_types.push(None);
                timed_outs.push(Some(*timed_out));
                error_msgs.push(Some(error_msg.clone()));
            }
        }
    }

    plan.execute(
        &[
            ids.into_datum(),
            status_codes.into_datum(),
            contents.into_datum(),
            headers.into_datum(),
            content_types.into_datum(),
            timed_outs.into_datum(),
            error_msgs.into_datum(),
        ],
        pg_sys::SPI_OK_DELETE,
    );
}

fn claim(plan: &Plan, limit: usize) -> Vec<Claimed> {
    let limit = i32::try_from(limit).unwrap_or(i32::MAX);
    let n = plan.execute(&[limit.into_datum()], pg_sys::SPI_OK_UPDATE_RETURNING);

    let mut claimed = Vec::with_capacity(n as usize);
    unsafe {
        let table = pg_sys::SPI_tuptable;
        let tupdesc = (*table).tupdesc;
        for i in 0..n as usize {
            let tuple = *(*table).vals.add(i);
            let get = |col: i32| {
                let mut isnull = false;
                let datum = pg_sys::SPI_getbinval(tuple, tupdesc, col, &mut isnull);
                (datum, isnull)
            };
            let (d, null) = get(1);
            let id = i64::from_datum(d, null).expect("request id is not null");
            let (d, null) = get(2);
            let method = String::from_datum(d, null).expect("method is not null");
            let (d, null) = get(3);
            let url = String::from_datum(d, null).expect("url is not null");
            let (d, null) = get(4);
            let timeout_ms = i32::from_datum(d, null).expect("timeout_milliseconds is not null");
            let (d, null) = get(5);
            let headers = JsonB::from_datum(d, null);
            let (d, null) = get(6);
            let body = Vec::<u8>::from_datum(d, null);

            claimed.push(to_request(id, &method, url, timeout_ms, headers, body));
        }
    }
    claimed
}

fn to_request(
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
#[pg_schema]
mod tests {
    use super::*;

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
