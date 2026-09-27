//! In-memory storage for requests and responses, replacing the tables.
//!
//! Everything lives in one named dynamic shared memory segment (`GetNamedDSMSegment`), which
//! every backend and the worker attach to on first use:
//!
//! * a control block with an LWLock that protects everything below, the id counter and the ring
//!   state;
//! * a ring of committed request ids, in the order the worker should claim them;
//! * `QUEUE_CAPACITY` request slots and `RESPONSE_CAPACITY` response slots. A request or
//!   response with id `n` lives in slot `n % capacity`, so lookups are O(1). A new response
//!   evicts whatever older response occupies its slot, which bounds the store to (roughly) the
//!   most recent `RESPONSE_CAPACITY` responses. A request that would land on a live request's
//!   slot makes the enqueueing transaction fail ("request queue is full").
//!
//! Variable-length data (URL, headers, bodies) is kept in a DSA area, as encoded byte strings
//! referenced from the slots by `dsa_pointer`.
//!
//! Nothing is durable: a Postgres restart loses all queued requests and stored responses.

use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicI64, Ordering};

use pgrx::prelude::*;

use crate::consts::{QUEUE_CAPACITY, RESPONSE_CAPACITY};

const SEGMENT_NAME: &std::ffi::CStr = c"pg_rest";
const DSA_INIT_SEGMENT_SIZE: usize = 1024 * 1024;
const DSA_MAX_SEGMENT_SIZE: usize = 1 << 40;
const DSA_ALLOC_NO_OOM: i32 = 0x02;

const FREE: u8 = 0;
const PENDING: u8 = 1;
const CLAIMED: u8 = 2;

#[repr(C)]
struct Control {
    lock: pg_sys::LWLock,
    tranche_id: i32,
    dsa_handle: pg_sys::dsa_handle,
    next_id: AtomicI64,
    ring_head: usize,
    ring_len: usize,
    responses: usize,
    ttl_cursor: usize,
    /// Bumped by `clear()`. Requests are claimed with the current generation, and responses from
    /// an older one are discarded: their ids may have been reused since.
    generation: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RequestSlot {
    id: i64,
    state: u8,
    claimed_at: pg_sys::TimestampTz,
    payload: pg_sys::dsa_pointer,
    len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ResponseSlot {
    id: i64,
    created: pg_sys::TimestampTz,
    payload: pg_sys::dsa_pointer,
    len: usize,
}

const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

const RING_OFFSET: usize = align8(size_of::<Control>());
const REQUESTS_OFFSET: usize = RING_OFFSET + QUEUE_CAPACITY * size_of::<i64>();
const RESPONSES_OFFSET: usize = REQUESTS_OFFSET + QUEUE_CAPACITY * size_of::<RequestSlot>();
const SEGMENT_SIZE: usize = RESPONSES_OFFSET + RESPONSE_CAPACITY * size_of::<ResponseSlot>();

/// This backend's mapping of the shared state.
struct Mem {
    ctl: *mut Control,
    ring: *mut i64,
    requests: *mut RequestSlot,
    responses: *mut ResponseSlot,
    area: *mut pg_sys::dsa_area,
}

thread_local! {
    // Only ever touched from the backend's (or worker's) main thread.
    static MEM: std::cell::OnceCell<&'static Mem> = const { std::cell::OnceCell::new() };
}

#[pg_guard]
unsafe extern "C-unwind" fn init_segment(ptr: *mut c_void) {
    ptr::write_bytes(ptr.cast::<u8>(), 0, SEGMENT_SIZE);
    let ctl = ptr.cast::<Control>();
    let tranche = pg_sys::LWLockNewTrancheId();
    pg_sys::LWLockInitialize(&raw mut (*ctl).lock, tranche);
    (*ctl).tranche_id = tranche;
    (*ctl).next_id = AtomicI64::new(1);
}

fn mem() -> &'static Mem {
    MEM.with(|m| *m.get_or_init(|| Box::leak(Box::new(attach()))))
}

/// Maps the shared segment and the DSA area into this process, creating them on first use.
fn attach() -> Mem {
    unsafe {
        let mut found = false;
        let base = pg_sys::GetNamedDSMSegment(
            SEGMENT_NAME.as_ptr(),
            SEGMENT_SIZE,
            Some(init_segment),
            &mut found,
        )
        .cast::<u8>();
        let ctl = base.cast::<Control>();
        pg_sys::LWLockRegisterTranche((*ctl).tranche_id, SEGMENT_NAME.as_ptr());

        // Create the DSA area on first use, attach to it otherwise. Mappings live for the life
        // of the process.
        pg_sys::LWLockAcquire(&raw mut (*ctl).lock, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let old = pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
        let area = if (*ctl).dsa_handle == 0 {
            let area = pg_sys::dsa_create_ext(
                (*ctl).tranche_id,
                DSA_INIT_SEGMENT_SIZE,
                DSA_MAX_SEGMENT_SIZE,
            );
            pg_sys::dsa_pin(area);
            (*ctl).dsa_handle = pg_sys::dsa_get_handle(area);
            area
        } else {
            pg_sys::dsa_attach((*ctl).dsa_handle)
        };
        pg_sys::dsa_pin_mapping(area);
        pg_sys::MemoryContextSwitchTo(old);
        pg_sys::LWLockRelease(&raw mut (*ctl).lock);

        Mem {
            ctl,
            ring: base.add(RING_OFFSET).cast(),
            requests: base.add(REQUESTS_OFFSET).cast(),
            responses: base.add(RESPONSES_OFFSET).cast(),
            area,
        }
    }
}

impl Mem {
    fn with_lock<R>(&self, exclusive: bool, f: impl FnOnce() -> R) -> R {
        let mode = if exclusive {
            pg_sys::LWLockMode::LW_EXCLUSIVE
        } else {
            pg_sys::LWLockMode::LW_SHARED
        };
        unsafe { pg_sys::LWLockAcquire(&raw mut (*self.ctl).lock, mode) };
        let result = f();
        unsafe { pg_sys::LWLockRelease(&raw mut (*self.ctl).lock) };
        result
    }

    // Shared memory: only called inside `with_lock`, which serializes writers.
    #[allow(clippy::mut_from_ref)]
    fn ctl(&self) -> &mut Control {
        unsafe { &mut *self.ctl }
    }

    // Shared memory: only called inside `with_lock`, which serializes writers.
    #[allow(clippy::mut_from_ref)]
    fn request(&self, id: i64) -> &mut RequestSlot {
        unsafe { &mut *self.requests.add(id as usize % QUEUE_CAPACITY) }
    }

    // Shared memory: only called inside `with_lock`, which serializes writers.
    #[allow(clippy::mut_from_ref)]
    fn response(&self, id: i64) -> &mut ResponseSlot {
        unsafe { &mut *self.responses.add(id as usize % RESPONSE_CAPACITY) }
    }

    /// Copies `bytes` into the DSA area. `None` if the area is out of memory.
    fn store(&self, bytes: &[u8]) -> Option<pg_sys::dsa_pointer> {
        unsafe {
            let dp = pg_sys::dsa_allocate_extended(self.area, bytes.len().max(1), DSA_ALLOC_NO_OOM);
            if dp == 0 {
                return None;
            }
            let dst = pg_sys::dsa_get_address(self.area, dp).cast::<u8>();
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
            Some(dp)
        }
    }

    fn load(&self, dp: pg_sys::dsa_pointer, len: usize) -> Vec<u8> {
        unsafe {
            let src = pg_sys::dsa_get_address(self.area, dp).cast::<u8>();
            std::slice::from_raw_parts(src, len).to_vec()
        }
    }

    fn free(&self, dps: &[pg_sys::dsa_pointer]) {
        for &dp in dps {
            unsafe { pg_sys::dsa_free(self.area, dp) };
        }
    }

    fn ring_push_back(&self, id: i64) {
        let ctl = self.ctl();
        let at = (ctl.ring_head + ctl.ring_len) % QUEUE_CAPACITY;
        unsafe { *self.ring.add(at) = id };
        ctl.ring_len += 1;
    }

    fn ring_push_front(&self, id: i64) {
        let ctl = self.ctl();
        ctl.ring_head = (ctl.ring_head + QUEUE_CAPACITY - 1) % QUEUE_CAPACITY;
        unsafe { *self.ring.add(ctl.ring_head) = id };
        ctl.ring_len += 1;
    }

    fn ring_pop_front(&self) -> Option<i64> {
        let ctl = self.ctl();
        if ctl.ring_len == 0 {
            return None;
        }
        let id = unsafe { *self.ring.add(ctl.ring_head) };
        ctl.ring_head = (ctl.ring_head + 1) % QUEUE_CAPACITY;
        ctl.ring_len -= 1;
        Some(id)
    }
}

fn now() -> pg_sys::TimestampTz {
    unsafe { pg_sys::GetCurrentTimestamp() }
}

/// A new request id.
pub fn next_id() -> i64 {
    mem().ctl().next_id.fetch_add(1, Ordering::Relaxed)
}

/// Makes `requests` (id, encoded payload) visible to the worker, all or nothing.
pub fn enqueue(requests: &[(i64, Vec<u8>)]) -> Result<(), String> {
    let mem = mem();
    let mut payloads = Vec::with_capacity(requests.len());
    for (_, bytes) in requests {
        match mem.store(bytes) {
            Some(dp) => payloads.push(dp),
            None => {
                mem.free(&payloads);
                return Err("out of shared memory for queued requests".into());
            }
        }
    }
    let result = mem.with_lock(true, || {
        let ctl = mem.ctl();
        if ctl.ring_len + requests.len() > QUEUE_CAPACITY
            || requests
                .iter()
                .any(|(id, _)| mem.request(*id).state != FREE)
        {
            return Err(format!("request queue is full ({QUEUE_CAPACITY} requests)"));
        }
        for ((id, bytes), dp) in requests.iter().zip(&payloads) {
            *mem.request(*id) = RequestSlot {
                id: *id,
                state: PENDING,
                claimed_at: 0,
                payload: *dp,
                len: bytes.len(),
            };
            mem.ring_push_back(*id);
        }
        Ok(())
    });
    if result.is_err() {
        mem.free(&payloads);
    }
    result
}

/// Claims up to `max` pending requests, oldest first. Returns the current generation and the
/// requests' ids and encoded payloads.
pub fn claim(max: usize) -> (u64, Vec<(i64, Vec<u8>)>) {
    let mem = mem();
    let now = now();
    mem.with_lock(true, || {
        let mut claimed = Vec::new();
        while claimed.len() < max {
            let Some(id) = mem.ring_pop_front() else {
                break;
            };
            let slot = mem.request(id);
            // Skip ids whose request was deleted after it was queued.
            if slot.id == id && slot.state == PENDING {
                slot.state = CLAIMED;
                slot.claimed_at = now;
                claimed.push((id, mem.load(slot.payload, slot.len)));
            }
        }
        (mem.ctl().generation, claimed)
    })
}

/// Stores responses (id, generation it was claimed in, encoded payload) and removes their
/// requests. Responses claimed before the last `clear()` are discarded, since their ids may
/// have been reused. A response is still stored if its request was deleted while in flight,
/// as it was with tables.
pub fn retire(responses: &[(i64, u64, Vec<u8>)]) {
    let mem = mem();
    let now = now();
    let mut stored = Vec::with_capacity(responses.len());
    for (id, generation, bytes) in responses {
        // Out of memory: the response is lost, but the request is still retired.
        stored.push((*id, *generation, mem.store(bytes), bytes.len()));
    }
    let mut to_free = Vec::new();
    mem.with_lock(true, || {
        let ctl = mem.ctl();
        for (id, generation, dp, len) in &stored {
            if *generation != ctl.generation {
                to_free.extend(*dp);
                continue;
            }
            let request = mem.request(*id);
            if request.id == *id && request.state != FREE {
                to_free.push(request.payload);
                *request = RequestSlot {
                    id: 0,
                    state: FREE,
                    claimed_at: 0,
                    payload: 0,
                    len: 0,
                };
            }
            if let Some(dp) = dp {
                let slot = mem.response(*id);
                if slot.id != 0 {
                    to_free.push(slot.payload);
                } else {
                    ctl.responses += 1;
                }
                *slot = ResponseSlot {
                    id: *id,
                    created: now,
                    payload: *dp,
                    len: *len,
                };
            }
        }
    });
    mem.free(&to_free);
}

/// Makes requests claimed by a previous worker claimable again, ahead of the pending ones.
/// Returns how many were reset.
pub fn reset_claims() -> usize {
    let mem = mem();
    mem.with_lock(true, || {
        let mut ids: Vec<i64> = (0..QUEUE_CAPACITY)
            .map(|i| unsafe { &mut *mem.requests.add(i) })
            .filter(|s| s.state == CLAIMED)
            .map(|s| {
                s.state = PENDING;
                s.claimed_at = 0;
                s.id
            })
            .collect();
        ids.sort_unstable();
        for id in ids.iter().rev() {
            mem.ring_push_front(*id);
        }
        ids.len()
    })
}

/// Deletes up to one chunk of responses older than `ttl_micros`, scanning a window of slots
/// per call so that the lock is held briefly. Returns how many were deleted.
pub fn expire(ttl_micros: i64, window: usize) -> usize {
    let mem = mem();
    let cutoff = now() - ttl_micros;
    let mut to_free = Vec::new();
    mem.with_lock(true, || {
        let ctl = mem.ctl();
        for _ in 0..window.min(RESPONSE_CAPACITY) {
            let slot = unsafe { &mut *mem.responses.add(ctl.ttl_cursor) };
            if slot.id != 0 && slot.created < cutoff {
                to_free.push(slot.payload);
                *slot = ResponseSlot {
                    id: 0,
                    created: 0,
                    payload: 0,
                    len: 0,
                };
                ctl.responses -= 1;
            }
            ctl.ttl_cursor = (ctl.ttl_cursor + 1) % RESPONSE_CAPACITY;
        }
    });
    mem.free(&to_free);
    to_free.len()
}

pub struct RequestRow {
    pub id: i64,
    pub claimed_at: Option<pg_sys::TimestampTz>,
    pub payload: Vec<u8>,
}

pub struct ResponseRow {
    pub id: i64,
    pub created: pg_sys::TimestampTz,
    pub payload: Vec<u8>,
}

/// Every queued (pending or claimed) request, by id.
pub fn requests() -> Vec<RequestRow> {
    let mem = mem();
    let mut rows = mem.with_lock(false, || {
        (0..QUEUE_CAPACITY)
            .map(|i| unsafe { &*mem.requests.add(i) })
            .filter(|s| s.state != FREE)
            .map(|s| RequestRow {
                id: s.id,
                claimed_at: (s.state == CLAIMED).then_some(s.claimed_at),
                payload: mem.load(s.payload, s.len),
            })
            .collect::<Vec<_>>()
    });
    rows.sort_unstable_by_key(|r| r.id);
    rows
}

/// Every stored response, by id.
pub fn responses() -> Vec<ResponseRow> {
    let mem = mem();
    let mut rows = mem.with_lock(false, || {
        (0..RESPONSE_CAPACITY)
            .map(|i| unsafe { &*mem.responses.add(i) })
            .filter(|s| s.id != 0)
            .map(|s| ResponseRow {
                id: s.id,
                created: s.created,
                payload: mem.load(s.payload, s.len),
            })
            .collect::<Vec<_>>()
    });
    rows.sort_unstable_by_key(|r| r.id);
    rows
}

/// The response to request `id`, if it is stored.
pub fn response(id: i64) -> Option<ResponseRow> {
    let mem = mem();
    mem.with_lock(false, || {
        let slot = mem.response(id);
        (slot.id == id && id != 0).then(|| ResponseRow {
            id,
            created: slot.created,
            payload: mem.load(slot.payload, slot.len),
        })
    })
}

/// How many responses are stored.
pub fn response_count() -> usize {
    let mem = mem();
    mem.with_lock(false, || mem.ctl().responses)
}

pub fn delete_request(id: i64) -> bool {
    let mem = mem();
    let freed = mem.with_lock(true, || {
        let slot = mem.request(id);
        (slot.id == id && slot.state != FREE).then(|| {
            let dp = slot.payload;
            *slot = RequestSlot {
                id: 0,
                state: FREE,
                claimed_at: 0,
                payload: 0,
                len: 0,
            };
            dp
        })
    });
    if let Some(dp) = freed {
        mem.free(&[dp]);
    }
    freed.is_some()
}

pub fn delete_response(id: i64) -> bool {
    let mem = mem();
    let freed = mem.with_lock(true, || {
        let slot = mem.response(id);
        (slot.id == id && id != 0).then(|| {
            let dp = slot.payload;
            *slot = ResponseSlot {
                id: 0,
                created: 0,
                payload: 0,
                len: 0,
            };
            mem.ctl().responses -= 1;
            dp
        })
    });
    if let Some(dp) = freed {
        mem.free(&[dp]);
    }
    freed.is_some()
}

/// Forgets every queued request and stored response, and restarts ids at 1 (as recreating the
/// extension's tables and sequence did on main). Used when the extension is created.
pub fn clear() {
    let mem = mem();
    let mut to_free = Vec::new();
    mem.with_lock(true, || {
        for i in 0..QUEUE_CAPACITY {
            let slot = unsafe { &mut *mem.requests.add(i) };
            if slot.state != FREE {
                to_free.push(slot.payload);
                *slot = RequestSlot {
                    id: 0,
                    state: FREE,
                    claimed_at: 0,
                    payload: 0,
                    len: 0,
                };
            }
        }
        for i in 0..RESPONSE_CAPACITY {
            let slot = unsafe { &mut *mem.responses.add(i) };
            if slot.id != 0 {
                to_free.push(slot.payload);
                *slot = ResponseSlot {
                    id: 0,
                    created: 0,
                    payload: 0,
                    len: 0,
                };
            }
        }
        let ctl = mem.ctl();
        ctl.ring_head = 0;
        ctl.ring_len = 0;
        ctl.responses = 0;
        ctl.next_id.store(1, Ordering::Relaxed);
        ctl.generation += 1;
    });
    mem.free(&to_free);
}

/// Length-prefixed encoding of the fields stored in shared memory.
pub mod codec {
    pub struct Writer(pub Vec<u8>);

    impl Writer {
        pub fn new() -> Self {
            Writer(Vec::with_capacity(256))
        }
        pub fn i64(&mut self, v: i64) {
            self.0.extend_from_slice(&v.to_le_bytes());
        }
        pub fn bytes(&mut self, v: Option<&[u8]>) {
            match v {
                None => self.i64(-1),
                Some(b) => {
                    self.i64(b.len() as i64);
                    self.0.extend_from_slice(b);
                }
            }
        }
        pub fn str(&mut self, v: Option<&str>) {
            self.bytes(v.map(str::as_bytes));
        }
    }

    pub struct Reader<'a>(pub &'a [u8]);

    impl Reader<'_> {
        pub fn i64(&mut self) -> i64 {
            let (head, rest) = self.0.split_at(8);
            self.0 = rest;
            i64::from_le_bytes(head.try_into().unwrap())
        }
        pub fn bytes(&mut self) -> Option<Vec<u8>> {
            let len = self.i64();
            if len < 0 {
                return None;
            }
            let (head, rest) = self.0.split_at(len as usize);
            self.0 = rest;
            Some(head.to_vec())
        }
        pub fn str(&mut self) -> Option<String> {
            self.bytes()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        }
    }
}

/// A request as stored in shared memory.
pub struct QueuedRequest {
    pub method: String,
    pub url: String,
    /// JSON text of the headers object.
    pub headers: Option<String>,
    pub body: Option<Vec<u8>>,
    pub timeout_milliseconds: i32,
}

impl QueuedRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = codec::Writer::new();
        w.str(Some(&self.method));
        w.str(Some(&self.url));
        w.str(self.headers.as_deref());
        w.bytes(self.body.as_deref());
        w.i64(self.timeout_milliseconds as i64);
        w.0
    }

    pub fn decode(bytes: &[u8]) -> QueuedRequest {
        let mut r = codec::Reader(bytes);
        QueuedRequest {
            method: r.str().unwrap_or_default(),
            url: r.str().unwrap_or_default(),
            headers: r.str(),
            body: r.bytes(),
            timeout_milliseconds: r.i64() as i32,
        }
    }
}

/// A response as stored in shared memory. Mirrors the columns of the old `_http_response` table.
pub struct StoredResponse {
    pub status_code: Option<i32>,
    pub content_type: Option<String>,
    /// JSON text of the headers object.
    pub headers: Option<String>,
    pub content: Option<String>,
    pub timed_out: Option<bool>,
    pub error_msg: Option<String>,
}

impl StoredResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = codec::Writer::new();
        w.i64(self.status_code.map_or(i64::MIN, i64::from));
        w.str(self.content_type.as_deref());
        w.str(self.headers.as_deref());
        w.str(self.content.as_deref());
        w.i64(self.timed_out.map_or(-1, i64::from));
        w.str(self.error_msg.as_deref());
        w.0
    }

    pub fn decode(bytes: &[u8]) -> StoredResponse {
        let mut r = codec::Reader(bytes);
        let status = r.i64();
        let content_type = r.str();
        let headers = r.str();
        let content = r.str();
        let timed_out = r.i64();
        StoredResponse {
            status_code: (status != i64::MIN).then_some(status as i32),
            content_type,
            headers,
            content,
            timed_out: (timed_out >= 0).then_some(timed_out == 1),
            error_msg: r.str(),
        }
    }
}
