//! Worker state shared between the background worker and user backends.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use pgrx::prelude::*;
use pgrx::{PGRXSharedMemory, PgAtomic};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    NotYet = 1,
    Running = 2,
    Exited = 3,
}

#[repr(C)]
pub struct WorkerState {
    /// Set by user backends at commit time when new requests were enqueued. Cleared by the worker
    /// when it goes looking for requests.
    pub should_wake: AtomicBool,
    /// Set by `worker_restart()`. The worker exits gracefully and the postmaster restarts it.
    pub got_restart: AtomicBool,
    /// A `WorkerStatus`.
    pub status: AtomicU32,
    /// `*mut pg_sys::Latch` of the running worker, or null. The latch lives in the worker's
    /// `PGPROC`, which is in shared memory, so the address is valid in every backend.
    pub latch: AtomicUsize,
}

// SAFETY: only contains atomics, which are valid to share between processes.
unsafe impl PGRXSharedMemory for WorkerState {}

impl Default for WorkerState {
    fn default() -> Self {
        Self {
            // Start with a wake pending so that requests enqueued before the worker started, or
            // while it was restarting, are picked up.
            should_wake: AtomicBool::new(true),
            got_restart: AtomicBool::new(false),
            status: AtomicU32::new(WorkerStatus::NotYet as u32),
            latch: AtomicUsize::new(0),
        }
    }
}

pub static WORKER_STATE: PgAtomic<WorkerState> = unsafe { PgAtomic::new(c"pg_rest worker state") };

pub fn init() {
    pgrx::pg_shmem_init!(WORKER_STATE);
}

pub fn state() -> &'static WorkerState {
    WORKER_STATE.get()
}

impl WorkerState {
    pub fn status(&self) -> WorkerStatus {
        match self.status.load(Ordering::Acquire) {
            2 => WorkerStatus::Running,
            3 => WorkerStatus::Exited,
            _ => WorkerStatus::NotYet,
        }
    }

    pub fn set_status(&self, status: WorkerStatus) {
        self.status.store(status as u32, Ordering::Release);
    }

    /// Sets the worker's latch, if a worker is running.
    pub fn set_latch(&self) {
        let latch = self.latch.load(Ordering::Acquire) as *mut pg_sys::Latch;
        if !latch.is_null() {
            unsafe { pg_sys::SetLatch(latch) };
        }
    }
}
