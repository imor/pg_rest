//! Lets tokio threads wake the worker's main thread without calling any Postgres API.
//!
//! Tokio threads must not call `SetLatch` (or anything else in Postgres), so instead they write a
//! byte to one end of a socket pair. The main thread waits on the other end together with its
//! latch using `WaitLatchOrSocket`.

use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Held by the main thread.
pub struct WakeReceiver {
    sock: UnixStream,
    pending: Arc<AtomicBool>,
}

/// Cloned into every request task.
#[derive(Clone)]
pub struct WakeSender {
    sock: Arc<UnixStream>,
    pending: Arc<AtomicBool>,
}

pub fn wake_pair() -> std::io::Result<(WakeSender, WakeReceiver)> {
    let (rx, tx) = UnixStream::pair()?;
    rx.set_nonblocking(true)?;
    tx.set_nonblocking(true)?;
    let pending = Arc::new(AtomicBool::new(false));
    Ok((
        WakeSender {
            sock: Arc::new(tx),
            pending: pending.clone(),
        },
        WakeReceiver { sock: rx, pending },
    ))
}

impl WakeSender {
    /// Wakes the main thread. Must be called after the message it should see has been sent.
    ///
    /// Only the first wake after the main thread last drained writes a byte, so a burst of
    /// responses costs one syscall.
    pub fn wake(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            // `WouldBlock` means the buffer is full, so a wake is already pending. Other errors
            // mean the main thread is gone. Either way there's nothing to do.
            let _ = (&*self.sock).write(&[1]);
        }
    }
}

impl WakeReceiver {
    pub fn fd(&self) -> RawFd {
        self.sock.as_raw_fd()
    }

    /// Consumes pending wakes. Call this *before* draining the channel: a sender that sends
    /// after the channel was drained sees `pending == false` and writes a new byte, so the next
    /// wait returns immediately and no wake is lost.
    pub fn drain(&self) {
        let mut buf = [0u8; 256];
        loop {
            match (&self.sock).read(&mut buf) {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => break, // WouldBlock: drained
            }
        }
        // An RMW rather than a store: if a sender's `swap(true)` came first, this synchronizes
        // with it, so the message the sender sent before waking is visible to the drain that
        // follows. If it comes after, the sender sees `false` and writes a byte.
        self.pending.swap(false, Ordering::AcqRel);
    }
}
