//! Emscripten I/O reactor: a drop-in replacement for the mio-backed
//! [`super::driver`] where `mio` is unavailable. Emscripten's `sockfs`/websocket
//! sockets are event-driven; this reactor owns the process-global
//! `emscripten_set_socket_*_callback` handlers and turns each event into a
//! readiness update + waker wake on the matching [`ScheduledIo`], routing
//! through the driver's `unpark` (the same contract as timers). Readiness is
//! delivered by the JS loop, so `park`/`park_timeout` are no-ops.
//!
//! Two readiness sources cooperate. The sockfs callbacks are the fast path:
//! each carries the affected fd, so it sets that fd's readiness directly and
//! re-drives. The slow path is [`Handle::poll_ready`], one non-blocking
//! `poll(2)` over the whole registered set, which the kernel runs at its
//! progression cliff — the emscripten analogue of parking on `epoll`. It exists
//! for readiness the callbacks can't carry: a listener's accept event arrives
//! on the accepted peer's fd, not the listener's, and a `block_on`-parked fd has
//! no callback pending. A fd is registered once (duplicate registration
//! rejected, like mio).

use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::loom::sync::Mutex;
use crate::runtime::driver;
use super::{registration_set, IoDriverMetrics, RegistrationSet, ScheduledIo, Tick};

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};
use std::fmt;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use crate::emscripten::ffi::{
    emscripten_set_socket_close_callback, emscripten_set_socket_connection_callback,
    emscripten_set_socket_error_callback, emscripten_set_socket_message_callback,
    emscripten_set_socket_open_callback,
};
use crate::emscripten::event_loop::drive;

/// The concrete [`PollEvented`](crate::io::PollEvented) source on emscripten —
/// the analogue of `mio::net::TcpStream`. It owns a non-blocking socket fd
/// (closed on drop), is registered with the reactor by that fd, and performs the
/// stream `recv`/`send` syscalls through `io::Read`/`io::Write` on `&Source` (so
/// `PollEvented` can drive `AsyncRead`/`AsyncWrite`).
#[cfg(feature = "net")]
#[derive(Debug)]
pub(crate) struct Source {
    fd: RawFd,
}

#[cfg(feature = "net")]
impl Source {
    /// Takes ownership of `fd`, closing it on drop.
    pub(crate) fn from_raw_fd(fd: RawFd) -> Source {
        Source { fd }
    }

    /// Releases ownership of the fd without closing it (for `into_std`-style
    /// handoff back to a `std` socket).
    pub(crate) fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        std::mem::forget(self);
        fd
    }
}

#[cfg(feature = "net")]
impl AsRawFd for Source {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

#[cfg(feature = "net")]
impl Drop for Source {
    fn drop(&mut self) {
        // SAFETY: we own `fd` for the lifetime of `Source`.
        unsafe { libc::close(self.fd) };
    }
}

#[cfg(feature = "net")]
impl io::Read for &Source {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // SAFETY: `recv` reads up to `buf.len()` bytes into `buf`.
        let n = unsafe {
            libc::recv(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }
}

#[cfg(feature = "net")]
impl io::Write for &Source {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // SAFETY: `send` writes up to `buf.len()` bytes from `buf`.
        let n = unsafe {
            libc::send(self.fd, buf.as_ptr() as *const libc::c_void, buf.len(), 0)
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        // `IoSlice` is ABI-compatible with `iovec`. sockfs routes `writev`
        // through its stream write op.
        let n = unsafe {
            libc::writev(self.fd, bufs.as_ptr() as *const libc::iovec, bufs.len() as libc::c_int)
        };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// I/O driver. Inert on emscripten: all progress is callback-driven.
pub(crate) struct Driver {
    _private: (),
}

/// A reference to the I/O reactor used to register/deregister fds.
pub(crate) struct Handle {
    registrations: RegistrationSet,
    synced: Mutex<registration_set::Synced>,
    pub(crate) metrics: IoDriverMetrics,
}

/// This thread's I/O reactor state: the fd→`ScheduledIo` map the global socket
/// callbacks consult, the one-time callback-install latch, and reused `poll(2)`
/// scratch. Single-thread, so the thread-local `RefCell` below is sound and
/// keeps `Handle: Send + Sync`.
struct Reactor {
    /// Registered socket fds and their readiness slots.
    fds: HashMap<RawFd, Arc<ScheduledIo>>,
    /// Whether the process-global socket callbacks have been installed.
    callbacks_installed: bool,
    /// Reused `poll_ready` scratch, kept to avoid reallocating on every cliff
    /// `poll(2)`; taken out for the duration of a poll, then returned empty.
    poll_entries: Vec<(RawFd, Arc<ScheduledIo>)>,
    poll_fds: Vec<libc::pollfd>,
}

thread_local! {
    static REACTOR: RefCell<Reactor> = RefCell::new(Reactor::new());
}

impl Reactor {
    fn new() -> Reactor {
        Reactor {
            fds: HashMap::new(),
            callbacks_installed: false,
            poll_entries: Vec::new(),
            poll_fds: Vec::new(),
        }
    }

    /// Run `f` with this thread's reactor.
    fn with<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
        REACTOR.with(|r| f(&mut r.borrow_mut()))
    }

    /// Install the process-global readiness callbacks, once. Done lazily on the
    /// first fd registration rather than at driver build: emscripten only links
    /// its socket subsystem (and these `emscripten_set_socket_*_callback` entry
    /// points) when the program uses sockets, so installing eagerly aborts a
    /// net-enabled program that only ever resolves names. By the first
    /// registration a socket fd exists, so the subsystem is present.
    fn install_callbacks(&mut self) {
        if self.callbacks_installed {
            return;
        }
        self.callbacks_installed = true;
        // SAFETY: process-global callback registration; the handlers are valid
        // for the process lifetime.
        unsafe {
            emscripten_set_socket_message_callback(std::ptr::null_mut(), Some(on_message));
            emscripten_set_socket_open_callback(std::ptr::null_mut(), Some(on_open));
            emscripten_set_socket_connection_callback(std::ptr::null_mut(), Some(on_connection));
            emscripten_set_socket_close_callback(std::ptr::null_mut(), Some(on_close));
            emscripten_set_socket_error_callback(std::ptr::null_mut(), Some(on_error));
        }
    }

    /// Apply `ready` to `fd`'s `ScheduledIo` (if registered), wake its waiters,
    /// then re-drive. The fast path: each callback carries the fd it concerns,
    /// so it updates that fd directly without a `poll(2)`. The reactor borrow is
    /// dropped before `wake`, which may run wakers.
    fn dispatch(fd: RawFd, ready: Ready) {
        let io = Reactor::with(|r| r.fds.get(&fd).cloned());
        if let Some(io) = io {
            io.set_readiness(Tick::Set, |curr| curr | ready);
            // Latch the drive flag while running wakers so their unparks record
            // a pending pick-up; the synchronous `drive` below consumes it,
            // keeping the socket fast path a single drive (no host-turn hop).
            let guard = crate::emscripten::event_loop::enter_drive();
            io.wake(ready);
            drop(guard);
        }
        drive();
    }

    /// Re-derive readiness for every registered fd from one non-blocking
    /// `poll(2)`, applying it and waking waiters. Returns `true` only if some
    /// fd's readiness *transitioned* (a bit went unset→set) — a connected socket
    /// is perpetually `POLLOUT`, so "is anything ready" can't be the signal or
    /// the drive would never idle.
    ///
    /// The kernel calls this at its progression cliff (the `epoll`-park
    /// analogue); the sockfs callbacks handle the common per-fd case directly.
    ///
    /// This reactor is **level-triggered**: `poll(2)` reports current readiness,
    /// not edges. So consumers must clear readiness only on an actual `EAGAIN`,
    /// never proactively on a short read/write — `PollEvented`'s partial-read
    /// `clear_readiness` optimization is correctly gated to edge-triggered
    /// selectors (epoll/kqueue) and excludes emscripten; enabling it here would
    /// drop a wake while data remained and hang the task.
    fn poll_ready() -> bool {
        // Snapshot the registered set into the reused scratch, cloning the
        // `Arc`s so the reactor borrow is dropped before `wake` (which may run
        // wakers). `take` leaves the buffers empty for the duration of the poll.
        let (mut entries, mut pollfds) = Reactor::with(|r| {
            let mut entries = std::mem::take(&mut r.poll_entries);
            let mut pollfds = std::mem::take(&mut r.poll_fds);
            for (&fd, io) in &r.fds {
                entries.push((fd, io.clone()));
                pollfds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN | libc::POLLOUT,
                    revents: 0,
                });
            }
            (entries, pollfds)
        });

        let mut progressed = false;
        if !entries.is_empty() {
            // SAFETY: `pollfds` is a valid array of `len` entries. A zero
            // timeout is an instantaneous probe: emscripten's poll() routes
            // it through a plain non-suspending import, so it is callable
            // here, on a host-callback frame (under JSPI a suspending import
            // would trap on any stack not entered via a promising export).
            let n = unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, 0) };
            if n > 0 {
                for ((_, io), pfd) in entries.iter().zip(&pollfds) {
                    let ready = revents_to_ready(pfd.revents);
                    if ready.is_empty() {
                        continue;
                    }
                    let fresh = ready - io.set_readiness(Tick::Set, |curr| curr | ready);
                    if !fresh.is_empty() {
                        io.wake(fresh);
                        progressed = true;
                    }
                }
            }
        }

        // Release the `Arc` refs but keep the capacity for the next cliff.
        entries.clear();
        pollfds.clear();
        Reactor::with(|r| {
            r.poll_entries = entries;
            r.poll_fds = pollfds;
        });
        progressed
    }
}

fn _assert_kinds() {
    fn _assert<T: Send + Sync>() {}
    _assert::<Handle>();
}

impl Driver {
    pub(crate) fn new(_nevents: usize) -> io::Result<(Driver, Handle)> {
        let (registrations, synced) = RegistrationSet::new();
        let handle = Handle {
            registrations,
            synced: Mutex::new(synced),
            metrics: IoDriverMetrics::default(),
        };
        // Callbacks are installed lazily on the first registration — see
        // `install_callbacks`.
        Ok((Driver { _private: () }, handle))
    }

    pub(crate) fn park(&mut self, _rt_handle: &driver::Handle) {
        // Nothing to block on: readiness arrives via the JS loop.
    }

    pub(crate) fn park_timeout(&mut self, _rt_handle: &driver::Handle, _duration: Duration) {}

    pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.io();
        let ios = handle.registrations.shutdown(&mut handle.synced.lock());
        // Remove only this driver's fds: the thread-local map is shared with
        // any other runtime on the thread (notably the persistent event-loop
        // runtime), whose registrations must survive this one's teardown.
        let own: std::collections::HashSet<*const ScheduledIo> =
            ios.iter().map(Arc::as_ptr).collect();
        Reactor::with(|r| r.fds.retain(|_, io| !own.contains(&Arc::as_ptr(io))));
        for io in ios {
            io.shutdown();
        }
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Driver")
    }
}

impl Handle {
    /// External wakes reach the scheduler through the driver's `unpark`. With no
    /// reactor thread to wake, request a pick-up so the woken task is polled.
    /// Never drives inline (`Waker::wake` stays O(1) from host context).
    pub(crate) fn unpark(&self) {
        crate::emscripten::event_loop::request_pickup();
    }

    /// Register a socket fd. A second registration of a still-registered fd
    /// fails with [`AlreadyExists`](io::ErrorKind::AlreadyExists) (matching mio,
    /// and stopping a reused fd from inheriting stale readiness). Readiness is
    /// derived by `poll`, so `interest` doesn't gate it here.
    pub(super) fn add_source(&self, fd: RawFd, _interest: Interest) -> io::Result<Arc<ScheduledIo>> {
        Reactor::with(|reactor| {
            reactor.install_callbacks();
            if reactor.fds.contains_key(&fd) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "fd already registered with the emscripten I/O reactor",
                ));
            }
            let scheduled_io = self.registrations.allocate(&mut self.synced.lock())?;
            reactor.fds.insert(fd, scheduled_io.clone());
            self.metrics.incr_fd_count();
            Ok(scheduled_io)
        })
    }

    /// See [`Reactor::poll_ready`]; the kernel calls this at its progression
    /// cliff (the `epoll`-park analogue).
    pub(crate) fn poll_ready(&self) -> bool {
        Reactor::poll_ready()
    }

    /// Deregisters a socket fd from the reactor.
    pub(super) fn deregister_source(
        &self,
        fd: RawFd,
        registration: &Arc<ScheduledIo>,
    ) -> io::Result<()> {
        Reactor::with(|r| r.fds.remove(&fd));
        self.registrations
            .deregister(&mut self.synced.lock(), registration);
        self.metrics.dec_fd_count();
        Ok(())
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handle")
    }
}

fn revents_to_ready(revents: libc::c_short) -> Ready {
    let mut ready = Ready::EMPTY;
    if revents & libc::POLLIN != 0 {
        ready |= Ready::READABLE;
    }
    if revents & libc::POLLOUT != 0 {
        ready |= Ready::WRITABLE;
    }
    if revents & libc::POLLERR != 0 {
        ready |= Ready::ERROR;
    }
    if revents & libc::POLLHUP != 0 {
        ready |= Ready::READ_CLOSED | Ready::WRITE_CLOSED;
    }
    ready
}

extern "C-unwind" fn on_message(fd: c_int, _user_data: *mut c_void) {
    Reactor::dispatch(fd, Ready::READABLE);
}

extern "C-unwind" fn on_open(fd: c_int, _user_data: *mut c_void) {
    Reactor::dispatch(fd, Ready::WRITABLE);
}

extern "C-unwind" fn on_connection(_fd: c_int, _user_data: *mut c_void) {
    // sockfs reports `connection` on the accepted peer's fd, not the listener's,
    // so it can't be attributed; the cliff `poll_ready` reads the listener's
    // accept-readiness instead. Just re-drive.
    drive();
}

extern "C-unwind" fn on_close(fd: c_int, _user_data: *mut c_void) {
    Reactor::dispatch(fd, Ready::READ_CLOSED | Ready::WRITE_CLOSED);
}

extern "C-unwind" fn on_error(
    fd: c_int,
    _err: c_int,
    _msg: *const c_char,
    _user_data: *mut c_void,
) {
    Reactor::dispatch(fd, Ready::READABLE | Ready::WRITABLE | Ready::ERROR);
}
