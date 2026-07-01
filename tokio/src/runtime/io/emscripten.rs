//! Emscripten I/O reactor: a drop-in replacement for the mio-backed
//! [`super::driver`] where `mio` is unavailable. It is built on emscripten's
//! epoll (`epoll_create1`/`epoll_ctl`/`epoll_wait`): one long-lived,
//! **edge-triggered** (`EPOLLET`) epoll fd per thread aggregates every
//! registered socket — the same model as mio's epoll selector, so emscripten is
//! a genuine edge-triggered platform here, not a level-triggered `poll(2)` one.
//!
//! Two readiness sources cooperate, both draining that one epoll set:
//!
//! - **Fast path / wake source** — [`emscripten_epoll_set_callback`] registers a
//!   persistent, non-blocking consumer on the epoll. emscripten can't block in
//!   `epoll_wait` on its single host thread, so instead the runtime delivers the
//!   ready set to [`on_epoll`] on a fresh host tick whenever the set makes
//!   progress. It applies readiness, wakes waiters, and re-drives — the analogue
//!   of mio's reactor thread returning from a blocking `epoll_wait`.
//! - **Cliff probe** — [`Handle::poll_ready`] is one non-blocking
//!   `epoll_wait(.., 0)`, run by the kernel at its progression cliff (the
//!   `epoll`-park analogue). Within a synchronous `block_on` drive the callback
//!   tick hasn't fired yet, so this surfaces already-pending readiness without a
//!   host round-trip.
//!
//! Both funnel into the same [`ScheduledIo`]; edge-triggered dedup makes the
//! overlap harmless — whichever drains an edge first delivers it, the other sees
//! an empty ready list. A fd is registered once (duplicate registration
//! rejected, like mio). Readiness is delivered by the JS loop, so
//! `park`/`park_timeout` are no-ops.
//!
//! Edge-triggered here means *wakeups* are edges — it does **not** license
//! `PollEvented`'s short-read/short-write `clear_readiness` optimization, which
//! is gated out for emscripten in `io/poll_evented.rs`. That optimization
//! assumes a short read drained the kernel socket buffer (true for a byte-stream
//! socket), but emscripten's `sockfs` `recv` is frame-granular: a `writev` of N
//! slices echoes back as N separate ws frames, so a short `recv` returns one
//! frame with more still buffered behind no fresh edge. So readiness is cleared
//! only on an actual `EAGAIN` (the unconditional `WouldBlock` path), never
//! proactively — otherwise a multi-frame read hangs.

use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::loom::sync::Mutex;
use crate::runtime::driver;
use super::{registration_set, IoDriverMetrics, RegistrationSet, ScheduledIo, Tick};

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::fmt;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use crate::emscripten::ffi::{
    emscripten_epoll_set_callback, epoll_create1, epoll_ctl, epoll_wait,
};
use crate::emscripten::event_loop::drive;

/// Edge-triggered read+write+peer-close interest registered for every fd, so the
/// epoll set behaves like mio's (one `EPOLLET` registration, readiness re-derived
/// by tokio on each edge).
const INTEREST: u32 =
    (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET) as u32;

/// Output-buffer cap for the callback delivery and the cliff `epoll_wait`. More
/// than this ready at once simply drains across ticks / cliff revisits.
const MAX_EVENTS: usize = 256;

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

/// This thread's I/O reactor state: the long-lived epoll fd that aggregates
/// every registered socket, the fd→`ScheduledIo` map the readiness paths consult
/// (keyed the way epoll carries `data.u64`), and a reused `epoll_wait` output
/// buffer. Single-thread, so the thread-local `RefCell` below is sound and keeps
/// `Handle: Send + Sync`.
struct Reactor {
    /// The thread's epoll fd, or `-1` until lazily created on first registration.
    epfd: RawFd,
    /// Registered socket fds and their readiness slots.
    fds: HashMap<RawFd, Arc<ScheduledIo>>,
    /// Reused cliff `epoll_wait` output buffer; taken out for the duration of a
    /// probe, then returned empty.
    events: Vec<libc::epoll_event>,
}

thread_local! {
    static REACTOR: RefCell<Reactor> = RefCell::new(Reactor::new());
}

impl Reactor {
    fn new() -> Reactor {
        Reactor {
            epfd: -1,
            fds: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// Run `f` with this thread's reactor.
    fn with<R>(f: impl FnOnce(&mut Reactor) -> R) -> R {
        REACTOR.with(|r| f(&mut r.borrow_mut()))
    }

    /// Lazily create the epoll fd and arm its readiness callback, once. Done on
    /// the first fd registration rather than at driver build so a net-enabled
    /// program that only ever resolves names never creates one. Returns the
    /// epoll fd.
    fn ensure_epoll(&mut self) -> io::Result<RawFd> {
        if self.epfd >= 0 {
            return Ok(self.epfd);
        }
        // SAFETY: standard `epoll_create1`. No `EPOLL_CLOEXEC` — emscripten has
        // no `exec`, so close-on-exec is moot.
        let epfd = unsafe { epoll_create1(0) };
        if epfd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `epfd` is a fresh epoll fd; `on_epoll` is valid for the
        // process lifetime. The callback is non-blocking (no JSPI needed).
        let rc = unsafe {
            emscripten_epoll_set_callback(
                epfd,
                MAX_EVENTS as c_int,
                Some(on_epoll),
                std::ptr::null_mut(),
            )
        };
        if rc != 0 {
            unsafe { libc::close(epfd) };
            return Err(io::Error::from_raw_os_error(-rc));
        }
        self.epfd = epfd;
        Ok(epfd)
    }

    /// Drain `events[..nready]` into their `ScheduledIo`s and wake waiters. The
    /// shared core of both the callback (`on_epoll`) and the cliff `epoll_wait`.
    /// Returns `true` if any fd's readiness *transitioned* (a bit went
    /// unset→set). The reactor borrow is dropped before `wake`, which may run
    /// wakers.
    fn dispatch_events(events: &[libc::epoll_event]) -> bool {
        let mut progressed = false;
        for ev in events {
            let fd = ev.u64 as RawFd;
            let ready = epoll_to_ready(ev.events);
            if ready.is_empty() {
                continue;
            }
            let io = Reactor::with(|r| r.fds.get(&fd).cloned());
            if let Some(io) = io {
                let fresh = ready - io.set_readiness(Tick::Set, |curr| curr | ready);
                if !fresh.is_empty() {
                    io.wake(fresh);
                    progressed = true;
                }
            }
        }
        progressed
    }

    /// One non-blocking `epoll_wait(.., 0)` over the set, dispatched and woken.
    /// Returns `true` if any readiness transitioned.
    ///
    /// The kernel calls this at its progression cliff (the `epoll`-park
    /// analogue); the callback (`on_epoll`) handles the common host-driven case.
    /// Edge-triggered, so a drained edge is consumed once — tokio re-derives
    /// readiness and clears on `EAGAIN`, exactly as on mio's epoll.
    fn poll_ready() -> bool {
        let epfd = Reactor::with(|r| r.epfd);
        if epfd < 0 {
            return false;
        }
        // Take the reused buffer out (sized to the cap) for the probe.
        let mut events = Reactor::with(|r| {
            let mut events = std::mem::take(&mut r.events);
            events.clear();
            events.reserve(MAX_EVENTS);
            events
        });

        // SAFETY: `events` has capacity for `MAX_EVENTS`. A zero timeout is an
        // instantaneous probe: emscripten routes it through a plain
        // non-suspending syscall, so it is callable here, on a host-callback
        // frame (a blocking wait would need JSPI/ASYNCIFY).
        let n = unsafe { epoll_wait(epfd, events.as_mut_ptr(), MAX_EVENTS as c_int, 0) };
        let mut progressed = false;
        if n > 0 {
            // SAFETY: `epoll_wait` wrote `n` valid entries.
            unsafe { events.set_len(n as usize) };
            progressed = Reactor::dispatch_events(&events);
        }

        // Release any borrowed contents but keep the capacity for the next cliff.
        events.clear();
        Reactor::with(|r| r.events = events);
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
        Reactor::with(|r| {
            let epfd = r.epfd;
            r.fds.retain(|&fd, io| {
                if own.contains(&Arc::as_ptr(io)) {
                    if epfd >= 0 {
                        // SAFETY: `DEL` ignores the event pointer; a closed fd
                        // just yields `EBADF`.
                        unsafe { epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
                    }
                    false
                } else {
                    true
                }
            });
        });
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

    /// Register a socket fd on the epoll set (edge-triggered, read+write). A
    /// second registration of a still-registered fd fails with
    /// [`AlreadyExists`](io::ErrorKind::AlreadyExists) (matching mio, and
    /// stopping a reused fd from inheriting stale readiness). epoll watches
    /// read+write unconditionally and tokio re-derives, so `interest` doesn't
    /// gate it here.
    pub(super) fn add_source(&self, fd: RawFd, _interest: Interest) -> io::Result<Arc<ScheduledIo>> {
        Reactor::with(|reactor| {
            let epfd = reactor.ensure_epoll()?;
            if reactor.fds.contains_key(&fd) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "fd already registered with the emscripten I/O reactor",
                ));
            }
            let scheduled_io = self.registrations.allocate(&mut self.synced.lock())?;
            let mut ev = libc::epoll_event {
                events: INTEREST,
                u64: fd as u64,
            };
            // SAFETY: `epfd` is the reactor's epoll fd; `ev` outlives the call.
            let rc = unsafe { epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
            if rc != 0 {
                let err = io::Error::last_os_error();
                self.registrations
                    .deregister(&mut self.synced.lock(), &scheduled_io);
                return Err(err);
            }
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

    /// Deregisters a socket fd from the reactor and the epoll set.
    pub(super) fn deregister_source(
        &self,
        fd: RawFd,
        registration: &Arc<ScheduledIo>,
    ) -> io::Result<()> {
        Reactor::with(|r| {
            if r.fds.remove(&fd).is_some() && r.epfd >= 0 {
                // SAFETY: `DEL` ignores the event pointer. An already-closed fd
                // returns `EBADF` (emscripten epoll auto-evicts it); ignore it.
                unsafe { epoll_ctl(r.epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
            }
        });
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

/// Map an `epoll_event` mask to tokio [`Ready`], mirroring mio's epoll selector
/// (`Ready::from_mio`) so emscripten readiness matches every other epoll target.
fn epoll_to_ready(events: u32) -> Ready {
    let events = events as c_int;
    let mut ready = Ready::EMPTY;
    if events & (libc::EPOLLIN | libc::EPOLLPRI) != 0 {
        ready |= Ready::READABLE;
    }
    if events & libc::EPOLLOUT != 0 {
        ready |= Ready::WRITABLE;
    }
    if events & libc::EPOLLERR != 0 {
        ready |= Ready::ERROR;
    }
    // `is_read_closed`: HUP, or RDHUP alongside readable data.
    if events & libc::EPOLLHUP != 0
        || (events & libc::EPOLLIN != 0 && events & libc::EPOLLRDHUP != 0)
    {
        ready |= Ready::READ_CLOSED;
    }
    // `is_write_closed`: HUP, a writable-side error, or a bare error.
    if events & libc::EPOLLHUP != 0
        || (events & libc::EPOLLOUT != 0 && events & libc::EPOLLERR != 0)
        || events == libc::EPOLLERR
    {
        ready |= Ready::WRITE_CLOSED;
    }
    ready
}

/// The epoll readiness callback armed by [`Reactor::ensure_epoll`]: the runtime
/// delivers the ready set here on a fresh host tick. Apply it, wake waiters, and
/// re-drive — the wake source that gets the host loop turning on socket
/// readiness (including resuming a `block_on`-parked stack).
extern "C-unwind" fn on_epoll(
    _epfd: c_int,
    events: *mut libc::epoll_event,
    nready: c_int,
    _userdata: *mut c_void,
) {
    if nready <= 0 {
        return;
    }
    // SAFETY: the runtime guarantees `events[..nready]` are valid for the call.
    let events = unsafe { std::slice::from_raw_parts(events, nready as usize) };
    // Latch the drive flag while running wakers so their unparks record a
    // pending pick-up; the synchronous `drive` below consumes it, keeping this
    // delivery a single drive (no extra host-turn hop).
    let guard = crate::emscripten::event_loop::enter_drive();
    Reactor::dispatch_events(events);
    drop(guard);
    drive();
}
