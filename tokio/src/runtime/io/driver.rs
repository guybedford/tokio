// Signal handling
cfg_signal_internal_and_unix! {
    mod signal;
}
cfg_io_uring! {
    mod uring;
    use uring::UringContext;
    use crate::sync::OnceCell;
}

use crate::io::ready::Ready;

#[cfg(not(target_os = "emscripten"))]
use crate::io::interest::Interest;
#[cfg(not(target_os = "emscripten"))]
use crate::loom::sync::Mutex;
#[cfg(not(target_os = "emscripten"))]
use crate::runtime::driver;
#[cfg(not(target_os = "emscripten"))]
use crate::runtime::io::registration_set;
#[cfg(not(target_os = "emscripten"))]
use crate::runtime::io::{IoDriverMetrics, RegistrationSet, ScheduledIo};

#[cfg(not(target_os = "emscripten"))]
use mio::event::Source;
#[cfg(not(target_os = "emscripten"))]
use std::fmt;
#[cfg(not(target_os = "emscripten"))]
use std::io;
#[cfg(not(target_os = "emscripten"))]
use std::sync::Arc;
#[cfg(not(target_os = "emscripten"))]
use std::time::Duration;

/// I/O driver, backed by Mio.
#[cfg(not(target_os = "emscripten"))]
pub(crate) struct Driver {
    /// True when an event with the signal token is received
    signal_ready: bool,

    /// Reuse the `mio::Events` value across calls to poll.
    events: mio::Events,

    /// The system event queue.
    poll: mio::Poll,
}

/// A reference to an I/O driver.
#[cfg(not(target_os = "emscripten"))]
pub(crate) struct Handle {
    /// Registers I/O resources.
    registry: mio::Registry,

    /// Tracks all registrations
    registrations: RegistrationSet,

    /// State that should be synchronized
    synced: Mutex<registration_set::Synced>,

    /// Used to wake up the reactor from a call to `turn`.
    /// Not supported on `Wasi` due to lack of threading support.
    #[cfg(not(target_os = "wasi"))]
    waker: mio::Waker,

    pub(crate) metrics: IoDriverMetrics,

    #[cfg(all(
        tokio_unstable,
        feature = "io-uring",
        feature = "rt",
        feature = "fs",
        target_os = "linux",
    ))]
    pub(crate) uring_context: Mutex<UringContext>,

    #[cfg(all(
        tokio_unstable,
        feature = "io-uring",
        feature = "rt",
        feature = "fs",
        target_os = "linux",
    ))]
    pub(crate) uring_probe: OnceCell<Option<io_uring::Probe>>,
}

#[derive(Debug)]
pub(crate) struct ReadyEvent {
    pub(super) tick: u8,
    pub(crate) ready: Ready,
    pub(super) is_shutdown: bool,
}

#[cfg(all(unix, feature = "net"))]
impl ReadyEvent {
    pub(crate) fn with_ready(&self, ready: Ready) -> Self {
        Self {
            ready,
            tick: self.tick,
            is_shutdown: self.is_shutdown,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(super) enum Direction {
    Read,
    Write,
}

pub(super) enum Tick {
    Set,
    Clear(u8),
}

#[cfg(not(target_os = "emscripten"))]
const TOKEN_WAKEUP: mio::Token = mio::Token(0);
#[cfg(not(target_os = "emscripten"))]
const TOKEN_SIGNAL: mio::Token = mio::Token(1);

#[cfg(not(target_os = "emscripten"))]
fn _assert_kinds() {
    fn _assert<T: Send + Sync>() {}

    _assert::<Handle>();
}

// ===== impl Driver =====

#[cfg(not(target_os = "emscripten"))]
impl Driver {
    /// Creates a new event loop, returning any error that happened during the
    /// creation.
    pub(crate) fn new(nevents: usize) -> io::Result<(Driver, Handle)> {
        let poll = mio::Poll::new()?;
        #[cfg(not(target_os = "wasi"))]
        let waker = mio::Waker::new(poll.registry(), TOKEN_WAKEUP)?;
        let registry = poll.registry().try_clone()?;

        let driver = Driver {
            signal_ready: false,
            events: mio::Events::with_capacity(nevents),
            poll,
        };

        let (registrations, synced) = RegistrationSet::new();

        let handle = Handle {
            registry,
            registrations,
            synced: Mutex::new(synced),
            #[cfg(not(target_os = "wasi"))]
            waker,
            metrics: IoDriverMetrics::default(),
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring",
                feature = "rt",
                feature = "fs",
                target_os = "linux",
            ))]
            uring_context: Mutex::new(UringContext::new()),
            #[cfg(all(
                tokio_unstable,
                feature = "io-uring",
                feature = "rt",
                feature = "fs",
                target_os = "linux",
            ))]
            uring_probe: OnceCell::new(),
        };

        Ok((driver, handle))
    }

    pub(crate) fn park(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.io();
        self.turn(handle, None);
    }

    pub(crate) fn park_timeout(&mut self, rt_handle: &driver::Handle, duration: Duration) {
        let handle = rt_handle.io();
        self.turn(handle, Some(duration));
    }

    pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
        let handle = rt_handle.io();
        let ios = handle.registrations.shutdown(&mut handle.synced.lock());

        // `shutdown()` must be called without holding the lock.
        for io in ios {
            io.shutdown();
        }
    }

    fn turn(&mut self, handle: &Handle, max_wait: Option<Duration>) {
        debug_assert!(!handle.registrations.is_shutdown(&handle.synced.lock()));

        handle.release_pending_registrations();

        let events = &mut self.events;

        // Block waiting for an event to happen, peeling out how many events
        // happened.
        match self.poll.poll(events, max_wait) {
            Ok(()) => {}
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            #[cfg(target_os = "wasi")]
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
                // In case of wasm32_wasi this error happens, when trying to poll without subscriptions
                // just return from the park, as there would be nothing, which wakes us up.
            }
            Err(e) => panic!("unexpected error when polling the I/O driver: {e:?}"),
        }

        // Process all the events that came in, dispatching appropriately
        let mut ready_count = 0;
        for event in events.iter() {
            let token = event.token();

            if token == TOKEN_WAKEUP {
                // Nothing to do, the event is used to unblock the I/O driver
            } else if token == TOKEN_SIGNAL {
                self.signal_ready = true;
            } else {
                let ready = Ready::from_mio(event);
                let ptr = super::EXPOSE_IO.from_exposed_addr(token.0);

                // Safety: we ensure that the pointers used as tokens are not freed
                // until they are both deregistered from mio **and** we know the I/O
                // driver is not concurrently polling. The I/O driver holds ownership of
                // an `Arc<ScheduledIo>` so we can safely cast this to a ref.
                let io: &ScheduledIo = unsafe { &*ptr };

                io.set_readiness(Tick::Set, |curr| curr | ready);
                io.wake(ready);

                ready_count += 1;
            }
        }

        #[cfg(all(
            tokio_unstable,
            feature = "io-uring",
            feature = "rt",
            feature = "fs",
            target_os = "linux",
        ))]
        {
            let mut guard = handle.get_uring().lock();
            let ctx = &mut *guard;
            ctx.dispatch_completions();
        }

        handle.metrics.incr_ready_count_by(ready_count);
    }
}

#[cfg(not(target_os = "emscripten"))]
impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Driver")
    }
}

#[cfg(not(target_os = "emscripten"))]
impl Handle {
    /// Forces a reactor blocked in a call to `turn` to wakeup, or otherwise
    /// makes the next call to `turn` return immediately.
    ///
    /// This method is intended to be used in situations where a notification
    /// needs to otherwise be sent to the main reactor. If the reactor is
    /// currently blocked inside of `turn` then it will wake up and soon return
    /// after this method has been called. If the reactor is not currently
    /// blocked in `turn`, then the next call to `turn` will not block and
    /// return immediately.
    pub(crate) fn unpark(&self) {
        #[cfg(not(target_os = "wasi"))]
        self.waker.wake().expect("failed to wake I/O driver");
    }

    /// Registers an I/O resource with the reactor for a given `mio::Ready` state.
    ///
    /// The registration token is returned.
    pub(super) fn add_source(
        &self,
        source: &mut impl mio::event::Source,
        interest: Interest,
    ) -> io::Result<Arc<ScheduledIo>> {
        let scheduled_io = self.registrations.allocate(&mut self.synced.lock())?;
        let token = scheduled_io.token();

        // we should remove the `scheduled_io` from the `registrations` set if registering
        // the `source` with the OS fails. Otherwise it will leak the `scheduled_io`.
        if let Err(e) = self.registry.register(source, token, interest.to_mio()) {
            // safety: `scheduled_io` is part of the `registrations` set.
            unsafe {
                self.registrations
                    .remove(&mut self.synced.lock(), &scheduled_io)
            };

            return Err(e);
        }

        // TODO: move this logic to `RegistrationSet` and use a `CountedLinkedList`
        self.metrics.incr_fd_count();

        Ok(scheduled_io)
    }

    /// Deregisters an I/O resource from the reactor.
    pub(super) fn deregister_source(
        &self,
        registration: &Arc<ScheduledIo>,
        source: &mut impl Source,
    ) -> io::Result<()> {
        // Deregister the source with the OS poller **first**
        // Cleanup ALWAYS happens
        let os_result = self.registry.deregister(source);

        if self
            .registrations
            .deregister(&mut self.synced.lock(), registration)
        {
            self.unpark();
        }

        self.metrics.dec_fd_count();

        os_result // Return error after cleanup
    }

    fn release_pending_registrations(&self) {
        if self.registrations.needs_release() {
            self.registrations.release(&mut self.synced.lock());
        }
    }
}

#[cfg(not(target_os = "emscripten"))]
impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handle")
    }
}

impl Direction {
    pub(super) fn mask(self) -> Ready {
        match self {
            Direction::Read => Ready::READABLE | Ready::READ_CLOSED,
            Direction::Write => Ready::WRITABLE | Ready::WRITE_CLOSED,
        }
    }
}

// ===== emscripten =====
//
// emscripten can't block its single host thread in `epoll_wait`, so the driver
// is callback-driven instead of a park loop. It still uses mio's edge-triggered
// epoll selector and `mio::net::*` sockets — only the *wait* is replaced:
// [`Driver::new`] arms emscripten's `emscripten_epoll_set_callback` on the
// epoll fd (exposed by mio via `AsRawFd`). The host loop invokes [`on_ready`]
// on a fresh tick whenever the set has ready events; it fetches them with a
// zero-timeout `mio::Poll::poll` (`epoll_wait(.., 0)` — the moral equivalent of
// a blocking wait that just returned), dispatches into `ScheduledIo`, wakes
// waiters, and re-drives. `park`/`park_timeout` are therefore no-ops.
//
// External wakes reach the scheduler through [`Handle::unpark`], which requests
// a host pick-up rather than waking a mio `Waker` (there is no reactor thread).
#[cfg(target_os = "emscripten")]
mod emscripten {
    use super::Tick;
    use crate::io::interest::Interest;
    use crate::io::ready::Ready;
    use crate::loom::sync::Mutex;
    use crate::runtime::driver;
    use crate::runtime::io::{registration_set, IoDriverMetrics, RegistrationSet, ScheduledIo};

    use std::cell::RefCell;
    use std::fmt;
    use std::io;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::time::Duration;

    /// I/O driver. Owns the mio `Poll` whose epoll fd carries the armed
    /// readiness callback; kept alive for the runtime's lifetime so the callback
    /// stays armed. Dropping it disarms the callback and closes the epoll fd.
    pub(crate) struct Driver {
        /// Shared with [`Handle`]; the armed callback's `user_data` resolves to
        /// this stable heap allocation.
        state: Arc<PollState>,
    }

    /// The callback's target: the epoll set and a reused `epoll_wait` output
    /// buffer. Single-borrow discipline: [`on_ready`] is only entered from a
    /// fresh host tick, never re-entered while `drain` holds the borrow.
    struct PollState {
        inner: RefCell<PollInner>,
    }

    struct PollInner {
        poll: mio::Poll,
        events: mio::Events,
    }

    /// SAFETY: this runtime configuration is single-threaded (emscripten
    /// without OS threads); the driver never actually crosses threads. The
    /// impls only satisfy the generic runtime's auto-trait bounds.
    unsafe impl Send for PollState {}
    unsafe impl Sync for PollState {}

    /// A reference to the I/O reactor: a clone of the driver's `Registry` (same
    /// epoll fd) for registering/deregistering sockets, plus the shared
    /// [`PollState`] for draining.
    pub(crate) struct Handle {
        registry: mio::Registry,
        registrations: RegistrationSet,
        synced: Mutex<registration_set::Synced>,
        state: Arc<PollState>,
        pub(crate) metrics: IoDriverMetrics,
    }

    fn _assert_kinds() {
        fn _assert<T: Send + Sync>() {}
        _assert::<Handle>();
    }

    /// Drain `events` into their `ScheduledIo`s and wake waiters.
    fn dispatch(events: &mio::Events) {
        for event in events.iter() {
            let ready = Ready::from_mio(event);
            if ready.is_empty() {
                continue;
            }
            let ptr = crate::runtime::io::EXPOSE_IO.from_exposed_addr(event.token().0);
            // SAFETY: token pointers stay valid until the fd is deregistered
            // from mio; the driver owns an `Arc<ScheduledIo>` and this is a
            // single thread, so there is no concurrent free.
            let io: &ScheduledIo = unsafe { &*ptr };
            let fresh = ready - io.set_readiness(Tick::Set, |curr| curr | ready);
            if !fresh.is_empty() {
                io.wake(fresh);
            }
        }
    }

    /// The readiness callback armed by [`Driver::new`]: the host loop calls it
    /// on a fresh tick while the epoll set has uncollected ready events. It is
    /// a pure signal — collect the ready set with a zero-timeout `epoll_wait`,
    /// wake waiters, and re-drive (the wake source that turns the host loop on
    /// socket readiness, including resuming a `block_on`-parked stack).
    unsafe extern "C-unwind" fn on_ready(user_data: *mut std::ffi::c_void) {
        // SAFETY: `user_data` points at the driver's shared `PollState`, which
        // outlives the armed callback (`Driver::drop` disarms before the last
        // strong ref can drop).
        let state = unsafe { &*(user_data as *const PollState) };
        // Without `rt` there is no scheduler to wake: just apply readiness so
        // manual pollers observe it.
        #[cfg(not(feature = "rt"))]
        state.drain();
        // Dispatch readiness and resume any JSPI-parked stacks; the woken
        // `block_on` re-enters its fixed point from the resume.
        #[cfg(feature = "rt")]
        {
            state.drain();
            crate::runtime::jspi::unpark_all();
        }
    }

    impl PollState {
        /// Fetch and dispatch the epoll set's ready events with zero-timeout
        /// `epoll_wait`s until empty — the analogue of a blocking wait
        /// returning. Draining fully (rather than one buffer's worth) keeps an
        /// edge from being consumed without its waiter woken.
        fn drain(&self) {
            let mut inner = self.inner.borrow_mut();
            let inner = &mut *inner;
            loop {
                if inner
                    .poll
                    .poll(&mut inner.events, Some(Duration::ZERO))
                    .is_err()
                {
                    return;
                }
                if inner.events.is_empty() {
                    return;
                }
                dispatch(&inner.events);
            }
        }
    }

    impl Driver {
        pub(crate) fn new(nevents: usize) -> io::Result<(Driver, Handle)> {
            let poll = mio::Poll::new()?;
            let registry = poll.registry().try_clone()?;
            let state = Arc::new(PollState {
                inner: RefCell::new(PollInner {
                    poll,
                    events: mio::Events::with_capacity(nevents.max(1)),
                }),
            });
            let epfd = state.inner.borrow().poll.as_raw_fd();
            let rc = unsafe {
                crate::emscripten::ffi::emscripten_epoll_set_callback(
                    epfd,
                    Some(on_ready),
                    &*state as *const PollState as *mut std::ffi::c_void,
                )
            };
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
            let (registrations, synced) = RegistrationSet::new();
            let handle = Handle {
                registry,
                registrations,
                synced: Mutex::new(synced),
                state: state.clone(),
                metrics: IoDriverMetrics::default(),
            };
            Ok((Driver { state }, handle))
        }

        pub(crate) fn park(&mut self, _rt_handle: &driver::Handle) {
            // Nothing to block on: readiness arrives via the host loop callback.
        }

        pub(crate) fn park_timeout(&mut self, _rt_handle: &driver::Handle, _duration: Duration) {}

        pub(crate) fn shutdown(&mut self, rt_handle: &driver::Handle) {
            let handle = rt_handle.io();
            let ios = handle.registrations.shutdown(&mut handle.synced.lock());
            for io in ios {
                io.shutdown();
            }
        }
    }

    impl Drop for Driver {
        fn drop(&mut self) {
            // Disarm before the `PollState` box (the callback's `user_data`)
            // is freed. Closing the epoll fd would also disarm, but only after
            // `PollInner` drops — a window where a queued delivery could fire
            // against freed state.
            let epfd = self.state.inner.borrow().poll.as_raw_fd();
            unsafe {
                crate::emscripten::ffi::emscripten_epoll_set_callback(
                    epfd,
                    None,
                    std::ptr::null_mut(),
                );
            }
        }
    }

    impl fmt::Debug for Driver {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Driver")
        }
    }

    impl Handle {
        /// External wakes reach the scheduler through the driver's `unpark`.
        /// With no reactor thread to wake, resume the JSPI-parked stacks; each
        /// re-checks its own fixed point. Without `rt` nothing can be parked,
        /// so there is nothing to wake.
        pub(crate) fn unpark(&self) {
            #[cfg(feature = "rt")]
            crate::runtime::jspi::unpark_all();
        }

        /// One full drain of the reactor: fetch and dispatch all ready events.
        pub(crate) fn drain(&self) {
            self.state.drain();
        }

        pub(crate) fn add_source(
            &self,
            source: &mut impl mio::event::Source,
            interest: Interest,
        ) -> io::Result<Arc<ScheduledIo>> {
            let scheduled_io = self.registrations.allocate(&mut self.synced.lock())?;
            let token = scheduled_io.token();
            if let Err(e) = self.registry.register(source, token, interest.to_mio()) {
                // SAFETY: `scheduled_io` is part of the `registrations` set.
                unsafe {
                    self.registrations
                        .remove(&mut self.synced.lock(), &scheduled_io)
                };
                return Err(e);
            }
            self.metrics.incr_fd_count();
            Ok(scheduled_io)
        }

        pub(crate) fn deregister_source(
            &self,
            registration: &Arc<ScheduledIo>,
            source: &mut impl mio::event::Source,
        ) -> io::Result<()> {
            let os_result = self.registry.deregister(source);
            self.registrations
                .deregister(&mut self.synced.lock(), registration);
            self.metrics.dec_fd_count();
            os_result
        }
    }

    impl fmt::Debug for Handle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Handle")
        }
    }
}

#[cfg(target_os = "emscripten")]
pub(crate) use emscripten::{Driver, Handle};
