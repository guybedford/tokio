//! Host glue for emscripten hosted event-loop runtimes: `setTimeout` arming,
//! keepalive, and the host wake plumbing behind [`HostedRuntime::schedule`] and
//! [`HostedRuntime::drive`]. Each hosted runtime owns a heap-pinned
//! [`HostedState`] — the identity every host callback (`user_data`) resolves
//! to — so any number of runtimes coexist on the thread, each driven by its
//! own host turns. The scheduler-coupled kernel (the `Core` pumping and
//! `drive_loop`) lives in the `HostedEventLoop` scheduler at
//! `runtime/scheduler/current_thread/hosted_event_loop.rs`.
//!
//! Three things are genuinely per-*thread*, not per-runtime, and live in the
//! [`HostContext`] member of tokio's `runtime::context` thread-local:
//! * the drive latch: tokio forbids nested `enter_runtime` on a thread, so at
//!   most one runtime may be mid-drive at a time; a wake for any runtime while
//!   the latch is held is recorded and delivered when the holder exits.
//! * the pending list: which runtimes recorded such a wake.
//! * the JSPI-parked stacks: suspended stacks belong to the thread (a
//!   `ParkThread::park` isn't even runtime-associated). A pick-up resolves
//!   them all; each re-checks its own fixed point and re-parks if its wake
//!   hasn't arrived.
//!
//! [`HostedRuntime::schedule`]: crate::runtime::HostedRuntime::schedule
//! [`HostedRuntime::drive`]: crate::runtime::HostedRuntime::drive

use crate::emscripten::ffi::{
    emscripten_clear_timeout, emscripten_get_now, emscripten_promise_await,
    emscripten_promise_create, emscripten_promise_destroy, emscripten_promise_resolve,
    emscripten_runtime_keepalive_pop, emscripten_runtime_keepalive_push, emscripten_set_timeout,
    EmPromise, EM_PROMISE_FULFILL,
};
use crate::runtime::scheduler::{drive_exec, Driven, HostedExec};

use std::sync::{Arc, Weak};

use std::cell::{Cell, OnceCell, RefCell};
use std::ffi::c_void;

#[derive(Clone, Copy)]
struct Armed {
    id: i32,
    fires_at_ms: f64,
}

/// Polls per event-loop drive before yielding to the host: large enough to
/// amortize the `setTimeout(0)` round-trip, small enough not to stall a frame.
/// Shared with `block_on`'s JSPI drive so both bound their polls identically.
pub(crate) const HOST_DRIVE_BUDGET: u32 = 4096;

/// Whether this binary can park. JSPI is a link-time choice (`-sJSPI`); when
/// absent a would-suspend `block_on` panics rather than park, the only sound
/// semantics when the host loop can't run while wasm is on the stack. Never
/// consulted by `schedule`/`drive`, which cannot suspend.
pub(crate) fn jspi_linked() -> bool {
    unsafe { crate::emscripten::ffi::emscripten_has_asyncify() == 2 }
}

/// This thread's host-loop state: the drive latch, the runtimes whose
/// pick-ups it latched, and the JSPI-parked stacks — held in tokio's
/// per-thread [`runtime::context`](crate::runtime::context) rather than a
/// parallel thread-local.
pub(crate) struct HostContext {
    /// True while a drive or `block_on` fixed point is on the stack (including
    /// suspended mid-task, e.g. inside a blocking `fd_wait`). A wake arriving
    /// under it is recorded in `pending` rather than driving: driving would
    /// nest `enter_runtime`, and wakes must stay O(1) regardless.
    in_drive: Cell<bool>,
    /// Runtimes that latched a pick-up while `in_drive` was held; drained (and
    /// delivered) when the holder releases the latch.
    pending: RefCell<Vec<Weak<HostedState>>>,
    /// The JSPI-parked stacks on this thread, as the promise each is suspended
    /// on plus its optional deadline timer.
    parked: RefCell<Vec<Parked>>,
}

/// One JSPI-parked stack: the promise its `emscripten_promise_await` is
/// suspended on, and its deadline timer (`None` once fired or when deadline-less).
struct Parked {
    promise: EmPromise,
    timer: Option<i32>,
}

impl HostContext {
    pub(crate) const fn new() -> HostContext {
        HostContext {
            in_drive: Cell::new(false),
            pending: RefCell::new(Vec::new()),
            parked: RefCell::new(Vec::new()),
        }
    }
}

fn with_host<R>(f: impl FnOnce(&HostContext) -> R) -> R {
    crate::runtime::context::with_hosted(f)
}

/// One hosted runtime's host-loop identity. Heap-pinned behind `Arc` so the
/// raw `user_data` pointers carried by its host callbacks (the drive
/// `setTimeout`, the epoll readiness callback) stay valid and stable; every
/// armed callback is disarmed before the last strong ref drops (the timer in
/// `Drop` below, the epoll callback in the io driver's `Drop`).
pub(crate) struct HostedState {
    /// The drive capability, set by the builder once the scheduler exists.
    /// Weak: an armed host callback must not keep a dead runtime alive.
    exec: OnceCell<Weak<HostedExec>>,
    /// Handle back to self for latch registration from `&self` callbacks.
    self_weak: Weak<HostedState>,
    /// The single outstanding `setTimeout` for this runtime's soonest timer
    /// deadline, or `None`. At most one host timer per runtime is outstanding.
    armed: Cell<Option<Armed>>,
    /// A pick-up was latched for this runtime while the thread's drive latch
    /// was held; consumed by the next drive or latch-exit delivery.
    pending: Cell<bool>,
    /// True while this runtime's in-flight roots hold an emscripten keepalive
    /// ref; released when a drive finds it idle.
    keepalive: Cell<bool>,
}

/// SAFETY: this runtime configuration is single-threaded (emscripten without
/// OS threads for the runtime); `HostedState` never actually crosses threads.
/// The impls only satisfy auto-trait bounds of the structures that hold it.
unsafe impl Send for HostedState {}
unsafe impl Sync for HostedState {}

impl std::fmt::Debug for HostedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostedState").finish()
    }
}

impl HostedState {
    #[cfg(tokio_unstable)] // only the HostedRuntime builder constructs states
    pub(crate) fn new() -> Arc<HostedState> {
        Arc::new_cyclic(|weak| HostedState {
            exec: OnceCell::new(),
            self_weak: weak.clone(),
            armed: Cell::new(None),
            pending: Cell::new(false),
            keepalive: Cell::new(false),
        })
    }

    /// Wire the drive capability once the scheduler exists (builder only).
    #[cfg(tokio_unstable)]
    pub(crate) fn set_exec(&self, exec: Weak<HostedExec>) {
        self.exec.set(exec).ok().expect("exec set once");
    }

    /// Request that this runtime be driven: how a wake arriving from host
    /// context (a task scheduled or a waker fired outside any drive) reaches
    /// the scheduler. Never runs tasks on the caller's stack — `Waker::wake`
    /// stays O(1) and can't reenter user code. While the thread's drive latch
    /// is held the request is recorded for the holder's exit; otherwise it is
    /// delivered immediately (parked drives resumed, a `setTimeout(0)`
    /// pick-up armed).
    pub(crate) fn request_pickup(&self) {
        if !self.latch_if_driving() {
            // Deliver immediately: resume any JSPI-parked stacks (their wake
            // may be the one being delivered) and arm a `setTimeout(0)` host
            // turn for this runtime. Both are O(1) deferrals — no task runs
            // on the caller's stack.
            unpark_all();
            self.arm_timeout(0.0);
        }
    }

    /// If the thread's drive latch is held, record a pending pick-up for the
    /// holder's exit (once — the `pending` flag dedups) and return `true`.
    fn latch_if_driving(&self) -> bool {
        with_host(|h| {
            let latched = h.in_drive.get();
            if latched && !self.pending.replace(true) {
                h.pending.borrow_mut().push(self.self_weak.clone());
            }
            latched
        })
    }

    /// Drive this runtime to a quiescent fixed point if the thread is free;
    /// otherwise latch a pick-up for the current holder's exit (its own fixed
    /// point observes queued work; a foreign holder converts it to a host
    /// turn). Returns whether a drive ran.
    pub(crate) fn drive(&self) -> bool {
        if self.latch_if_driving() {
            return false;
        }
        self.drive_inner();
        true
    }

    /// One pick-up: drain I/O readiness, drive the runtime cooperatively, then
    /// arm the next host wake (a `setTimeout` for the soonest timer) or, if
    /// idle, release the keepalive and let the host rest.
    fn drive_inner(&self) {
        let Some(exec) = self.exec.get().and_then(Weak::upgrade) else {
            // The runtime is gone; nothing to drive (a disarm is in flight).
            return;
        };

        let guard = enter_drive();

        // Harvest the reactor first, inside the latch: its wakes settle into
        // queued work (and `pending`, consumed just below) so the fixed point
        // observes everything in one pass.
        #[cfg(feature = "net")]
        if let Some(io) = exec.rt_handle.inner.driver().io.as_ref() {
            io.drain();
        }
        // A pick-up consumed by this drive: parked stacks are not queued work,
        // so their wake must still be delivered by resuming them.
        if self.pending.take() {
            unpark_all();
        }

        let driven = drive_exec(&exec, HOST_DRIVE_BUDGET);
        // Releasing the latch delivers any pick-ups latched during the drive
        // (including this runtime's, if a wake arrived mid-fixed-point through
        // a path it could not observe).
        drop(guard);

        match driven {
            Driven::Timer(ms) => self.arm_timeout(ms),
            Driven::Yield => self.arm_timeout(0.0),
            // The core is checked out elsewhere; that holder re-arms on exit.
            Driven::Busy => {}
            Driven::Idle => {
                self.disarm_timeout();
                self.keepalive_pop();
            }
        }
    }

    /// Hold an emscripten keepalive ref for this runtime's in-flight roots
    /// (idempotent; released by an idle drive).
    #[cfg(tokio_unstable)] // only `HostedRuntime::schedule` pushes through here
    pub(crate) fn keepalive_push(&self) {
        if !self.keepalive.replace(true) {
            unsafe { emscripten_runtime_keepalive_push() };
        }
    }

    /// Release the keepalive ref, if held (idle drive, or runtime drop with
    /// roots still in flight).
    fn keepalive_pop(&self) {
        if self.keepalive.replace(false) {
            // SAFETY: paired with the push in `keepalive_push`.
            unsafe { emscripten_runtime_keepalive_pop() };
        }
    }

    /// Arm a host `setTimeout` for `delay_ms` from now, coalescing to the
    /// soonest deadline: a nearer outstanding timer is kept; a farther one is
    /// replaced.
    fn arm_timeout(&self, delay_ms: f64) {
        let delay = delay_ms.max(0.0);
        let fires_at = unsafe { emscripten_get_now() } + delay;
        if let Some(prev) = self.armed.get() {
            if prev.fires_at_ms <= fires_at {
                return;
            }
            clear_timeout(prev.id);
        }
        // SAFETY: the armed callback's `user_data` is this `HostedState`,
        // which outlives it: `Drop` clears any armed timer before the
        // allocation is freed, and `clearTimeout` cancels even a
        // queued-but-unfired callback.
        let id = unsafe {
            emscripten_set_timeout(
                Some(timeout_entry),
                delay,
                self as *const HostedState as *mut c_void,
            )
        };
        self.armed.set(Some(Armed {
            id,
            fires_at_ms: fires_at,
        }));
    }

    /// Cancel the outstanding (unfired) host timer, if any.
    fn disarm_timeout(&self) {
        if let Some(a) = self.armed.take() {
            clear_timeout(a.id);
        }
    }
}

impl Drop for HostedState {
    fn drop(&mut self) {
        // Disarm before the allocation the armed callback points at is freed.
        self.disarm_timeout();
        // Roots that never completed (runtime dropped mid-flight) must not
        // pin the emscripten instance alive forever.
        self.keepalive_pop();
    }
}

/// A hosted runtime's drive `setTimeout` fired: forget the spent timer id and
/// drive it. The id must NOT be cleared — the timeout is already spent, and
/// emscripten released its keepalive ref on fire. Goes through the
/// latch-aware [`HostedState::drive`]: the timer can fire while another
/// runtime's drive holds the thread (a task suspended mid-poll in a blocking
/// `fd_wait`), and driving inline there would nest `enter_runtime`.
unsafe extern "C-unwind" fn timeout_entry(user_data: *mut c_void) {
    // SAFETY: `user_data` is the runtime's `HostedState`, kept alive (and its
    // timer disarmed on drop) by the owning runtime; see `arm_timeout`.
    let state = unsafe { &*(user_data as *const HostedState) };
    state.armed.set(None);
    state.drive();
}

// ===== thread-level latch, pick-up, and park plumbing =====

/// Marks a drive on the stack for its lifetime, restoring the prior latch
/// state on drop (even across an unwind) and, on the outermost release,
/// delivering every pick-up latched while it was held — so a wake arriving
/// under a drive or a foreign `block_on` is never dropped.
pub(crate) struct DriveGuard {
    prev: bool,
}

impl Drop for DriveGuard {
    fn drop(&mut self) {
        with_host(|h| h.in_drive.set(self.prev));
        if !self.prev {
            drain_pending();
        }
    }
}

/// Enter a drive: set the thread's drive latch until the returned guard drops.
pub(crate) fn enter_drive() -> DriveGuard {
    DriveGuard {
        prev: with_host(|h| h.in_drive.replace(true)),
    }
}

/// Deliver every latched pick-up. Skips runtimes whose flag was already
/// consumed (their drive observed the work) and runtimes that have died.
/// `unpark_all` is thread-global (it resolves every parked stack), so it runs
/// at most once per drain rather than once per runtime.
fn drain_pending() {
    let mut unparked = false;
    loop {
        // Take the list wholesale: a delivery may latch anew (spurious but
        // legal), and the borrow must not be held across it.
        let pending = with_host(|h| std::mem::take(&mut *h.pending.borrow_mut()));
        if pending.is_empty() {
            return;
        }
        for weak in pending {
            if let Some(state) = weak.upgrade() {
                if state.pending.take() {
                    if !unparked {
                        unparked = true;
                        unpark_all();
                    }
                    state.arm_timeout(0.0);
                }
            }
        }
    }
}

/// Route an external wake through the driver's `unpark`: request the hosted
/// runtime's pick-up when this driver belongs to one, else just resume the
/// JSPI-parked stacks (each re-checks its own condition and re-parks if this
/// wake wasn't its). Never drives inline, so `Waker::wake` stays O(1).
pub(crate) fn route_unpark(hosted: Option<&Arc<HostedState>>) {
    match hosted {
        Some(hosted) => hosted.request_pickup(),
        None => unpark_all(),
    }
}

/// Resolve every parked stack's promise. Each resumes as a microtask, re-checks
/// its fixed point, and re-parks if its wake hasn't arrived. Re-resolving a
/// settled promise is a no-op.
pub(crate) fn unpark_all() {
    with_host(|h| {
        for parked in h.parked.borrow().iter() {
            // SAFETY: the handle lives until its stack resumes and destroys it,
            // which can't happen while this callback runs.
            unsafe {
                emscripten_promise_resolve(parked.promise, EM_PROMISE_FULFILL, std::ptr::null_mut())
            };
        }
    });
}

/// A parked stack's deadline timer fired: settle its promise. Forget the spent
/// timer id so the resume path won't clear it or double-release its keepalive ref.
unsafe extern "C-unwind" fn park_timeout_entry(promise: *mut c_void) {
    with_host(|h| {
        if let Some(parked) = h
            .parked
            .borrow_mut()
            .iter_mut()
            .find(|p| p.promise == promise)
        {
            parked.timer = None;
        }
    });
    unsafe { emscripten_promise_resolve(promise, EM_PROMISE_FULFILL, std::ptr::null_mut()) };
}

/// Suspend the current drive on the host event loop via JSPI until a wake
/// ([`HostedState::request_pickup`] / [`unpark_all`]) or `timeout_ms` elapses
/// (negative = no timer). The kernel's park primitive: called at a `block_on`
/// cliff (after checking its core back in) and by `ParkThread::park` for the
/// blocking APIs.
///
/// Across the suspension the per-stack context is swapped out — the
/// runtime-entered flag and the drive latch — so reentrant host callbacks can
/// drive runtimes (including the suspended one's, whose core is back in its
/// slot) as if this stack weren't there. Restored on resume, which runs as a
/// microtask: never mid-fixed-point.
pub(crate) fn park_on_host(timeout_ms: f64) {
    // Pick-ups latched by the drive that is now parking would otherwise wait
    // out the suspension: deliver them first (we are not yet registered as
    // parked, so this cannot self-resolve).
    drain_pending();

    struct ParkGuard {
        prev_enter: crate::runtime::context::EnterRuntime,
        prev_latch: bool,
    }
    impl Drop for ParkGuard {
        fn drop(&mut self) {
            with_host(|h| h.in_drive.set(self.prev_latch));
            crate::runtime::context::jspi_restore_runtime_after_park(self.prev_enter);
            // SAFETY: paired with the push below.
            unsafe { emscripten_runtime_keepalive_pop() };
        }
    }

    let promise = unsafe { emscripten_promise_create() };
    let timer = if timeout_ms >= 0.0 {
        // SAFETY: the resume path clears the timer (or it has fired) before the
        // promise is destroyed, so the callback's `user_data` never dangles.
        Some(unsafe { emscripten_set_timeout(Some(park_timeout_entry), timeout_ms, promise) })
    } else {
        None
    };
    with_host(|h| h.parked.borrow_mut().push(Parked { promise, timer }));

    {
        // A suspended stack is pending work emscripten can't see: under
        // `EXIT_RUNTIME` with no keepalive, a managed callback firing
        // mid-suspension would `maybeExit` and tear the runtime down under us.
        // Hold a keepalive ref for the suspension's lifetime.
        unsafe { emscripten_runtime_keepalive_push() };
        let _guard = ParkGuard {
            prev_enter: crate::runtime::context::jspi_exit_runtime_for_park(),
            prev_latch: with_host(|h| h.in_drive.replace(false)),
        };
        // SAFETY: suspends this stack until the promise settles.
        let _ = unsafe { emscripten_promise_await(promise) };
    }

    // Deregister and tear down. JS is run-to-completion, so the timer can't fire
    // between the resume microtask and here.
    let parked = with_host(|h| {
        let mut parked = h.parked.borrow_mut();
        let i = parked
            .iter()
            .position(|p| p.promise == promise)
            .expect("parked entry present at resume");
        parked.remove(i)
    });
    if let Some(timer) = parked.timer {
        clear_timeout(timer);
    }
    unsafe { emscripten_promise_destroy(promise) };
}

/// Cancel an armed host timeout, releasing its keepalive ref. `clearTimeout`
/// alone never releases the ref `emscripten_set_timeout` took, so the explicit
/// pop is needed to avoid pinning the instance alive under `EXIT_RUNTIME`.
fn clear_timeout(id: i32) {
    unsafe {
        emscripten_clear_timeout(id);
        emscripten_runtime_keepalive_pop();
    }
}
