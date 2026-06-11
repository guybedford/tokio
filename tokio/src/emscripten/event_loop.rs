//! The event-loop runtime: a `current_thread` runtime driven cooperatively by
//! the host JS event loop instead of by parking a thread, so it never blocks
//! the host. There is one per thread, addressed ambiently through [`schedule`]
//! and [`drive`] (the runtime is a `thread_local` singleton, built on first
//! use). It backs embedder-driven `async fn` exports (e.g. a
//! `Promise`-returning JS export); `#[tokio::main]` / `#[tokio::test]` instead
//! use the native expansion over a JSPI-parking `block_on`.
//!
//! This module is the **host glue**: `setTimeout` arming, keepalive, the
//! `schedule`/`drive` entry points. The scheduler-coupled kernel (the `Core`
//! pumping, `block_on`, `drive_loop`) lives in the `HostedEventLoop` scheduler at
//! `runtime/scheduler/current_thread/hosted_event_loop.rs`.

use crate::emscripten::ffi::{
    emscripten_clear_timeout, emscripten_get_now, emscripten_promise_await,
    emscripten_promise_create, emscripten_promise_destroy, emscripten_promise_resolve,
    emscripten_runtime_keepalive_pop, emscripten_runtime_keepalive_push, emscripten_set_timeout,
    EmPromise, EM_PROMISE_FULFILL,
};
use crate::runtime::{scheduler::Driven, task::JoinError, Builder, LocalRuntime};
use crate::util::trace::SpawnMeta;

use std::cell::{Cell, OnceCell, RefCell};
use std::ffi::c_void;
use std::future::Future;

#[derive(Clone, Copy)]
struct Armed {
    id: i32,
    fires_at_ms: f64,
}

/// Polls per event-loop drive before yielding to the host: large enough to
/// amortize the `setTimeout(0)` round-trip, small enough not to stall a frame.
const HOST_DRIVE_BUDGET: u32 = 4096;

/// This thread's event-loop state: the `current_thread` runtime plus the
/// host-timer bookkeeping and drive/keepalive latches. Per-field cells (rather
/// than one outer `RefCell`) because a drive holds the post-init-immutable
/// `runtime` while running tasks that independently arm timers and flip the
/// latches. The I/O reactor is a separate thread-local (see
/// `runtime::io::emscripten`), keyed by the global socket callbacks.
struct EventLoop {
    /// The event-loop runtime, built on first use.
    runtime: OnceCell<LocalRuntime>,
    /// The single outstanding `setTimeout` (the soonest timer deadline), or
    /// `None` when none is armed. At most one host timer is ever outstanding;
    /// `arm_timeout` / `disarm_timeout` / `note_timeout_fired` are its only
    /// mutators.
    armed: Cell<Option<Armed>>,
    /// True while a drive is on the stack. A wake during a drive records
    /// readiness for the active fixed point or a later pick-up rather than
    /// nesting a drive.
    in_drive: Cell<bool>,
    /// A pick-up was requested while a drive (or `block_on`) held `in_drive`.
    /// Consumed by the next drive (its fixed point observes the queued work) or
    /// converted to a `setTimeout(0)` when the latch holder exits — so a wake
    /// arriving under a foreign `block_on` is never dropped.
    pending: Cell<bool>,
    /// The JSPI-parked stacks on this thread (drives suspended at their
    /// progression cliff), as the promise each is awaiting plus its optional
    /// wake timer. A pick-up delivery resolves them all; each resumes,
    /// re-checks its fixed point, and re-parks if its wake hasn't arrived.
    parked: RefCell<Vec<Parked>>,
    /// True while a keepalive ref stops emscripten tearing the runtime down
    /// between turns; released once idle.
    keepalive: Cell<bool>,
}

/// One JSPI-parked stack: the `em_promise` its `emscripten_promise_await` is
/// suspended on, and the host timer armed for its timer deadline (cleared by
/// the resume path; `None` once fired or when parked without a deadline).
struct Parked {
    promise: EmPromise,
    timer: Option<i32>,
}

impl EventLoop {
    const fn new() -> EventLoop {
        EventLoop {
            runtime: OnceCell::new(),
            armed: Cell::new(None),
            in_drive: Cell::new(false),
            pending: Cell::new(false),
            parked: RefCell::new(Vec::new()),
            keepalive: Cell::new(false),
        }
    }
}

thread_local! {
    static EVENT_LOOP: EventLoop = const { EventLoop::new() };
}

/// Marks a drive on the stack for its lifetime, restoring the prior latch state
/// on drop (even across an unwind). Used by the host pick-up, by `block_on`,
/// and around reactor wake dispatch, so a wake arriving mid-drive latches a
/// pending pick-up instead of nesting a drive.
pub(crate) struct DriveGuard {
    prev: bool,
}

impl Drop for DriveGuard {
    fn drop(&mut self) {
        EVENT_LOOP.with(|el| el.in_drive.set(self.prev));
    }
}

/// Enter a drive: set `in_drive` until the returned guard drops.
pub(crate) fn enter_drive() -> DriveGuard {
    DriveGuard {
        prev: EVENT_LOOP.with(|el| el.in_drive.replace(true)),
    }
}

/// Converts a pending pick-up into a delivery on drop. Held (outermost) across
/// `block_on`, whose fixed point only pumps its own runtime: an event-loop wake
/// arriving under it must still get a host turn, including on the unwind path.
pub(crate) struct PendingFlush;

impl Drop for PendingFlush {
    fn drop(&mut self) {
        if EVENT_LOOP.with(|el| el.pending.take()) {
            deliver_pickup();
        }
    }
}

pub(crate) fn pending_flush() -> PendingFlush {
    PendingFlush
}

/// Deliver a pick-up: resume any JSPI-parked drives (their wake may be the one
/// being delivered; each re-checks its fixed point and re-parks if not) and arm
/// a `setTimeout(0)` host turn for the event-loop runtime. Both halves are
/// O(1) deferrals — no task runs on the caller's stack.
fn deliver_pickup() {
    unpark_all();
    arm_timeout(0.0);
}

/// Resolve every parked stack's promise. Each resumes as a microtask (after
/// the current callback unwinds), re-checks its fixed point, and re-parks if
/// its wake hasn't arrived. Entries stay registered until their own resume
/// path removes them; re-resolving a settled promise is a no-op.
fn unpark_all() {
    EVENT_LOOP.with(|el| {
        for parked in el.parked.borrow().iter() {
            // SAFETY: the handle is live until its parked stack resumes and
            // destroys it, which cannot happen while this callback runs.
            unsafe {
                emscripten_promise_resolve(parked.promise, EM_PROMISE_FULFILL, std::ptr::null_mut())
            };
        }
    });
}

/// A parked stack's deadline timer: settle its promise so it resumes. The
/// timer is spent, so forget its id — the resume path must neither clear it
/// nor release the keepalive ref emscripten already released on fire.
unsafe extern "C-unwind" fn park_timeout_entry(promise: *mut c_void) {
    EVENT_LOOP.with(|el| {
        if let Some(parked) = el
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

/// Request that the event-loop runtime be driven: how a wake arriving from host
/// context (a task scheduled or a waker fired outside any drive) reaches the
/// scheduler. Never runs tasks on the caller's stack — `Waker::wake` stays O(1)
/// and can't reenter user code. While a drive or `block_on` holds the latch the
/// request is recorded for its exit; otherwise it is delivered immediately
/// (parked drives resumed, a `setTimeout(0)` pick-up armed).
pub(crate) fn request_pickup() {
    let latched = EVENT_LOOP.with(|el| {
        if el.in_drive.get() {
            el.pending.set(true);
            true
        } else {
            false
        }
    });
    if !latched {
        deliver_pickup();
    }
}

/// Suspend the current drive on the host event loop via JSPI until a wake
/// ([`request_pickup`]) or `timeout_ms` elapses (negative = no timer). The
/// kernel's park primitive: called at a `block_on` progression cliff (after
/// checking its core back in) and by `ParkThread::park` for the blocking APIs.
///
/// Across the suspension the per-stack context is swapped out — the
/// runtime-entered flag and the `in_drive` latch — so host callbacks reentering
/// the instance can drive runtimes (including the suspended one's, whose core
/// is back in its slot) as if this stack weren't there. Restored on resume,
/// which runs as a microtask: never while another drive is mid-fixed-point.
pub(crate) fn park_on_host(timeout_ms: f64) {
    // A pick-up latched during the drive that is now parking would otherwise
    // wait out the suspension: deliver it first (we are not yet registered as
    // parked, so this cannot self-resolve).
    if EVENT_LOOP.with(|el| el.pending.take()) {
        deliver_pickup();
    }

    struct ParkGuard {
        prev_enter: crate::runtime::context::EnterRuntime,
        prev_latch: bool,
    }
    impl Drop for ParkGuard {
        fn drop(&mut self) {
            EVENT_LOOP.with(|el| el.in_drive.set(self.prev_latch));
            crate::runtime::context::jspi_restore_runtime_after_park(self.prev_enter);
            // SAFETY: paired with the push below.
            unsafe { emscripten_runtime_keepalive_pop() };
        }
    }

    let promise = unsafe { emscripten_promise_create() };
    let timer = if timeout_ms >= 0.0 {
        // SAFETY: the resume path below clears the timer (or it has already
        // fired) before the promise handle is destroyed, so the callback's
        // `user_data` never dangles.
        Some(unsafe { emscripten_set_timeout(Some(park_timeout_entry), timeout_ms, promise) })
    } else {
        None
    };
    EVENT_LOOP.with(|el| el.parked.borrow_mut().push(Parked { promise, timer }));

    {
        // A suspended stack is pending work the emscripten runtime can't see:
        // with `EXIT_RUNTIME` and no keepalive, any managed callback (e.g. the
        // event loop's own `setTimeout` pick-up) firing mid-suspension would
        // run `maybeExit` and tear the runtime down under us. Hold a keepalive
        // ref for the suspension's lifetime.
        unsafe { emscripten_runtime_keepalive_push() };
        let _guard = ParkGuard {
            prev_enter: crate::runtime::context::jspi_exit_runtime_for_park(),
            prev_latch: EVENT_LOOP.with(|el| el.in_drive.replace(false)),
        };
        // SAFETY: suspends this stack (JSPI) until the promise settles — via
        // `unpark_all` or the deadline timer.
        let _ = unsafe { emscripten_promise_await(promise) };
    }

    // Deregister and tear down. The timer cannot fire between the resume
    // microtask and here (JS is run-to-completion), so clearing then
    // destroying cannot race the callback.
    let parked = EVENT_LOOP.with(|el| {
        let mut parked = el.parked.borrow_mut();
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

/// Run `f` with the event-loop runtime, building it on first use.
fn with_runtime<R>(f: impl FnOnce(&LocalRuntime) -> R) -> R {
    EVENT_LOOP.with(|el| {
        let rt = el.runtime.get_or_init(|| {
            let mut builder = Builder::new_current_thread();
            builder.enable_all();
            builder
                .build_hosted_event_loop_runtime()
                .expect("failed to build the emscripten event-loop runtime")
        });
        f(rt)
    })
}

/// Enqueues `future` as a root on this thread's event-loop runtime, delivering
/// its outcome to `on_complete` once it resolves. **Never runs it
/// synchronously**: the root is queued only — call [`drive`] to run it before
/// returning to the host (a `setTimeout(0)` pick-up is also armed, so the work
/// is not lost if the caller doesn't), and `on_complete` is never invoked
/// before `schedule` returns.
///
/// Any number of roots may be in flight, and the future need not be `Send`
/// (single host thread). A panic in it is caught and delivered as
/// `Err(JoinError)` rather than unwinding the driver, so embedders (e.g. the
/// `#[wasm_bindgen(tokio)]` → `Promise` bridge) can map `Ok`/`Err` to
/// resolve/reject.
pub fn schedule<F, C>(future: F, on_complete: C)
where
    F: Future + 'static,
    F::Output: 'static,
    C: FnOnce(Result<F::Output, JoinError>) + 'static,
{
    with_runtime(|rt| {
        let handle = rt.handle();
        // SAFETY: the runtime is local to this single thread (`local_tid` set to
        // it), so spawning non-`Send` tasks and driving them here is sound.
        let join = {
            let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&future));
            unsafe { handle.spawn_local_named(future, meta) }
        };
        // A second task awaits the root's `JoinHandle`, turning a root panic into
        // `Err(JoinError)` here rather than an unwind of the driver.
        let completer = async move {
            on_complete(join.await);
        };
        let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&completer));
        // Detached; delivers `on_complete` when the root resolves.
        unsafe {
            drop(handle.spawn_local_named(completer, meta));
        }
    });
    if !EVENT_LOOP.with(|el| el.keepalive.replace(true)) {
        unsafe { emscripten_runtime_keepalive_push() };
    }
}

/// Drive this thread's event-loop runtime to a quiescent fixed point: run ready
/// tasks and fired timers, harvest I/O readiness, then arm the next host wake (a
/// `setTimeout` for the soonest timer) or, if idle, let the instance rest.
/// Returns `false` (a no-op) while a drive is already on the stack — that
/// drive's fixed point observes the wake, so drives never nest; `true` if it ran
/// one. Also how external wakes (driver `unpark`, I/O reactor) re-enter.
///
/// Submit work with [`schedule`], then `drive` to run it. After the first
/// `drive`, the host's own wakes (timer ticks, socket callbacks) re-drive the
/// runtime, so a one-shot `schedule` + `drive` is self-sustaining.
pub fn drive() -> bool {
    if EVENT_LOOP.with(|el| el.in_drive.get()) {
        // Record the request: an event-loop drive's fixed point observes it; a
        // foreign `block_on` does not, so its exit converts it to a pick-up.
        EVENT_LOOP.with(|el| el.pending.set(true));
        return false;
    }
    drive_inner();
    true
}

/// One pick-up: drive the event-loop runtime cooperatively, then arm a
/// `setTimeout` for the next timer (or `setTimeout(0)` to yield), or, if idle,
/// release the keepalive and let the host rest.
fn drive_inner() {
    // A pick-up requested before this drive: the fixed point below observes
    // everything *queued* (so no `setTimeout(0)` is needed), but parked stacks
    // are not queued work — their wake must still be delivered by resuming
    // them. (E.g. the reactor latches the drive flag around its wakes, so a
    // wake for a parked `block_on`'s runtime lands here as pending.)
    if EVENT_LOOP.with(|el| el.pending.take()) {
        unpark_all();
    }

    let guard = enter_drive();
    let driven = with_runtime(|rt| rt.drive(HOST_DRIVE_BUDGET));
    drop(guard);

    match driven {
        Driven::Timer(ms) => arm_timeout(ms),
        Driven::Yield => arm_timeout(0.0),
        // The core is checked out elsewhere; that holder re-arms on exit, so
        // leave the timer and keepalive untouched.
        Driven::Busy => {}
        Driven::Idle => {
            disarm_timeout();
            if EVENT_LOOP.with(|el| el.keepalive.replace(false)) {
                unsafe { emscripten_runtime_keepalive_pop() };
            }
        }
    }

    // A pick-up requested during this drive came through a path the fixed
    // point can't satisfy (e.g. a wake for a parked drive's runtime); deliver
    // it rather than drop it. After Idle's disarm, so the host turn survives.
    if EVENT_LOOP.with(|el| el.pending.take()) {
        deliver_pickup();
    }
}

unsafe extern "C-unwind" fn timeout_entry(_arg: *mut c_void) {
    note_timeout_fired();
    drive_inner();
}

/// Cancel an armed host timeout, releasing its keepalive ref.
/// `emscripten_set_timeout` holds a runtime-keepalive ref until the callback
/// fires (`safeSetTimeout`), but `emscripten_clear_timeout` is a bare
/// `clearTimeout` that never releases it — without the explicit pop every
/// cancelled timer would pin the instance alive, which under `EXIT_RUNTIME`
/// suppresses `onExit` when `main` finishes.
fn clear_timeout(id: i32) {
    unsafe {
        emscripten_clear_timeout(id);
        emscripten_runtime_keepalive_pop();
    }
}

/// Arm a host `setTimeout` for `delay_ms` from now, coalescing to the soonest
/// deadline: a nearer outstanding timer is kept; a farther one is replaced.
fn arm_timeout(delay_ms: f64) {
    let delay = delay_ms.max(0.0);
    let fires_at = unsafe { emscripten_get_now() } + delay;
    if let Some(prev) = EVENT_LOOP.with(|el| el.armed.get()) {
        if prev.fires_at_ms <= fires_at {
            return;
        }
        clear_timeout(prev.id);
    }
    let id = unsafe { emscripten_set_timeout(Some(timeout_entry), delay, std::ptr::null_mut()) };
    EVENT_LOOP.with(|el| {
        el.armed.set(Some(Armed {
            id,
            fires_at_ms: fires_at,
        }))
    });
}

/// Cancel the outstanding (unfired) host timer, if any. Used when the runtime
/// goes idle.
fn disarm_timeout() {
    if let Some(a) = EVENT_LOOP.with(|el| el.armed.take()) {
        clear_timeout(a.id);
    }
}

/// Forget the host timer that just fired. Runs from the timer's own callback, so
/// the `setTimeout` is already spent — unlike [`disarm_timeout`] it must NOT
/// `emscripten_clear_timeout`; it only drops the dead id so the following drive
/// re-arms cleanly. No wake is dropped across the momentary `None`: JS is
/// run-to-completion, so nothing runs before `drive` re-establishes `IN_DRIVE`.
fn note_timeout_fired() {
    EVENT_LOOP.with(|el| el.armed.set(None));
}
