//! The event-loop runtime: a `current_thread` runtime driven cooperatively by
//! the host JS event loop instead of by parking a thread, so it never blocks the
//! host. There is one per thread, addressed ambiently through [`schedule`] and
//! [`drive`] (the runtime is a `thread_local` singleton, built on first use). It
//! backs `#[tokio::main]` / `#[tokio::test]` and embedder-driven `async fn`
//! exports (e.g. a `Promise`-returning JS export).
//!
//! This module is the **host glue**: `setTimeout` arming, keepalive, the
//! `schedule`/`drive` entry points. The scheduler-coupled kernel (the `Core`
//! pumping, `block_on`, `drive_loop`) lives in the `HostedEventLoop` scheduler at
//! `runtime/scheduler/current_thread/hosted_event_loop.rs`.

use crate::emscripten::ffi::{
    emscripten_clear_timeout, emscripten_get_now, emscripten_runtime_keepalive_pop,
    emscripten_runtime_keepalive_push, emscripten_set_timeout,
};
use crate::runtime::{scheduler::Driven, task::JoinError, Builder, LocalRuntime};
use crate::util::trace::SpawnMeta;

use std::cell::{Cell, OnceCell};
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
    /// True while a keepalive ref stops emscripten tearing the runtime down
    /// between turns; released once idle.
    keepalive: Cell<bool>,
    /// `#[tokio::test(start_paused = …)]` mirror, read when the runtime is built.
    #[cfg_attr(not(feature = "test-util"), allow(dead_code))]
    start_paused: Cell<bool>,
}

impl EventLoop {
    const fn new() -> EventLoop {
        EventLoop {
            runtime: OnceCell::new(),
            armed: Cell::new(None),
            in_drive: Cell::new(false),
            keepalive: Cell::new(false),
            start_paused: Cell::new(false),
        }
    }
}

thread_local! {
    static EVENT_LOOP: EventLoop = const { EventLoop::new() };
}

/// Marks a drive on the stack for its lifetime, clearing it on drop (even across
/// an unwind). Used by the host pick-up and by `block_on`, so a wake arriving
/// mid-drive no-ops instead of nesting a drive.
pub(crate) struct DriveGuard;

impl Drop for DriveGuard {
    fn drop(&mut self) {
        EVENT_LOOP.with(|el| el.in_drive.set(false));
    }
}

/// Enter a drive: set `in_drive` until the returned guard drops.
pub(crate) fn enter_drive() -> DriveGuard {
    EVENT_LOOP.with(|el| el.in_drive.set(true));
    DriveGuard
}

/// Set `start_paused` before the runtime is first built (mirrors
/// `#[tokio::test(start_paused = …)]`); no effect once it exists.
#[cfg_attr(not(feature = "test-util"), allow(dead_code))]
pub(crate) fn configure_start_paused(start_paused: bool) {
    EVENT_LOOP.with(|el| el.start_paused.set(start_paused));
}

/// Run `f` with the event-loop runtime, building it on first use.
fn with_runtime<R>(f: impl FnOnce(&LocalRuntime) -> R) -> R {
    EVENT_LOOP.with(|el| {
        let rt = el.runtime.get_or_init(|| {
            let mut builder = Builder::new_current_thread();
            builder.enable_all();
            #[cfg(feature = "test-util")]
            builder.start_paused(el.start_paused.get());
            builder
                .build_hosted_event_loop_runtime()
                .expect("failed to build the emscripten event-loop runtime")
        });
        f(rt)
    })
}

/// Enqueues `future` as a root on this thread's event-loop runtime, delivering
/// its outcome to `on_complete` once it resolves. **Does not drive it** — call
/// [`drive`] to run scheduled work.
///
/// Returns immediately. Any number of roots may be in flight, and the future
/// need not be `Send` (single host thread). A panic in it is caught and
/// delivered as `Err(JoinError)` rather than unwinding the driver, so embedders
/// (e.g. the `#[wasm_bindgen(tokio)]` → `Promise` bridge) can map `Ok`/`Err` to
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
        return false;
    }
    drive_inner();
    true
}

/// One pick-up: drive the event-loop runtime cooperatively, then arm a
/// `setTimeout` for the next timer (or `setTimeout(0)` to yield), or, if idle,
/// release the keepalive and let the host rest.
fn drive_inner() {
    let guard = enter_drive();
    let driven = with_runtime(|rt| rt.drive(HOST_DRIVE_BUDGET));
    drop(guard);

    match driven {
        Driven::Timer(ms) => arm_timeout(ms),
        Driven::Yield => arm_timeout(0.0),
        Driven::Idle => {
            disarm_timeout();
            if EVENT_LOOP.with(|el| el.keepalive.replace(false)) {
                unsafe { emscripten_runtime_keepalive_pop() };
            }
        }
    }
}

unsafe extern "C-unwind" fn timeout_entry(_arg: *mut c_void) {
    note_timeout_fired();
    drive_inner();
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
        unsafe { emscripten_clear_timeout(prev.id) };
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
        unsafe { emscripten_clear_timeout(a.id) };
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
