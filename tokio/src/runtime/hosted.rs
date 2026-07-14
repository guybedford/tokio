//! Host glue for the emscripten runtime: the JSPI park primitive and its
//! timer plumbing over `emscripten::ffi`. A `block_on` (or blocking API) that
//! cannot make progress suspends its calling stack on the host JS event loop
//! here, and is resumed by host `setTimeout` wakes and external wakers. The
//! scheduler-coupled kernel (the `Core` pumping, `block_on`, `drive_loop`)
//! lives at `runtime/scheduler/current_thread/hosted_event_loop.rs`.
//!
//! The JSPI-parked stacks are per-*thread* state (a `ParkThread::park` isn't
//! even runtime-associated) and live in the [`HostContext`] member of tokio's
//! `runtime::context` thread-local.

use crate::emscripten::ffi::{
    emscripten_clear_timeout, emscripten_promise_await, emscripten_promise_create,
    emscripten_promise_destroy, emscripten_promise_resolve, emscripten_runtime_keepalive_pop,
    emscripten_runtime_keepalive_push, emscripten_set_timeout, EmPromise, EM_PROMISE_FULFILL,
};

use std::cell::RefCell;
use std::ffi::c_void;

/// Polls per drive before yielding to the host: large enough to amortize the
/// host-turn round-trip, small enough not to stall a frame. Bounds `block_on`'s
/// JSPI drive so a self-rewaking task cannot starve the host loop.
pub(crate) const HOST_DRIVE_BUDGET: u32 = 4096;

/// This thread's host-loop state: the JSPI-parked stacks. Lives as a field of
/// tokio's canonical per-thread [`runtime::context`](crate::runtime::context)
/// struct — the same place the runtime-entered flag and scheduler context
/// live — rather than a parallel thread-local.
pub(crate) struct HostContext {
    /// The JSPI-parked stacks on this thread, as the promise each is suspended
    /// on plus its optional deadline timer.
    parked: RefCell<Vec<Parked>>,
}

/// One JSPI-parked stack: the `em_promise` its `emscripten_promise_await` is
/// suspended on, and the host timer armed for its timer deadline (cleared by
/// the resume path; `None` once fired or when parked without a deadline).
struct Parked {
    promise: EmPromise,
    timer: Option<i32>,
}

impl HostContext {
    pub(crate) const fn new() -> HostContext {
        HostContext {
            parked: RefCell::new(Vec::new()),
        }
    }
}

/// This thread's [`HostContext`], from `runtime::context`.
fn with_host<R>(f: impl FnOnce(&HostContext) -> R) -> R {
    crate::runtime::context::with_hosted(f)
}

/// Resolve every parked stack's promise. Each resumes as a microtask (after
/// the current callback unwinds), re-checks its fixed point, and re-parks if
/// its wake hasn't arrived. Entries stay registered until their own resume
/// path removes them; re-resolving a settled promise is a no-op. Never runs
/// tasks on the caller's stack, so `Waker::wake` stays O(1).
pub(crate) fn unpark_all() {
    with_host(|h| {
        for parked in h.parked.borrow().iter() {
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
/// ([`unpark_all`]) or `timeout_ms` elapses (negative = no timer). The
/// kernel's park primitive: called at a `block_on` progression cliff (after
/// checking its core back in) and by `ParkThread::park` for the blocking APIs.
///
/// Across the suspension the per-stack runtime-entered flag is swapped out, so
/// host callbacks reentering the instance can drive runtimes (including the
/// suspended one's, whose core is back in its slot) as if this stack weren't
/// there. Restored on resume, which runs as a microtask: never while another
/// drive is mid-fixed-point.
pub(crate) fn park_on_host(timeout_ms: f64) {
    struct ParkGuard {
        prev_enter: crate::runtime::context::EnterRuntime,
    }
    impl Drop for ParkGuard {
        fn drop(&mut self) {
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
    with_host(|h| h.parked.borrow_mut().push(Parked { promise, timer }));

    {
        // A suspended stack is pending work the emscripten runtime can't see:
        // with `EXIT_RUNTIME` and no keepalive, any managed callback firing
        // mid-suspension would run `maybeExit` and tear the runtime down under
        // us. Hold a keepalive ref for the suspension's lifetime.
        unsafe { emscripten_runtime_keepalive_push() };
        let _guard = ParkGuard {
            prev_enter: crate::runtime::context::jspi_exit_runtime_for_park(),
        };
        // SAFETY: suspends this stack (JSPI) until the promise settles — via
        // `unpark_all` or the deadline timer.
        let _ = unsafe { emscripten_promise_await(promise) };
    }

    // Deregister and tear down. The timer cannot fire between the resume
    // microtask and here (JS is run-to-completion), so clearing then
    // destroying cannot race the callback.
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
