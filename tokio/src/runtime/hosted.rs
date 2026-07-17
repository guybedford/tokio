//! The continuation lowering of the runtime's wait point.
//!
//! The scheduler drives to a fixed point, then waits. On this target there
//! are exactly two ways to express "wait, then continue": suspend the stack
//! and be resumed (JSPI — the coroutine lowering; `block_on` runs the native
//! drive loop and suspends at its park leaf), or return to the host and be
//! called back — this module. Same scheduler, same drivers; only the wait
//! differs.
//!
//! The host's axiom makes it collision-free: JS is run-to-completion, so a
//! callback only ever fires on an empty stack (the one exception, a
//! JSPI-suspended activation, is off the stack and its runtime-entered flag
//! is swapped out by the leaf). A drive never yields mid-drive, so a
//! callback can re-enter the drive **inline, unconditionally**. There is no
//! latch, no reentrancy check, and no deferral protocol — those states are
//! unexpressible. The one deferral is policy, not soundness: `Waker::wake`
//! from host context arms a 0 ms drive instead of running tasks on the
//! waker's stack ([`HostedState::arm_drive`]).
//!
//! In continuation style, the armed callbacks *are* the continuation, so
//! they hold only `Weak` refs: dropping the [`HostedRuntime`] drops the
//! runtime — in-flight roots die with it, exactly like a native
//! `Runtime::drop` — and a late callback upgrades to nothing.
//!
//! [`HostedRuntime`]: crate::runtime::HostedRuntime

use crate::emscripten::ffi::{
    emscripten_runtime_keepalive_pop, emscripten_runtime_keepalive_push, emscripten_set_timeout,
};
use crate::runtime::local_runtime::LocalRuntimeScheduler;
use crate::runtime::scheduler::current_thread::hosted_event_loop;
use crate::runtime::{context, LocalRuntime};

use std::cell::Cell;
use std::ffi::c_void;
use std::sync::{Arc, Weak};

/// A hosted runtime and its continuation state: the armed next-deadline
/// timer and pending 0 ms drive.
#[derive(Debug)]
pub(crate) struct HostedState {
    runtime: LocalRuntime,
    /// The deadline-timer epoch. Timers are fire-only — `clearTimeout`'s
    /// keepalive accounting is `EXIT_RUNTIME`-dependent (it leaks the arm's
    /// ref under `EXIT_RUNTIME=1`), so instead each drive bumps the epoch
    /// and a stale timer fires as a no-op, balancing its own accounting.
    timer_epoch: Cell<u64>,
    /// A 0 ms drive is armed ([`arm_drive`](Self::arm_drive) dedupe).
    drive_armed: Cell<bool>,
    /// Keepalive refs held for in-flight roots, released on completion or
    /// (the remainder) on drop.
    roots: Cell<usize>,
}

/// SAFETY: `wasm32-unknown-emscripten` without atomics has no second thread
/// of execution — builds with `target_feature = "atomics"` are rejected by a
/// `compile_error!` in `lib.rs` — so these impls can never be exercised
/// across threads; they only satisfy the auto-trait bounds of the handle
/// types the `Weak` hooks live in.
unsafe impl Send for HostedState {}
unsafe impl Sync for HostedState {}

impl HostedState {
    pub(crate) fn new(runtime: LocalRuntime) -> Arc<HostedState> {
        Arc::new(HostedState {
            runtime,
            timer_epoch: Cell::new(0),
            drive_armed: Cell::new(false),
            roots: Cell::new(0),
        })
    }

    pub(crate) fn runtime(&self) -> &LocalRuntime {
        &self.runtime
    }

    /// Hold the emscripten instance alive for an in-flight root.
    pub(crate) fn keepalive_push(&self) {
        self.roots.set(self.roots.get() + 1);
        // SAFETY: paired with `keepalive_pop`, or reclaimed on drop.
        unsafe { emscripten_runtime_keepalive_push() };
    }

    /// Release a completed root's keepalive ref.
    pub(crate) fn keepalive_pop(&self) {
        self.roots.set(self.roots.get() - 1);
        // SAFETY: pairs a `keepalive_push`.
        unsafe { emscripten_runtime_keepalive_pop() };
    }

    /// Drive to a quiescent fixed point, then arm the continuation: one
    /// `setTimeout` for the soonest timer deadline (readiness needs no
    /// arming; the reactor callback is persistent). Called from host
    /// callbacks — where the stack is empty by the host's axiom — and from
    /// [`HostedRuntime::drive`](crate::runtime::HostedRuntime::drive).
    pub(crate) fn drive(self: &Arc<Self>) {
        // Supersede any armed deadline timer: it will fire as a stale no-op.
        self.timer_epoch.set(self.timer_epoch.get() + 1);

        let (scheduler, rt_handle) = self.runtime.parts();
        let LocalRuntimeScheduler::CurrentThread(exec) = scheduler;
        let handle = rt_handle.inner.as_current_thread();

        context::enter_runtime(&rt_handle.inner, false, |_| {
            hosted_event_loop::drive_to_fixed_point(exec, handle);
        });

        if let Some(ms) = next_timer_ms(handle) {
            let user_data = Box::into_raw(Box::new(TimerArm {
                hosted: Arc::downgrade(self),
                epoch: self.timer_epoch.get(),
            }));
            // SAFETY: `timer_entry` consumes the box exactly once (timers
            // are fire-only, never cleared).
            unsafe { emscripten_set_timeout(Some(timer_entry), ms, user_data.cast()) };
        }
    }

    /// Arm a 0 ms drive, if none is armed. O(1): a wake from host context
    /// must never run tasks on the waker's stack.
    pub(crate) fn arm_drive(self: &Arc<Self>) {
        if self.drive_armed.replace(true) {
            return;
        }
        let raw = Weak::into_raw(Arc::downgrade(self));
        // SAFETY: `drive_entry` consumes the raw `Weak` exactly once.
        unsafe { emscripten_set_timeout(Some(drive_entry), 0.0, raw as *mut _) };
    }

}

/// An armed deadline timer's payload: whose drive, and which epoch armed it.
struct TimerArm {
    hosted: Weak<HostedState>,
    epoch: u64,
}

impl Drop for HostedState {
    fn drop(&mut self) {
        // A still-armed deadline timer fires as a no-op (its `Weak` is dead).
        // Reclaim keepalives for roots dying with the runtime, or the
        // instance would be pinned alive with nothing left to run.
        while self.roots.get() > 0 {
            self.keepalive_pop();
        }
    }
}

/// Milliseconds until the soonest unexpired timer deadline, if any.
#[cfg(feature = "time")]
fn next_timer_ms(handle: &Arc<crate::runtime::scheduler::current_thread::Handle>) -> Option<f64> {
    let clock = &handle.driver.clock;
    handle.driver.time.as_ref().and_then(|time| {
        let deadline = time.next_expiration_tick()?;
        let now = time.time_source().now(clock);
        let until = time
            .time_source()
            .tick_to_duration(deadline.saturating_sub(now));
        Some(until.as_secs_f64() * 1000.0)
    })
}

#[cfg(not(feature = "time"))]
fn next_timer_ms(
    _handle: &Arc<crate::runtime::scheduler::current_thread::Handle>,
) -> Option<f64> {
    None
}

/// The armed deadline timer fired: the stack is empty — drive, unless a
/// later drive superseded this arm (stale epoch: no-op).
unsafe extern "C-unwind" fn timer_entry(user_data: *mut c_void) {
    // SAFETY: `user_data` is the `TimerArm` box minted by `drive`.
    let arm = unsafe { Box::from_raw(user_data as *mut TimerArm) };
    let Some(hosted) = arm.hosted.upgrade() else {
        return;
    };
    if arm.epoch != hosted.timer_epoch.get() {
        return;
    }
    hosted.drive();
}

/// An armed 0 ms drive fired: the stack is empty — drive.
unsafe extern "C-unwind" fn drive_entry(user_data: *mut c_void) {
    // SAFETY: `user_data` is the raw `Weak` minted by `arm_drive`.
    let Some(hosted) = unsafe { Weak::from_raw(user_data as *const HostedState) }.upgrade() else {
        return;
    };
    hosted.drive_armed.set(false);
    hosted.drive();
}
