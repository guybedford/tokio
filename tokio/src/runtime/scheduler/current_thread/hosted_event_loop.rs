//! The emscripten `block_on` kernel: drives a root future to completion on
//! the `current_thread` scheduler, suspending the calling stack on the host
//! JS event loop via JSPI (see `runtime::hosted`) when it would otherwise
//! block. Kept here (a `current_thread` submodule) for the private access it
//! needs to the scheduler `Core`/`Context`.

use crate::loom::sync::Arc;
use crate::runtime::{
    context,
    scheduler::{self, Defer},
};

use super::{Context, Core, CurrentThread, Handle};

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::Poll::{Pending, Ready};


/// Bounds task polls per drive. Every JSPI drive is bounded — the cooperative
/// event-loop `drive()` and `block_on` alike — because both share the single
/// host thread: a self-rewaking task (`loop { yield_now().await }`, a `Notify`
/// ping-pong) would otherwise spin forever and freeze the host. On exhaustion
/// the driver yields a host turn (`drive()` returns `Yield` and re-arms
/// `setTimeout(0)`; `block_on` parks 0 ms on the host) then re-drives. Only a
/// non-JSPI `block_on` — which has no host loop to yield to and must reach a
/// synchronous fixed point or panic — is unbounded.
struct PollBudget {
    /// Remaining polls; `None` is unbounded.
    remaining: Option<u32>,
    /// Set when a bounded budget hits zero with work still ready.
    exhausted: bool,
}

impl PollBudget {
    fn unbounded() -> Self {
        Self {
            remaining: None,
            exhausted: false,
        }
    }

    fn bounded(polls: u32) -> Self {
        Self {
            remaining: Some(polls),
            exhausted: false,
        }
    }

    /// Account for one poll; returns `false` (and sets `exhausted`) once a
    /// bounded budget is spent.
    fn spend(&mut self) -> bool {
        match &mut self.remaining {
            None => true,
            Some(remaining) => {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    self.exhausted = true;
                    false
                } else {
                    true
                }
            }
        }
    }
}

/// Result of pumping the scheduler to a synchronous fixed point.
enum Outcome<T> {
    /// Root resolved.
    Completed(T),
    /// Root pending with a timer registered (resolves only via the event loop).
    WaitTimeout,
    /// Root pending, no timer; only an external waker can advance it.
    Suspend,
}

/// Synchronously drive `future` to completion on the current-thread scheduler
/// `exec`. Panics if it can't resolve without suspending (the host can't block).
/// Backs both `Runtime::block_on` and `LocalRuntime::block_on` on emscripten.
#[track_caller]
pub(crate) fn block_on<F: Future>(
    exec: &CurrentThread,
    handle: &scheduler::Handle,
    future: F,
) -> F::Output {
    crate::pin!(future);

    // One `enter_runtime` around the whole drive (as the native scheduler does):
    // it also panics on a re-entrant `block_on`, which must happen here — before
    // `pump` takes the core, which would else mask the misuse as a non-resolving
    // future.
    let outcome = context::enter_runtime(handle, false, |_| {
        prime_woken(handle);
        // SAFETY: `future` is pinned on this frame and outlives the pump.
        unsafe { pump(exec, handle, future) }
    });

    match outcome {
        Outcome::Completed(out) => out,
        Outcome::WaitTimeout | Outcome::Suspend => panic!(
            "Cannot block_on a future that does not resolve synchronously on a \
             runtime built with `emscripten_jspi(false)`: completing it would \
             require suspending to the host event loop, which needs JSPI. Build \
             the runtime with JSPI enabled (the default) and link with `-sJSPI`."
        ),
    }
}

/// Prime `woken` so the first `drive_loop` iteration polls the never-yet-polled
/// root (whose waker hasn't fired).
fn prime_woken(handle: &scheduler::Handle) {
    handle
        .as_current_thread()
        .shared
        .woken
        .store(true, Ordering::Release);
}

/// Check out `Core`, set the scheduler context, run the drive loop, then return
/// `Core`. Must be called inside [`block_on`]'s `enter_runtime`.
///
/// With the runtime's `jspi` config (the default), a fixed point that would
/// have to suspend parks the whole stack via JSPI instead of returning: the
/// core is checked back into its slot (so host callbacks reentering the
/// instance can drive — including a nested `block_on` on the same runtime),
/// the stack suspends until a wake or the next timer deadline, then re-acquires
/// the core and re-enters the fixed point. With `jspi` disabled the first
/// would-suspend outcome is returned and `block_on` panics.
///
/// # Safety
/// `future` must remain valid for the call.
unsafe fn pump<F: Future>(
    exec: &CurrentThread,
    sched: &scheduler::Handle,
    mut future: Pin<&mut F>,
) -> Outcome<F::Output> {
    let handle: Arc<Handle> = sched.as_current_thread().clone();
    let core = match exec.core.take() {
        Some(c) => c,
        None => return Outcome::Suspend,
    };
    let cx = scheduler::Context::CurrentThread(Context {
        handle,
        core: RefCell::new(Some(core)),
        defer: Defer::new(),
    });

    // Return the core to the scheduler on the way out, even on panic, so the
    // runtime stays tear-down-able (mirrors the native `CoreGuard`).
    struct RestoreCore<'a> {
        exec: &'a CurrentThread,
        cx: &'a scheduler::Context,
    }
    impl Drop for RestoreCore<'_> {
        fn drop(&mut self) {
            if let Some(core) = self.cx.expect_current_thread().core.borrow_mut().take() {
                self.exec.core.set(core);
            }
        }
    }
    let _restore = RestoreCore { exec, cx: &cx };

    let jspi = cx.expect_current_thread().handle.shared.config.jspi;

    loop {
        // Bracket each drive with the busy-time/poll metrics the native
        // scheduler records around its park loop.
        if let Some(core) = cx.expect_current_thread().core.borrow_mut().as_mut() {
            core.metrics.start_processing_scheduled_tasks();
        }
        // With JSPI, `block_on` drives cooperatively like the event-loop
        // `drive()`: a bounded budget so a self-rewaking task (a `yield_now`
        // spin, a `Notify` ping-pong) yields a host turn — letting the reactor
        // and other callbacks run — instead of starving the single thread.
        // Without JSPI there is no host loop to yield to, so it must run to a
        // synchronous fixed point (unbounded) or panic.
        let mut budget = if jspi {
            PollBudget::bounded(crate::runtime::hosted::HOST_DRIVE_BUDGET)
        } else {
            PollBudget::unbounded()
        };
        let outcome = context::set_scheduler(&cx, || drive_loop(future.as_mut(), &cx, &mut budget));
        {
            let inner = cx.expect_current_thread();
            if let Some(core) = inner.core.borrow_mut().as_mut() {
                core.metrics.end_processing_scheduled_tasks();
                core.submit_metrics(&inner.handle);
            }
        }
        match outcome {
            Outcome::Completed(_) => return outcome,
            Outcome::WaitTimeout | Outcome::Suspend => {
                if !jspi {
                    return outcome;
                }
                // Ready work still queued (budget spent, or a `yield_now`
                // deferral re-woke the root at the cliff): yield a 0 ms host turn
                // so the reactor and other host callbacks run, then re-drive.
                // Otherwise a genuine cliff: park until a wake or the next timer
                // deadline.
                let inner = cx.expect_current_thread();
                let handle = &inner.handle;
                let work_remains = budget.exhausted || has_ready_work(inner, handle);
                let timeout_ms = if work_remains {
                    0.0
                } else {
                    cliff_timeout_ms(handle)
                };
                park_at_cliff(exec, &cx, timeout_ms);
            }
        }
    }
}

/// Milliseconds until the next timer deadline, when the time driver exists and
/// has one armed. Without the `time` feature there is never a deadline.
#[cfg(feature = "time")]
fn next_timer_ms(handle: &Arc<Handle>) -> Option<f64> {
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
fn next_timer_ms(_handle: &Arc<Handle>) -> Option<f64> {
    None
}

/// Milliseconds until the next timer deadline (the bound on a cliff park), or
/// `-1.0` for wake-only when no timer is armed.
fn cliff_timeout_ms(handle: &Arc<Handle>) -> f64 {
    next_timer_ms(handle).unwrap_or(-1.0)
}

/// Park the drive at its progression cliff via JSPI: check the core into the
/// scheduler's slot, suspend for `timeout_ms` (`-1.0` = until a wake; `0.0` =
/// a bare host turn on budget exhaustion), then re-acquire the core. The native
/// `park_internal` analogue.
fn park_at_cliff(exec: &CurrentThread, cx: &scheduler::Context, timeout_ms: f64) {
    let inner = cx.expect_current_thread();
    let handle = &inner.handle;

    {
        let mut borrow = inner.core.borrow_mut();
        if let Some(core) = borrow.as_mut() {
            core.metrics.about_to_park();
            core.submit_metrics(handle);
        }
        // Check the core into the slot: host callbacks reentering the instance
        // while this stack is suspended can drive with it.
        if let Some(core) = borrow.take() {
            exec.core.set(core);
        }
    }

    crate::runtime::hosted::park_on_host(timeout_ms);

    // Re-acquire. Resumption is a microtask, so no drive is mid-fixed-point;
    // the slot is populated unless a sibling stack resumed first in this batch
    // and hasn't re-parked yet — re-park briefly rather than spin.
    loop {
        match exec.core.take() {
            Some(core) => {
                let mut borrow = cx.expect_current_thread().core.borrow_mut();
                *borrow = Some(core);
                borrow.as_mut().expect("core present").metrics.unparked();
                break;
            }
            None => crate::runtime::hosted::park_on_host(0.0),
        }
    }
}

/// Ready work is queued: a woken root, an injected task, or a scheduled task.
/// Distinguishes a spin that just needs a host turn (re-drive) from a genuine
/// cliff that may park (or auto-advance a paused clock).
fn has_ready_work(inner: &Context, handle: &Arc<Handle>) -> bool {
    handle.shared.woken.load(Ordering::Acquire)
        || handle.shared.inject.len() > 0
        || inner
            .core
            .borrow()
            .as_ref()
            .is_some_and(|core| !core.tasks.is_empty())
}

/// Drive until the root resolves, no progress is possible, or `budget` is
/// exhausted (event-loop drives only). On exhaustion it flushes deferred wakers
/// to the run queue and returns with `budget.exhausted` set.
fn drive_loop<F: Future>(
    mut future: Pin<&mut F>,
    cx: &scheduler::Context,
    budget: &mut PollBudget,
) -> Outcome<F::Output> {
    let inner = cx.expect_current_thread();
    let handle = &inner.handle;

    loop {
        if budget.exhausted {
            // Flush deferred (`yield_now`) wakers into the run queue so they
            // survive this drive's `Defer` being dropped and run on the next.
            inner.defer.wake();
            break;
        }
        let mut progressed = false;

        if handle.reset_woken() {
            let spent = budget.spend();
            if let Some(out) = poll_root(future.as_mut(), handle) {
                return Outcome::Completed(out);
            }
            // A `Pending` root is not progress: it has parked on io/timer or
            // re-deferred a `yield_now`, none of which can advance again without
            // a host turn (JS callbacks and timers only fire off the host loop).
            // Only genuine work below (a task ran, a timer fired, fresh io) keeps
            // the spin going; otherwise we fall through to the cliff and cede.
            if !spent {
                inner.defer.wake();
                break;
            }
        }
        if drain_tasks(handle, &inner.core, budget) {
            progressed = true;
        }
        #[cfg(feature = "time")]
        if let Some(t) = handle.driver.time.as_ref() {
            t.process(&handle.driver.clock);
            // `process` may fire the root waker (sets `woken`, doesn't queue a
            // task); count it as progress so the next iteration polls.
            if handle.shared.woken.load(Ordering::Acquire) {
                progressed = true;
            }
        }
        if drain_tasks(handle, &inner.core, budget) {
            progressed = true;
        }
        inner.defer.wake();
        if drain_tasks(handle, &inner.core, budget) {
            progressed = true;
        }

        if !progressed {
            // The progression cliff: no task or timer advanced. I/O readiness
            // arrives only via the epoll callback on a host tick, so pending
            // I/O is the host's to deliver — parking (JSPI) or returning to the
            // host lets it fire.
            // Auto-advance is a test-clock-only *nominal* jump straight to the
            // next deadline, so it may fire only when genuinely quiescent. A
            // pending wake/`yield_now` (e.g. a mid-flight `time::advance`) or a
            // queued task must re-drive first — otherwise the nominal jump
            // stacks onto controlled time. Real clocks never jump here; they
            // advance by measured host time when the timer park returns.
            if !has_ready_work(inner, handle) && auto_advance_to_next_timer(handle) {
                continue;
            }
            break;
        }
    }

    if next_timer_ms(handle).is_some() {
        return Outcome::WaitTimeout;
    }
    Outcome::Suspend
}

/// If the paused test clock may auto-advance, jump to the next timer deadline so
/// a synchronous drive can fire it; returns `true` if time advanced. Always
/// `false` without `test-util` or when the clock can't advance / has no timer.
#[cfg(feature = "test-util")]
fn auto_advance_to_next_timer(handle: &Arc<Handle>) -> bool {
    let clock = &handle.driver.clock;
    if !clock.can_auto_advance() {
        return false;
    }
    let time = match handle.driver.time.as_ref() {
        Some(t) => t,
        None => return false,
    };
    let deadline = match time.next_expiration_tick() {
        Some(d) => d,
        None => return false,
    };
    let delta = deadline.saturating_sub(time.time_source().now(clock));
    if delta == 0 {
        return false;
    }
    let _ = clock.advance(std::time::Duration::from_millis(delta));
    true
}

#[cfg(not(feature = "test-util"))]
fn auto_advance_to_next_timer(_handle: &Arc<Handle>) -> bool {
    false
}

fn drain_tasks(
    handle: &Arc<Handle>,
    core_cell: &RefCell<Option<Box<Core>>>,
    budget: &mut PollBudget,
) -> bool {
    let mut any = false;
    loop {
        if budget.exhausted {
            return any;
        }
        let mut borrow = core_cell.borrow_mut();
        let core = borrow.as_mut().expect("core present");
        if core.unhandled_panic {
            panic!(
                "a spawned task panicked and the runtime is configured to shut down on unhandled panic"
            );
        }
        core.tick();
        let next = core.next_task(handle);
        drop(borrow);
        match next {
            Some(task) => {
                any = true;
                let task = handle.shared.owned.assert_owner(task);

                // Bracket the poll with the same metrics the native `run_task`
                // records (poll count + per-poll timing histogram).
                core_cell
                    .borrow_mut()
                    .as_mut()
                    .expect("core present")
                    .metrics
                    .start_poll(task.get_scheduled_at().prepare(handle.shared.started_at));

                // Fresh coop budget per poll, matching native `run_task`.
                #[cfg(tokio_unstable)]
                {
                    let meta = task.task_meta();
                    handle.task_hooks.poll_start_callback(&meta);
                    crate::task::coop::budget(|| task.run());
                    handle.task_hooks.poll_stop_callback(&meta);
                }
                #[cfg(not(tokio_unstable))]
                crate::task::coop::budget(|| task.run());

                core_cell
                    .borrow_mut()
                    .as_mut()
                    .expect("core present")
                    .metrics
                    .end_poll();

                if !budget.spend() {
                    return any;
                }
            }
            None => return any,
        }
    }
}

fn poll_root<F: Future>(future: Pin<&mut F>, handle: &Arc<Handle>) -> Option<F::Output> {
    let waker = crate::util::waker_ref(handle);
    let mut cx = std::task::Context::from_waker(&waker);
    match crate::task::coop::budget(|| future.poll(&mut cx)) {
        Ready(out) => Some(out),
        Pending => None,
    }
}
