//! The hosted event-loop scheduler kernel: the `current_thread`-coupled half
//! of the event-loop runtime, kept here (a `current_thread` submodule) for its
//! private access to the scheduler `Core`/`Context`. The host-loop glue (timer
//! arming, keepalive, `schedule`, the public `drive`) is exposed via
//! `crate::runtime::hosted`.
//!
//! Two entry points share one fixed-point [`drive_loop`]:
//! * [`pump`] backs `block_on`: drives one root to completion, parking on the
//!   host via JSPI at its cliffs (panicking without `-sJSPI`).
//! * [`drive_exec`] backs `schedule`/`drive`: cooperatively pumps a hosted
//!   runtime to a quiescent fixed point, bounded by a poll budget, never
//!   blocking.

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

/// The `Arc`'d half of a hosted runtime's scheduler: the drive capability
/// that host callbacks resolve through the runtime's
/// [`HostedState`](crate::runtime::hosted::HostedState) (which holds a
/// `Weak` to this). Heap-shared so the user may move or drop the
/// [`LocalRuntime`](crate::runtime::LocalRuntime) freely while callbacks are
/// armed.
pub(crate) struct HostedExec {
    pub(crate) exec: CurrentThread,
    pub(crate) rt_handle: crate::runtime::Handle,
    #[cfg(tokio_unstable)] // read only by the HostedRuntime accessor
    pub(crate) hosted: Arc<crate::runtime::hosted::HostedState>,
}

impl std::fmt::Debug for HostedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostedExec").finish()
    }
}

/// The event-loop runtime's scheduler: a `current_thread` scheduler driven
/// cooperatively from the host event loop rather than by `block_on`. The
/// [`LocalRuntimeScheduler::HostedEventLoop`](crate::runtime::LocalRuntime) variant.
#[cfg(tokio_unstable)]
#[derive(Debug)]
pub(crate) struct HostedEventLoop {
    inner: Arc<HostedExec>,
}

#[cfg(tokio_unstable)]
impl HostedEventLoop {
    /// Wires the runtime's `HostedState` to the freshly built scheduler: after
    /// this, the state's armed host callbacks can drive it.
    pub(crate) fn new(
        exec: CurrentThread,
        rt_handle: crate::runtime::Handle,
        hosted: Arc<crate::runtime::hosted::HostedState>,
    ) -> HostedEventLoop {
        let inner = Arc::new(HostedExec {
            exec,
            rt_handle,
            hosted: hosted.clone(),
        });
        hosted.set_exec(Arc::downgrade(&inner));
        HostedEventLoop { inner }
    }

    pub(crate) fn hosted(&self) -> &Arc<crate::runtime::hosted::HostedState> {
        &self.inner.hosted
    }

    /// Synchronously drive `future` to completion (panicking if it would
    /// suspend), delegating to the inner `current_thread` scheduler — the same
    /// drive-or-panic `Runtime::block_on` gets. Distinct from [`drive_exec`],
    /// which pumps cooperatively and never panics.
    #[track_caller]
    pub(crate) fn block_on<F: Future>(&self, handle: &scheduler::Handle, future: F) -> F::Output {
        self.inner.exec.block_on(handle, future)
    }
}

/// Drive to a quiescent fixed point without blocking, bounded by `budget`
/// polls — the cooperative counterpart of `block_on`. Backs the host
/// pick-up in [`crate::runtime::hosted`].
pub(crate) fn drive_exec(hosted_exec: &HostedExec, budget: u32) -> Driven {
    let sched = hosted_exec.rt_handle.inner.clone();
    // `None` means the core is checked out (a drive or `block_on` on the
    // stack, or — under JSPI — a suspended one): report `Busy` so the
    // caller leaves the armed timer and keepalive alone. Conflating this
    // with `Idle` would disarm a wake the core holder still depends on.
    let core = match hosted_exec.exec.core.take() {
        Some(core) => core,
        None => return Driven::Busy,
    };
    let handle: Arc<Handle> = sched.as_current_thread().clone();
    let cx = scheduler::Context::CurrentThread(Context {
        handle,
        core: RefCell::new(Some(core)),
        defer: Defer::new(),
    });

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
    let _restore = RestoreCore {
        exec: &hosted_exec.exec,
        cx: &cx,
    };

    // Each cooperative drive is one host-loop cycle: it resumed from a host
    // wake (unpark) and ends by yielding control back (park). Record the same
    // busy-time/poll metrics the native scheduler does around its park loop.
    if let Some(core) = cx.expect_current_thread().core.borrow_mut().as_mut() {
        core.metrics.unparked();
        core.metrics.start_processing_scheduled_tasks();
    }

    // Reuse `drive_loop` (shared with `block_on`) with a never-ready root, so
    // scheduled roots and timers drain through the exact same path.
    let mut budget = PollBudget::bounded(budget);
    let pending = std::future::pending::<()>();
    crate::pin!(pending);
    let _: Outcome<()> = context::enter_runtime(&sched, false, |_| {
        context::set_scheduler(&cx, || drive_loop(pending, &cx, &mut budget))
    });

    {
        let inner = cx.expect_current_thread();
        if let Some(core) = inner.core.borrow_mut().as_mut() {
            core.metrics.end_processing_scheduled_tasks();
            core.metrics.about_to_park();
            core.submit_metrics(&inner.handle);
        }
    }

    let inner = cx.expect_current_thread();
    let handle = &inner.handle;

    // Budget spent with work still ready: yield to the host and re-drive.
    // A budget that ran dry on exactly the last task has nothing left to
    // yield for, so check the queues rather than the counter alone.
    if budget.exhausted {
        let work_remains = inner
            .core
            .borrow()
            .as_ref()
            .is_some_and(|core| !core.tasks.is_empty())
            || handle.shared.inject.len() > 0
            || handle.shared.woken.load(Ordering::Acquire);
        if work_remains {
            return Driven::Yield;
        }
    }

    match next_timer_ms(handle) {
        Some(ms) => Driven::Timer(ms),
        None => Driven::Idle,
    }
}

#[cfg(tokio_unstable)]
impl HostedEventLoop {
    /// Tear down the scheduler's tasks (mirrors `CurrentThread`'s `Drop`).
    pub(crate) fn shutdown(&mut self, handle: &scheduler::Handle) {
        self.inner.exec.shutdown(handle);
    }
}

/// What should wake the cooperatively-driven event-loop runtime next, returned
/// by [`drive_exec`]. The host loop acts on it: arm a timer, yield
/// immediately, or rest until an external wake.
pub(crate) enum Driven {
    /// Nothing ready and no timer: rest until an external wake (I/O callback).
    Idle,
    /// Pending work bounded by a timer; re-drive within `ms` at the latest.
    Timer(f64),
    /// Budget spent with work still ready; re-drive immediately (`setTimeout(0)`).
    Yield,
    /// The core is checked out by another drive on (or, under JSPI, suspended
    /// on) this thread; do nothing — the holder re-arms on exit.
    Busy,
}

/// Bounds task polls per drive so a busy runtime yields a host turn (parks 0 ms
/// then re-drives) instead of starving other agents. Only a non-JSPI `block_on`
/// is unbounded: it can't yield, so must reach a fixed point or panic.
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

    /// Account for one poll; returns `false` (and sets `exhausted`) once spent.
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
pub(crate) enum Outcome<T> {
    /// Root resolved.
    Completed(T),
    /// Root pending with a timer registered (resolves only via the event loop).
    WaitTimeout,
    /// Root pending, no timer; only an external waker can advance it.
    Suspend,
}

/// Check out `Core`, set the scheduler context, run the drive loop, return
/// `Core`. Must be called inside `block_on`'s `enter_runtime`.
///
/// With `-sJSPI`, a would-suspend fixed point parks the whole stack instead of
/// returning: the core is checked back in (so reentrant host callbacks, incl. a
/// nested `block_on`, can drive), the stack suspends until a wake or timer, then
/// re-acquires and re-enters. Without `-sJSPI` the outcome returns and `block_on`
/// panics.
///
/// # Safety
/// `future` must remain valid for the call.
pub(crate) unsafe fn pump<F: Future>(
    exec: &CurrentThread,
    handle: &Arc<Handle>,
    mut future: Pin<&mut F>,
) -> Outcome<F::Output> {
    let core = match exec.core.take() {
        Some(c) => c,
        None => return Outcome::Suspend,
    };
    let cx = scheduler::Context::CurrentThread(Context {
        handle: handle.clone(),
        core: RefCell::new(Some(core)),
        defer: Defer::new(),
    });

    // Return the core to the scheduler on the way out, even on panic, so the
    // runtime stays tear-down-able (mirrors native `CoreGuard`).
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

    // Mark a drive on the stack so an external wake (any hosted runtime's)
    // latches a pick-up instead of driving inline (which would re-enter
    // `enter_runtime` and panic). Releasing the guard — including on unwind —
    // delivers every pick-up latched while this `block_on` held the thread.
    let _drive = crate::runtime::hosted::enter_drive();

    let jspi = crate::runtime::hosted::jspi_linked();

    loop {
        // Bracket each drive with the metrics native records around its park loop.
        if let Some(core) = cx.expect_current_thread().core.borrow_mut().as_mut() {
            core.metrics.start_processing_scheduled_tasks();
        }
        // JSPI: bounded budget so a self-rewaking task yields a host turn rather
        // than starving the thread. Non-JSPI: unbounded (no host loop to yield to).
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
                // Ready work queued: yield a 0 ms host turn then re-drive.
                // Otherwise a genuine cliff: park until a wake or the next timer.
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

/// Milliseconds until the next armed timer deadline, if any.
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

/// Bound on a cliff park: ms until the next timer, or `-1.0` for wake-only.
fn cliff_timeout_ms(handle: &Arc<Handle>) -> f64 {
    next_timer_ms(handle).unwrap_or(-1.0)
}

/// Park the drive at its cliff via JSPI: check the core into the scheduler's
/// slot, suspend for `timeout_ms` (`-1.0` = until a wake, `0.0` = a bare host
/// turn), then re-acquire it. The native `park_internal` analogue.
fn park_at_cliff(exec: &CurrentThread, cx: &scheduler::Context, timeout_ms: f64) {
    let inner = cx.expect_current_thread();
    let handle = &inner.handle;

    {
        let mut borrow = inner.core.borrow_mut();
        if let Some(core) = borrow.as_mut() {
            core.metrics.about_to_park();
            core.submit_metrics(handle);
        }
        // Check into the slot so reentrant host callbacks can drive while parked.
        if let Some(core) = borrow.take() {
            exec.core.set(core);
        }
    }

    crate::runtime::hosted::park_on_host(timeout_ms);

    // Re-acquire. The slot is populated unless a sibling stack resumed first this
    // batch and hasn't re-parked yet — re-park briefly rather than spin.
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
/// Distinguishes a spin needing a host turn from a genuine cliff.
fn has_ready_work(inner: &Context, handle: &Arc<Handle>) -> bool {
    handle.shared.woken.load(Ordering::Acquire)
        || handle.shared.inject.len() > 0
        || inner
            .core
            .borrow()
            .as_ref()
            .is_some_and(|core| !core.tasks.is_empty())
}

/// Drive until the root resolves, no progress is possible, or `budget` is spent.
/// On exhaustion it flushes deferred wakers to the run queue.
fn drive_loop<F: Future>(
    mut future: Pin<&mut F>,
    cx: &scheduler::Context,
    budget: &mut PollBudget,
) -> Outcome<F::Output> {
    let inner = cx.expect_current_thread();
    let handle = &inner.handle;

    loop {
        if budget.exhausted {
            // Flush deferred wakers so they survive this drive's `Defer` drop.
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
            // re-deferred a `yield_now`, none of which advance without a host turn.
            // Only genuine work below keeps the spin going; else fall to the cliff.
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
            // `process` may fire the root waker (no task queued); count as progress.
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
            // The cliff: no task or timer advanced. Auto-advance (test-clock only)
            // is a nominal jump to the next deadline, valid only when quiescent —
            // pending work must re-drive first, else the jump stacks onto
            // controlled time. Real clocks advance by measured host time on park.
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
/// a synchronous drive can fire it; returns `true` if time advanced.
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

                // Bracket the poll with the metrics native `run_task` records.
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
