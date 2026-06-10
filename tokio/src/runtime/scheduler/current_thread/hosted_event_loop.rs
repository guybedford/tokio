//! The hosted event-loop scheduler kernel: the `current_thread`-coupled half
//! of the event-loop runtime, kept here (a `current_thread` submodule) for the
//! private access it needs to the scheduler `Core`/`Context`. The host-loop glue
//! (timer arming, keepalive, `schedule`, the public `drive`) is exposed via
//! `crate::emscripten::event_loop`.
//!
//! Two entry points share one fixed-point [`drive_loop`]:
//! * [`block_on`]: drives one root to completion synchronously; a future that
//!   would have to suspend panics (the host can't block).
//! * [`HostedEventLoop::drive`]: cooperatively pumps the persistent event-loop runtime
//!   to a quiescent fixed point, bounded by a poll budget, never blocking.

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

/// The event-loop runtime's scheduler: a `current_thread` scheduler driven
/// cooperatively from the host event loop rather than by `block_on`. The
/// [`LocalRuntimeScheduler::HostedEventLoop`](crate::runtime::LocalRuntime) variant.
#[derive(Debug)]
pub(crate) struct HostedEventLoop {
    inner: CurrentThread,
}

impl HostedEventLoop {
    pub(crate) fn new(inner: CurrentThread) -> HostedEventLoop {
        HostedEventLoop { inner }
    }

    /// Synchronously drive `future` to completion (panicking if it would
    /// suspend), delegating to the inner `current_thread` scheduler — the same
    /// drive-or-panic `Runtime::block_on` gets. Distinct from [`drive`](Self::drive),
    /// which pumps cooperatively and never panics.
    #[track_caller]
    pub(crate) fn block_on<F: Future>(&self, handle: &scheduler::Handle, future: F) -> F::Output {
        self.inner.block_on(handle, future)
    }

    /// Drive to a quiescent fixed point without blocking, bounded by `budget`
    /// polls — the cooperative counterpart of [`block_on`]. Backs the host
    /// pick-up in [`crate::emscripten::event_loop`].
    pub(crate) fn drive(&self, rt_handle: &crate::runtime::Handle, budget: u32) -> Driven {
        let sched = rt_handle.inner.clone();
        // `None` means the core is checked out (a drive or `block_on` on the
        // stack, or — under JSPI — a suspended one): report `Busy` so the
        // caller leaves the armed timer and keepalive alone. Conflating this
        // with `Idle` would disarm a wake the core holder still depends on.
        let core = match self.inner.core.take() {
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
            exec: &self.inner,
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

        // Budget spent with work still ready: yield to the host and re-drive.
        if budget.exhausted {
            return Driven::Yield;
        }

        let inner = cx.expect_current_thread();
        let handle = &inner.handle;
        let clock = &handle.driver.clock;
        match handle.driver.time.as_ref().and_then(|time| {
            let deadline = time.next_expiration_tick()?;
            let now = time.time_source().now(clock);
            Some(deadline.saturating_sub(now) as f64)
        }) {
            Some(ms) => Driven::Timer(ms),
            None => Driven::Idle,
        }
    }

    /// Tear down the scheduler's tasks (mirrors `CurrentThread`'s `Drop`).
    pub(crate) fn shutdown(&mut self, handle: &scheduler::Handle) {
        self.inner.shutdown(handle);
    }
}

/// What should wake the cooperatively-driven event-loop runtime next, returned
/// by [`HostedEventLoop::drive`]. The host loop acts on it: arm a timer, yield
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

/// Bounds task polls per drive. `block_on` (worker, no host loop to yield to)
/// is unbounded. The event-loop drive shares the host loop, so it's bounded: a
/// self-rewaking task (`loop { yield_now().await }`, a `Notify` ping-pong) would
/// otherwise spin forever and freeze the host; on exhaustion the drive yields
/// and re-arms `setTimeout(0)` to give the host a turn.
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
            "Cannot block_on a future that does not resolve synchronously on \
             single-threaded emscripten; it would require suspending to the host \
             event loop. Use `#[tokio::main]` / `#[tokio::test]` (driven via the \
             event-loop runtime) for futures that await timers or I/O."
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
/// # Safety
/// `future` must remain valid for the call.
unsafe fn pump<F: Future>(
    exec: &CurrentThread,
    sched: &scheduler::Handle,
    future: Pin<&mut F>,
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

    // An event-loop wake arriving during this `block_on` is latched as pending
    // (this fixed point only pumps `exec`'s runtime, not the event-loop one);
    // convert it to a host pick-up on the way out, including on unwind.
    // Declared before the latch guard so it flushes after the latch clears.
    let _flush = crate::emscripten::event_loop::pending_flush();

    // Mark a drive on the stack so an external wake (via the host `drive`)
    // records readiness instead of starting a nested drive (which would re-enter
    // `enter_runtime` and panic).
    let _drive = crate::emscripten::event_loop::enter_drive();

    // Bracket the drive with the busy-time/poll metrics the native scheduler
    // records. `block_on` runs to completion without ceding to a host loop, so
    // there's no park/unpark here.
    if let Some(core) = cx.expect_current_thread().core.borrow_mut().as_mut() {
        core.metrics.start_processing_scheduled_tasks();
    }
    let outcome = context::set_scheduler(&cx, || {
        // On a worker with no host loop to yield to, drive to completion.
        drive_loop(future, &cx, &mut PollBudget::unbounded())
    });
    {
        let inner = cx.expect_current_thread();
        if let Some(core) = inner.core.borrow_mut().as_mut() {
            core.metrics.end_processing_scheduled_tasks();
            core.submit_metrics(&inner.handle);
        }
    }
    outcome
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
    let clock = &handle.driver.clock;
    let time = handle.driver.time.as_ref();

    loop {
        if budget.exhausted {
            // Flush deferred (`yield_now`) wakers into the run queue so they
            // survive this drive's `Defer` being dropped and run on the next.
            inner.defer.wake();
            break;
        }
        let mut progressed = false;

        if handle.reset_woken() {
            progressed = true;
            if let Some(out) = poll_root(future.as_mut(), handle) {
                return Outcome::Completed(out);
            }
        }
        if drain_tasks(handle, &inner.core, budget) {
            progressed = true;
        }
        if let Some(t) = time {
            t.process(clock);
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
            // The progression cliff: no task or timer advanced. Before idling,
            // consult the I/O reactor's non-blocking `poll(2)` (the `epoll`-park
            // analogue) — it surfaces readiness no callback could carry (listener
            // accept, a `block_on`-parked fd). Loop again if it freed any waiter.
            #[cfg(feature = "net")]
            if let Some(io) = handle.driver.io.as_ref() {
                if io.poll_ready() {
                    continue;
                }
            }
            // Idle under a paused test clock: jump to the next timer and
            // re-process instead of waiting on the host (mirrors native
            // `park_thread_timeout`).
            if auto_advance_to_next_timer(handle, clock) {
                continue;
            }
            break;
        }
    }

    if let Some(t) = time {
        if t.next_expiration_tick().is_some() {
            return Outcome::WaitTimeout;
        }
    }
    Outcome::Suspend
}

/// If the paused test clock may auto-advance, jump to the next timer deadline so
/// a synchronous drive can fire it; returns `true` if time advanced. Always
/// `false` without `test-util` or when the clock can't advance / has no timer.
#[cfg(feature = "test-util")]
fn auto_advance_to_next_timer(handle: &Arc<Handle>, clock: &crate::time::Clock) -> bool {
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
fn auto_advance_to_next_timer(_handle: &Arc<Handle>, _clock: &crate::time::Clock) -> bool {
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
        if next.is_some() {
            // Bracket the poll with the same metrics the native `run_task`
            // records (poll count + per-poll timing histogram).
            core.metrics.start_poll();
        }
        drop(borrow);
        match next {
            Some(task) => {
                any = true;
                let task = handle.shared.owned.assert_owner(task);
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
