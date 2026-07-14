use crate::runtime::local_runtime::LocalRuntimeScheduler;
use crate::runtime::{Handle, LocalRuntime};
use crate::task::JoinError;
use crate::util::trace::SpawnMeta;

use std::future::Future;

/// An emscripten hosted event-loop runtime: a [`LocalRuntime`] driven
/// cooperatively by the host JS event loop instead of by parking a thread, so
/// it never blocks the host.
///
/// Built with [`Builder::build_hosted_event_loop_runtime`]. Submit roots with
/// [`schedule`](Self::schedule) and pump with [`drive`](Self::drive); after
/// the first drive the host's own wakes (timer ticks, socket readiness
/// callbacks) re-drive it, so a one-shot `schedule` + `drive` is
/// self-sustaining. Any number of hosted runtimes may coexist on the thread,
/// each driven by its own host turns.
///
/// Everything else a [`LocalRuntime`] offers is available through
/// [`local`](Self::local).
///
/// [`Builder::build_hosted_event_loop_runtime`]: crate::runtime::Builder::build_hosted_event_loop_runtime
#[derive(Debug)]
pub struct HostedRuntime {
    inner: LocalRuntime,
}

impl HostedRuntime {
    /// Wraps a hosted-scheduler `LocalRuntime` (builder only): the wrapper's
    /// existence is the proof that the scheduler is `HostedEventLoop`.
    pub(crate) fn new(inner: LocalRuntime) -> HostedRuntime {
        debug_assert!(matches!(
            inner.scheduler,
            LocalRuntimeScheduler::HostedEventLoop(_)
        ));
        HostedRuntime { inner }
    }

    fn hosted(&self) -> &std::sync::Arc<crate::runtime::hosted::HostedState> {
        match &self.inner.scheduler {
            LocalRuntimeScheduler::HostedEventLoop(ev) => ev.hosted(),
            // Unreachable by construction: `new` is builder-only.
            LocalRuntimeScheduler::CurrentThread(_) => {
                unreachable!("HostedRuntime wraps the hosted event-loop scheduler")
            }
        }
    }

    /// Enqueues `future` as a root on this runtime, delivering its outcome to
    /// `on_complete` once it resolves. **Never runs it synchronously**: the
    /// root is queued only — call [`drive`](Self::drive) to run it before
    /// returning to the host (a `setTimeout(0)` pick-up is also armed, so the
    /// work is not lost if the caller doesn't), and `on_complete` is never
    /// invoked before `schedule` returns.
    ///
    /// Any number of roots may be in flight, and the future need not be
    /// `Send` (single host thread). A panic in it is caught and delivered as
    /// `Err(JoinError)` rather than unwinding the driver, so embedders (e.g.
    /// a `Promise`-returning JS export bridge) can map `Ok`/`Err` to
    /// resolve/reject. The in-flight roots hold the emscripten runtime alive
    /// until they resolve.
    pub fn schedule<F, C>(&self, future: F, on_complete: C)
    where
        F: Future + 'static,
        F::Output: 'static,
        C: FnOnce(Result<F::Output, JoinError>) + 'static,
    {
        let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&future));
        // SAFETY: the runtime is local to this single thread (`local_tid` set
        // to it), so spawning non-`Send` tasks and driving them here is sound.
        let join = unsafe { self.inner.handle.spawn_local_named(future, meta) };
        // A second task awaits the root's `JoinHandle`, turning a root panic
        // into `Err(JoinError)` here rather than an unwind of the driver.
        let completer = async move {
            on_complete(join.await);
        };
        let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&completer));
        // Detached; delivers `on_complete` when the root resolves.
        unsafe {
            drop(self.inner.handle.spawn_local_named(completer, meta));
        }
        self.hosted().keepalive_push();
    }

    /// Drive this runtime to a quiescent fixed point: run ready tasks and
    /// fired timers, harvest I/O readiness, then arm the next host wake (a
    /// `setTimeout` for the soonest timer) or, if idle, let the instance
    /// rest. Returns `false` (a no-op) while a drive is already on the stack
    /// — that drive's fixed point observes the wake, so drives never nest;
    /// `true` if it ran one.
    pub fn drive(&self) -> bool {
        self.hosted().drive()
    }

    /// Returns a handle to this runtime, for spawning and runtime context.
    pub fn handle(&self) -> &Handle {
        self.inner.handle()
    }

    /// Runs a future to completion on this runtime, parking the calling stack
    /// on the host event loop (JSPI) when it would otherwise block. See
    /// [`LocalRuntime::block_on`].
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }

    /// The underlying [`LocalRuntime`], for everything not specific to the
    /// hosted event loop (entering, metrics, shutdown, ...).
    pub fn local(&self) -> &LocalRuntime {
        &self.inner
    }

    /// Unwraps into the underlying [`LocalRuntime`], giving up the
    /// hosted-specific surface (the host callbacks stay armed for the
    /// runtime's lifetime either way).
    pub fn into_local(self) -> LocalRuntime {
        self.inner
    }
}
