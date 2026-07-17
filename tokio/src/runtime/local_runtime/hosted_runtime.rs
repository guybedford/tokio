use crate::runtime::hosted::HostedState;
use crate::runtime::{Handle, LocalRuntime};
use crate::task::JoinError;
use crate::util::trace::SpawnMeta;

use std::future::Future;
use std::sync::Arc;

/// An emscripten hosted event-loop runtime: a [`LocalRuntime`] driven
/// cooperatively by the host JS event loop instead of by parking a thread,
/// so it never blocks the host.
///
/// Built with [`Builder::build_hosted_event_loop_runtime`]. Submit roots
/// with [`schedule`](Self::schedule); each drive runs to a quiescent fixed
/// point and arms the next host wake, so scheduled work is self-sustaining —
/// timer ticks and socket readiness re-enter the drive from the host loop.
/// Any number of hosted runtimes may coexist on the thread.
///
/// Dropping the `HostedRuntime` drops the runtime with native semantics:
/// in-flight roots are dropped, and armed host callbacks resolve to nothing.
///
/// [`Builder::build_hosted_event_loop_runtime`]: crate::runtime::Builder::build_hosted_event_loop_runtime
#[derive(Debug)]
pub struct HostedRuntime {
    state: Arc<HostedState>,
}

impl HostedRuntime {
    pub(crate) fn new(state: Arc<HostedState>) -> HostedRuntime {
        HostedRuntime { state }
    }

    /// Enqueues `future` as a root on this runtime, delivering its outcome
    /// to `on_complete` once it resolves. **Never runs it synchronously**:
    /// the root is queued and a 0 ms drive is armed, so `on_complete` is
    /// never invoked before `schedule` returns (call [`drive`](Self::drive)
    /// to run it before returning to the host).
    ///
    /// Any number of roots may be in flight, and the future need not be
    /// `Send` (single host thread). A panic in it is caught and delivered as
    /// `Err(JoinError)` rather than unwinding the driver, so embedders (e.g.
    /// a `Promise`-returning JS export bridge) can map `Ok`/`Err` to
    /// resolve/reject. In-flight roots hold the emscripten runtime alive
    /// until they resolve.
    pub fn schedule<F, C>(&self, future: F, on_complete: C)
    where
        F: Future + 'static,
        F::Output: 'static,
        C: FnOnce(Result<F::Output, JoinError>) + 'static,
    {
        let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&future));
        let (_, handle) = self.state.runtime().parts();
        // SAFETY: the runtime is local to this single thread (`local_tid`
        // set to it), so spawning non-`Send` tasks and driving them here is
        // sound.
        let join = unsafe { handle.spawn_local_named(future, meta) };
        // A second task awaits the root's `JoinHandle`, turning a root panic
        // into `Err(JoinError)` here rather than an unwind of the driver.
        self.state.keepalive_push();
        let state = self.state.clone();
        let completer = async move {
            on_complete(join.await);
            state.keepalive_pop();
        };
        let meta = SpawnMeta::new_unnamed(std::mem::size_of_val(&completer));
        // Detached; delivers `on_complete` when the root resolves.
        unsafe {
            drop(handle.spawn_local_named(completer, meta));
        }
        self.state.arm_drive();
    }

    /// Drive this runtime to a quiescent fixed point now: run ready tasks
    /// and fired timers, harvest I/O readiness, then arm the next host wake.
    /// Optional — scheduled work is driven by the host loop either way.
    pub fn drive(&self) {
        self.state.drive();
    }

    /// Returns a handle to this runtime, for spawning and runtime context.
    pub fn handle(&self) -> &Handle {
        self.state.runtime().handle()
    }

    /// The underlying [`LocalRuntime`], for everything not specific to the
    /// hosted event loop. `block_on` is rejected on a hosted runtime (like a
    /// nested runtime): its wait point is the host loop, so no stack can
    /// hold the result — use [`schedule`](Self::schedule).
    pub fn local(&self) -> &LocalRuntime {
        self.state.runtime()
    }
}
