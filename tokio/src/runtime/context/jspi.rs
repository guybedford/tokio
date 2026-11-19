//! Leaving the runtime across a Tokio-issued JSPI suspension.
//!
//! While `block_on` is suspended, the host loop runs other code on the
//! thread, and that code shares `CONTEXT`. JSPI keeps the suspended
//! activation's stack, and its suspending import returns into the same
//! activation before any other code of it runs. So the activation swaps the
//! context itself around the import: its own context out, the one the thread
//! had before its `block_on` back in, and the reverse on resume.
//!
//! The pre-entry context is recorded by value as `block_on` starts, never as
//! a pointer to a guard: address-taken locals live on the shared
//! linear-memory shadow stack, which other activations reuse while this one
//! is suspended.

use super::{Context, EnterRuntime, CONTEXT};

use crate::runtime::{scheduler, task::Id};
use crate::task::coop;
use crate::task::LocalSnapshot;
use crate::util::rand::FastRand;

use std::io::Write;
use std::sync::Arc;

/// Every field that `enter_runtime`, `set_current`, `set_scheduler` and task
/// polling write. The thread id belongs to the thread, shared by every
/// activation.
pub(crate) struct Snapshot {
    runtime: EnterRuntime,
    rng: Option<FastRand>,
    handle: Option<scheduler::Handle>,
    depth: usize,
    scheduler: *const scheduler::Context,
    task_id: Option<Id>,
    budget: coop::Budget,
    local: LocalSnapshot,
    outer: Option<Box<Snapshot>>,
}

impl Context {
    /// Copies the context, except `outer`, which the caller saves.
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            runtime: self.runtime.get(),
            rng: self.rng.get(),
            handle: self.current.handle.borrow().clone(),
            depth: self.current.depth.get(),
            scheduler: self.scheduler.inner.get(),
            task_id: self.current_task_id.get(),
            budget: self.budget.get(),
            local: LocalSnapshot::copy(),
            outer: None,
        }
    }

    fn take(&self) -> Snapshot {
        Snapshot {
            runtime: self.runtime.get(),
            rng: self.rng.get(),
            handle: self.current.handle.borrow_mut().take(),
            depth: self.current.depth.get(),
            scheduler: self.scheduler.inner.get(),
            task_id: self.current_task_id.get(),
            budget: self.budget.get(),
            local: LocalSnapshot::take(),
            outer: self.outer.take(),
        }
    }

    fn restore(&self, s: Snapshot) {
        self.runtime.set(s.runtime);
        self.rng.set(s.rng);
        *self.current.handle.borrow_mut() = s.handle;
        self.current.depth.set(s.depth);
        self.scheduler.inner.set(s.scheduler);
        self.current_task_id.set(s.task_id);
        self.budget.set(s.budget);
        s.local.restore();
        self.outer.set(s.outer);
    }
}

/// What host code must leave balanced across the suspension: whatever it
/// entered, it exits before the suspended activation resumes.
#[derive(PartialEq)]
struct Fingerprint {
    entered: bool,
    depth: usize,
    handle: Option<*const ()>,
}

impl Snapshot {
    fn fingerprint(&self) -> Fingerprint {
        Fingerprint {
            entered: self.runtime.is_entered(),
            depth: self.depth,
            handle: self.handle.as_ref().map(|h| match h {
                scheduler::Handle::CurrentThread(h) => Arc::as_ptr(h).cast(),
                #[cfg(feature = "rt-multi-thread")]
                scheduler::Handle::MultiThread(h) => Arc::as_ptr(h).cast(),
            }),
        }
    }
}

/// The previous `outer`, put back when `block_on` returns.
pub(crate) struct OuterGuard(Option<Box<Snapshot>>);

/// Records the thread's context as `block_on`'s pre-entry context, for its
/// suspensions to put back.
pub(crate) fn record_outer() -> OuterGuard {
    CONTEXT.with(|c| OuterGuard(c.outer.replace(Some(Box::new(c.snapshot())))))
}

impl Drop for OuterGuard {
    fn drop(&mut self) {
        CONTEXT.with(|c| c.outer.set(self.0.take()));
    }
}

pub(crate) struct LeaveGuard(Leave);

enum Leave {
    /// The suspended activation's own context, put back when it resumes.
    Runtime {
        own: Option<Snapshot>,
        outer: Fingerprint,
    },
    /// Entered without a recorded pre-entry context, so the runtime stays
    /// entered and only the scheduler context is cleared: host wakes during
    /// the suspension must take the remote path to unpark.
    Scheduler(*const scheduler::Context),
}

/// Puts back `block_on`'s pre-entry context for a suspension, if a runtime
/// is entered. Not entered (a thread parker outside any runtime), there is
/// nothing to leave.
pub(crate) fn leave_runtime_for_suspend() -> Option<LeaveGuard> {
    CONTEXT.with(|c| {
        if !c.runtime.get().is_entered() {
            return None;
        }
        let Some(outer) = c.outer.take() else {
            return Some(LeaveGuard(Leave::Scheduler(
                c.scheduler.inner.replace(std::ptr::null()),
            )));
        };
        let own = c.take();
        let fingerprint = outer.fingerprint();
        c.restore(*outer);
        Some(LeaveGuard(Leave::Runtime {
            own: Some(own),
            outer: fingerprint,
        }))
    })
}

impl Drop for LeaveGuard {
    fn drop(&mut self) {
        CONTEXT.with(|c| match &mut self.0 {
            Leave::Scheduler(scheduler) => c.scheduler.inner.set(*scheduler),
            Leave::Runtime { own, outer: expected } => {
                let outer = c.take();
                if outer.fingerprint() != *expected {
                    // A suspension Tokio did not issue left a runtime entered
                    // over the context being resumed. May run during an
                    // unwind, so abort rather than panic.
                    let _ = std::io::stderr().write_all(
                        b"a JSPI suspension not issued by Tokio left a runtime \
                          entered on this thread\n",
                    );
                    std::process::abort();
                }
                let mut own = own.take().unwrap();
                own.outer = Some(Box::new(outer));
                c.restore(own);
            }
        });
    }
}
