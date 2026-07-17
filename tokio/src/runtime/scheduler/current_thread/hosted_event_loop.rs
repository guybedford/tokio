//! The hosted drive: pump the `current_thread` scheduler to a quiescent
//! fixed point from host context, without suspending. `block_on` does not
//! come through here — it uses the native drive loop, suspending at the
//! park leaf; this is the continuation lowering's half (see
//! `runtime::hosted`), re-entered inline from host callbacks.
//!
//! A `current_thread` submodule for its private access to `Core`/`Context`.

use crate::loom::sync::Arc;
use crate::runtime::{
    context,
    scheduler::{self, Defer},
};

use super::{Context, CurrentThread, Handle};

use std::cell::RefCell;

/// Drive to a quiescent fixed point: run ready tasks in `event_interval`
/// batches (the native bound), firing due timers and harvesting I/O
/// readiness between batches, until nothing is runnable. Never waits: the
/// caller arms the next host wake from the timer wheel instead.
///
/// Must be called inside `enter_runtime`, on an empty stack (a host
/// callback or a hosted `drive()`).
pub(crate) fn drive_to_fixed_point(exec: &CurrentThread, handle: &Arc<Handle>) {
    let Some(core) = exec.core.take() else {
        // The core is checked into a suspended or shut-down context; that
        // owner's own drive observes any work this wake announced.
        return;
    };
    let cx = scheduler::Context::CurrentThread(Context {
        handle: handle.clone(),
        core: RefCell::new(Some(core)),
        defer: Defer::new(),
    });

    // Return the core on the way out, even on panic, so the runtime stays
    // tear-down-able (mirrors native `CoreGuard`).
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

    context::set_scheduler(&cx, || {
        let inner = cx.expect_current_thread();
        loop {
            run_batch(inner, handle);

            // The driver work a park would do: fire due timers, harvest
            // readiness (queued in the epoll set; deliveries between drives
            // are pure signals), flush deferred wakers.
            #[cfg(feature = "time")]
            if let Some(t) = handle.driver.time.as_ref() {
                t.process(&handle.driver.clock);
            }
            #[cfg(feature = "net")]
            if let Some(io) = handle.driver.io.as_ref() {
                io.drain();
            }
            inner.defer.wake();

            if !has_ready_work(inner, handle) {
                return;
            }
        }
    })
}

/// Run up to `event_interval` scheduled tasks (the native batch bound).
fn run_batch(inner: &Context, handle: &Arc<Handle>) {
    if let Some(core) = inner.core.borrow_mut().as_mut() {
        core.metrics.start_processing_scheduled_tasks();
    }
    for _ in 0..handle.shared.config.event_interval {
        let mut borrow = inner.core.borrow_mut();
        let core = borrow.as_mut().expect("core present");
        if core.unhandled_panic {
            panic!(
                "a spawned task panicked and the runtime is configured to shut down on unhandled panic"
            );
        }
        core.tick();
        let next = core.next_task(handle);
        drop(borrow);

        let task = match next {
            Some(task) => handle.shared.owned.assert_owner(task),
            None => break,
        };

        // Bracket the poll with the metrics native `run_task` records; fresh
        // coop budget per poll.
        inner
            .core
            .borrow_mut()
            .as_mut()
            .expect("core present")
            .metrics
            .start_poll(task.get_scheduled_at().prepare(handle.shared.started_at));
        #[cfg(tokio_unstable)]
        {
            let meta = task.task_meta();
            handle.task_hooks.poll_start_callback(&meta);
            crate::task::coop::budget(|| task.run());
            handle.task_hooks.poll_stop_callback(&meta);
        }
        #[cfg(not(tokio_unstable))]
        crate::task::coop::budget(|| task.run());
        inner
            .core
            .borrow_mut()
            .as_mut()
            .expect("core present")
            .metrics
            .end_poll();
    }
    if let Some(core) = inner.core.borrow_mut().as_mut() {
        core.metrics.end_processing_scheduled_tasks();
        core.submit_metrics(handle);
    }
}

/// Ready work is queued: an injected task or a scheduled task.
fn has_ready_work(inner: &Context, handle: &Arc<Handle>) -> bool {
    handle.shared.inject.len() > 0
        || inner
            .core
            .borrow()
            .as_ref()
            .is_some_and(|core| !core.tasks.is_empty())
}
