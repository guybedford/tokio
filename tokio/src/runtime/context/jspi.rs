//! Fiber-owned runtime context through Emscripten's JSPI lifecycle hooks.
//!
//! A Tokio-issued park leaves the scheduler context (`runtime::jspi::park`)
//! but keeps the runtime entered, so a sibling `block_on` from another
//! activation during it panics as a nested runtime, and a suspension Tokio
//! does not issue (a suspending import called from task code, such as a
//! blocking name lookup) keeps everything as it was.
//!
//! Built with `--cfg tokio_unstable_jspi_hooks` and linked with
//! `-sJSPI_HOOKS` (or `-sREENTRANT_JSPI`), the fiber lifecycle hooks of
//! `<emscripten/jspi.h>` make the thread's context fiber-owned instead:
//! [`hook`] moves it out at every suspension and back at the resume,
//! whether Tokio issued the suspension or task code did, and a fiber entered
//! from inside a running runtime starts from an empty context. Every
//! suspension is then a leave, and such a fiber is a sibling rather than a
//! nesting. A plain host callback (not a promising export) during a
//! suspension sees the empty context too: it has no current runtime and
//! must hold a `Handle`.
//!
//! `task_local!` values are not fiber-owned: one in scope across a
//! suspension issued from task code is visible to sibling fibers.

use super::{Context, EnterRuntime, CONTEXT};

use crate::runtime::{scheduler, task::Id};
use crate::task::coop;
use crate::task::LocalSnapshot;
use crate::util::rand::FastRand;

use std::cell::Cell;
use std::ffi::c_void;
use std::ptr;

extern "C" {
    /// `<emscripten/jspi.h>`; -1 when linked without `-sJSPI_HOOKS`.
    fn jspi_register(
        hook: unsafe extern "C" fn(u32, *mut c_void, i32) -> *mut c_void,
        mask: u32,
    ) -> i32;
}

const JSPI_ENTER: u32 = 1;
const JSPI_EXIT: u32 = 2;
const JSPI_SUSPEND: u32 = 4;
const JSPI_RESUME: u32 = 8;

/// The thread's Tokio context: what `enter_runtime` and `set_scheduler`
/// write, the poll-scoped task id and budget (a suspension from task code is
/// mid-poll), and the `LocalSet` state. The thread id belongs to the OS
/// thread, shared by every fiber.
struct Snapshot {
    runtime: EnterRuntime,
    rng: Option<FastRand>,
    handle: Option<scheduler::Handle>,
    depth: usize,
    scheduler: *const scheduler::Context,
    task_id: Option<Id>,
    budget: coop::Budget,
    local: LocalSnapshot,
}

impl Snapshot {
    const EMPTY: Snapshot = Snapshot {
        runtime: EnterRuntime::NotEntered,
        rng: None,
        handle: None,
        depth: 0,
        scheduler: ptr::null(),
        task_id: None,
        budget: coop::Budget::unconstrained(),
        local: LocalSnapshot::EMPTY,
    };
}

impl Context {
    /// Moves the context out, leaving the empty state.
    fn take(&self) -> Snapshot {
        let s = Snapshot {
            runtime: self.runtime.get(),
            rng: self.rng.get(),
            handle: self.current.handle.borrow_mut().take(),
            depth: self.current.depth.get(),
            scheduler: self.scheduler.inner.get(),
            task_id: self.current_task_id.get(),
            budget: self.budget.get(),
            local: LocalSnapshot::take(),
        };
        self.restore(Snapshot::EMPTY);
        s
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
    }
}

/// A fiber's own context while it is suspended, and the context of whoever
/// entered or last resumed it while it runs, put back when it leaves.
struct Fiber {
    own: Snapshot,
    host: Snapshot,
}

/// The `jspi.h` hook: `token` is the fiber's [`Fiber`], null until the first
/// event seen for it (main is already running when Tokio registers). Must not
/// unwind, which `extern "C"` enforces by aborting.
unsafe extern "C" fn hook(event: u32, token: *mut c_void, _error: i32) -> *mut c_void {
    let take = || CONTEXT.with(|c| c.take());
    let restore = |s| CONTEXT.with(|c| c.restore(s));
    let fresh = |host| {
        Box::into_raw(Box::new(Fiber {
            own: Snapshot::EMPTY,
            host,
        }))
    };
    // SAFETY: a non-null token is the `Fiber` this hook returned for the
    // fiber at an earlier event, freed only at its `JSPI_EXIT`.
    let fiber = unsafe { (token as *mut Fiber).as_mut() };
    match (event, fiber) {
        (JSPI_ENTER, _) => fresh(take()) as *mut c_void,
        (JSPI_EXIT, Some(fiber)) => {
            // SAFETY: the `Box` leaked by `fresh` for this fiber.
            restore(unsafe { Box::from_raw(fiber) }.host);
            token
        }
        (JSPI_SUSPEND, fiber) => {
            let fiber = match fiber {
                Some(fiber) => fiber,
                // SAFETY: freshly leaked, so live.
                None => unsafe { &mut *fresh(Snapshot::EMPTY) },
            };
            fiber.own = take();
            restore(std::mem::replace(&mut fiber.host, Snapshot::EMPTY));
            fiber as *mut Fiber as *mut c_void
        }
        (JSPI_RESUME, Some(fiber)) => {
            fiber.host = take();
            restore(std::mem::replace(&mut fiber.own, Snapshot::EMPTY));
            token
        }
        _ => token,
    }
}

/// Whether the fiber hooks own the context on this thread, registering them
/// on first use. Called on every runtime entry so a fiber entered from
/// inside a runtime is seen from its start.
pub(crate) fn hooks_active() -> bool {
    thread_local!(static ACTIVE: Cell<Option<bool>> = const { Cell::new(None) });
    ACTIVE.with(|a| match a.get() {
        Some(active) => active,
        None => {
            // SAFETY: registers a per-thread callback with a libc function.
            let active =
                unsafe { jspi_register(hook, JSPI_ENTER | JSPI_EXIT | JSPI_SUSPEND | JSPI_RESUME) }
                    == 0;
            a.set(Some(active));
            active
        }
    })
}
