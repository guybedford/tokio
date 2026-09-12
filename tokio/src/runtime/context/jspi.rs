//! Minimal JSPI primitives for `wasm32-unknown-emscripten`.
//!
//! [`sleep`] is the one suspending import the runtime issues, parking the
//! calling activation on a host timer.
//!
//! A suspension leaves the runtime, so sibling promising activations (fibers)
//! can each drive a runtime on the thread. Built with `--cfg tokio_jspi_hooks`
//! and linked with `-sJSPI_HOOKS`, the fiber lifecycle hooks of
//! `<emscripten/jspi.h>` make the thread's context fiber-owned: [`hook`] moves
//! it out at every suspension and back at the resume, whether Tokio issued the
//! suspension or task code did, and a fiber entered from inside a running
//! runtime starts from an empty context. Otherwise only Tokio's own parks are
//! a leave, through [`suspended`]: a suspending import called from task code
//! keeps the runtime entered, and a sibling `block_on` during it panics as a
//! nested runtime.
//!
//! `task_local!` values are not fiber-owned: one in scope across a
//! suspension issued from task code is visible to sibling fibers.

use super::{Context, EnterRuntime, CONTEXT};

use crate::runtime::{scheduler, task::Id};
use crate::task::coop;
use crate::task::LocalSnapshot;
use crate::util::rand::FastRand;

use std::ptr;
use std::sync::OnceLock;
use std::time::Duration;

// Emscripten EM_JS convention: the `__em_js__<name>` data export carries the
// JS body, and an `__asyncjs__` name gets `WebAssembly.Suspending` treatment
// under `-sJSPI`. `#[used]` is what exports it: on this target LLVM marks
// `llvm.used` symbols exported (the `EMSCRIPTEN_KEEPALIVE` mechanism), while
// rustc keeps `#[no_mangle]` statics out of the linker's export list.
//
// A zero-duration park is the scheduler's maintenance yield, and wants the
// cheapest resumption that still lets the host loop reach its timer phase.
// `setTimeout(0)` is clamped to a millisecond, while an immediate resumes
// after the current poll phase and schedules its successor into the next
// iteration, which begins by running expired timers. A microtask-flavoured
// queue (`queueMicrotask`, `process.nextTick`) would not do: those drain
// before the loop advances at all, so host timers could never fire and a
// self-waking task would starve them. Hosts without an immediate keep the
// clamped timeout.
#[allow(non_upper_case_globals)]
#[no_mangle]
#[used]
static __em_js____asyncjs__tokio_jspi_sleep: [u8; 169] = *b"(ms)<::>{ return Asyncify.handleAsync(async () => { await new Promise((r) => ms === 0 && typeof setImmediate == 'function' ? setImmediate(r) : setTimeout(r, ms)); }); }\0";

extern "C" {
    /// Reports the `ASYNCIFY` build mode: 0 = none, 1 = legacy `Asyncify`,
    /// 2 = JSPI. Only mode 2 supports Tokio's JSPI import.
    fn emscripten_has_asyncify() -> i32;
}

// Suspending import: parks on a host timeout. Unit return, never rejects,
// `Asyncify.handleAsync` keeps the runtime alive across the suspension.
#[link(wasm_import_module = "env")]
extern "C-unwind" {
    #[link_name = "__asyncjs__tokio_jspi_sleep"]
    fn tokio_jspi_sleep_import(ms: f64);
}

/// Whether JSPI suspension is available: linked with `-sJSPI`.
pub(crate) fn jspi_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    // SAFETY: an Emscripten libc query with no arguments and no side effects.
    *ENABLED.get_or_init(|| unsafe { emscripten_has_asyncify() == 2 })
}

/// The thread's Tokio context: what `enter_runtime` and `set_scheduler`
/// write, the poll-scoped task id and budget (a suspension from task code is
/// mid-poll), and the `LocalSet` state. The thread id belongs to the OS
/// thread, shared by every fiber.
#[derive(Clone)]
pub(super) struct Snapshot {
    runtime: EnterRuntime,
    rng: Option<FastRand>,
    handle: Option<scheduler::Handle>,
    depth: usize,
    scheduler: *const scheduler::Context,
    task_id: Option<Id>,
    budget: coop::Budget,
    entry: Option<Box<Snapshot>>,
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
        entry: None,
        local: LocalSnapshot::EMPTY,
    };
}

impl Context {
    pub(super) fn snapshot(&self) -> Snapshot {
        Snapshot {
            runtime: self.runtime.get(),
            rng: self.rng.get(),
            handle: self.current.handle.borrow().clone(),
            depth: self.current.depth.get(),
            scheduler: self.scheduler.inner.get(),
            task_id: self.current_task_id.get(),
            budget: self.budget.get(),
            entry: self.entry.borrow().clone(),
            local: LocalSnapshot::current(),
        }
    }

    /// Moves the context out, leaving the empty state.
    fn take(&self) -> Snapshot {
        let s = self.snapshot();
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
        *self.entry.borrow_mut() = s.entry;
        s.local.restore();
    }
}

/// Whether the fiber hooks own the context on this thread, registering them
/// on first use. Called on every runtime entry so a fiber entered from
/// inside a runtime is seen from its start.
#[cfg(not(tokio_jspi_hooks))]
pub(super) fn hooks_active() -> bool {
    false
}

#[cfg(tokio_jspi_hooks)]
pub(super) use hooks::hooks_active;

#[cfg(tokio_jspi_hooks)]
mod hooks {
    use super::{Snapshot, CONTEXT};

    use std::cell::Cell;
    use std::ffi::c_void;

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

    /// A fiber's own context while it is suspended, and the context of whoever
    /// entered or last resumed it while it runs, put back when it leaves.
    struct Fiber {
        own: Snapshot,
        host: Snapshot,
    }

    /// The `jspi.h` hook: `token` is the fiber's [`Fiber`], null until the
    /// first event seen for it (main is already running when Tokio
    /// registers). Must not unwind, which `extern "C"` enforces by aborting.
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
                restore(unsafe { Box::from_raw(fiber) }.host);
                token
            }
            (JSPI_SUSPEND, fiber) => {
                let fiber = match fiber {
                    Some(fiber) => fiber,
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

    pub(in crate::runtime::context) fn hooks_active() -> bool {
        thread_local!(static ACTIVE: Cell<Option<bool>> = const { Cell::new(None) });
        ACTIVE.with(|a| match a.get() {
            Some(active) => active,
            None => {
                // SAFETY: registers a per-thread callback with a libc function.
                let active = unsafe {
                    jspi_register(hook, JSPI_ENTER | JSPI_EXIT | JSPI_SUSPEND | JSPI_RESUME)
                } == 0;
                a.set(Some(active));
                active
            }
        })
    }
}

/// Runs `f`, which may suspend this activation, with the runtime left: until
/// `f` returns the thread carries the context the runtime was entered from.
/// Restores on unwind too: a JS exception out of the import (such as
/// `SuspendError` from a non-promising activation) unwinds through the
/// `C-unwind` boundary, and the runtime's own guards then unwind cleanly.
/// Outside a runtime there is nothing to leave, so the context is untouched.
/// With the fiber hooks active the [`hook`] does this for every suspension.
fn suspended<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Snapshot>);

    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(mine) = self.0.take() {
                CONTEXT.with(|c| c.restore(mine));
            }
        }
    }

    let _restore = Restore(if hooks_active() {
        None
    } else {
        CONTEXT.with(|c| {
            let mine = c.take();
            match mine.entry.as_deref().cloned() {
                Some(entry) => {
                    c.restore(entry);
                    Some(mine)
                }
                None => {
                    c.restore(mine);
                    None
                }
            }
        })
    });
    f()
}

/// Suspend the owning activation for `dur` on a host timer.
pub(crate) fn sleep(dur: Duration) {
    let ms = dur.as_secs_f64() * 1000.0;
    // SAFETY: the import takes an `f64` and returns nothing. Under `-sJSPI`
    // it suspends this activation; the caller has checked `jspi_enabled`.
    suspended(|| unsafe { tokio_jspi_sleep_import(ms) })
}

/// The I/O driver's `epoll_wait` of `max_wait` (`None` = no deadline) as a
/// park.
///
/// Under JSPI a non-zero wait suspends on the host loop until readiness or
/// the deadline, but a zero-timeout `epoll_wait` is a synchronous probe, and
/// the host loop is the only producer of readiness, so the scheduler's
/// maintenance park would never let Node deliver socket events. Yield a
/// host turn first, as the zero-duration `ParkThread` park does. Without JSPI
/// `epoll_wait` cannot block at all and returns at once, so a real wait would
/// spin.
#[cfg(feature = "net")]
pub(crate) fn io_wait<R>(max_wait: Option<Duration>, wait: impl FnOnce() -> R) -> R {
    let immediate = max_wait == Some(Duration::ZERO);
    if jspi_enabled() {
        if immediate {
            sleep(Duration::ZERO);
        }
        suspended(wait)
    } else if immediate {
        wait()
    } else {
        panic!(
            "cannot block on wasm32-unknown-emscripten: waiting for I/O \
             readiness needs the build to link `-sJSPI`"
        );
    }
}
