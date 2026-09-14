//! Minimal JSPI primitives for `wasm32-unknown-emscripten`.
//!
//! [`sleep`] is the runtime's own suspending import, parking the calling
//! activation on a host timer; with `net`, Emscripten's `epoll_wait` suspends
//! as well.
//!
//! Each promising activation is a fiber: a cooperative thread of its own that
//! shares the OS thread's storage. [`jspi_local!`] declares fiber-local
//! storage: built with `--cfg tokio_jspi_hooks` and linked with
//! `-sJSPI_HOOKS` (or `-sREENTRANT_JSPI`), every fiber sees its own value,
//! tracked through the lifecycle hooks of `<emscripten/jspi.h>`, so with the
//! runtime context declared this way each fiber may drive its own runtime,
//! and a park or any other suspension leaves the runtime for the fiber's
//! siblings. Otherwise it is a plain thread-local, and a `block_on` from
//! another activation while one is suspended panics as a nested runtime.

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

/// Suspend the owning activation for `dur` on a host timer.
pub(crate) fn sleep(dur: Duration) {
    let ms = dur.as_secs_f64() * 1000.0;
    // SAFETY: the import takes an `f64` and returns nothing. Under `-sJSPI`
    // it suspends this activation; the caller has checked `jspi_enabled`.
    unsafe { tokio_jspi_sleep_import(ms) }
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
        wait()
    } else if immediate {
        wait()
    } else {
        panic!(
            "cannot block on wasm32-unknown-emscripten: waiting for I/O \
             readiness needs the build to link `-sJSPI`"
        );
    }
}

/// Declares fiber-local storage, with the syntax and `with`/`try_with` API of
/// `thread_local!`. Outside any fiber, or without the hooks, it is the
/// thread-local.
macro_rules! jspi_local {
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = const { $init:expr } $(;)?) => {
        $(#[$attr])*
        $vis static $name: $crate::runtime::context::jspi::LocalKey<$t> = {
            ::std::thread_local!(static ROOT: $t = const { $init });
            $crate::runtime::context::jspi::LocalKey::new(&ROOT, || $init)
        };
    };
}
pub(crate) use jspi_local;

pub(crate) use local::LocalKey;

#[cfg(not(tokio_jspi_hooks))]
mod local {
    use std::thread::AccessError;

    pub(crate) struct LocalKey<T: 'static> {
        root: &'static std::thread::LocalKey<T>,
    }

    impl<T: 'static> LocalKey<T> {
        pub(crate) const fn new(root: &'static std::thread::LocalKey<T>, _init: fn() -> T) -> Self {
            LocalKey { root }
        }

        pub(crate) fn with<R>(&'static self, f: impl FnOnce(&T) -> R) -> R {
            self.root.with(f)
        }

        pub(crate) fn try_with<R>(&'static self, f: impl FnOnce(&T) -> R) -> Result<R, AccessError> {
            self.root.try_with(f)
        }
    }
}

#[cfg(tokio_jspi_hooks)]
mod local {
    use std::cell::{Cell, RefCell};
    use std::ffi::c_void;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::AccessError;

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

    type Slot = Option<(*mut (), unsafe fn(*mut ()))>;

    /// A fiber's storage: one slot per `jspi_local!`, filled on first use.
    /// `None` for a fiber already running when the hooks were registered
    /// (`main`), whose storage is the thread's.
    struct Fiber {
        /// Whoever entered or last resumed this fiber, current again while it
        /// is suspended and after it exits.
        host: *mut Fiber,
        slots: Option<RefCell<Vec<Slot>>>,
    }

    impl Drop for Fiber {
        fn drop(&mut self) {
            if let Some(slots) = &self.slots {
                for (ptr, drop) in slots.borrow_mut().drain(..).flatten() {
                    // SAFETY: allocated by `LocalKey::with` for this slot.
                    unsafe { drop(ptr) }
                }
            }
        }
    }

    thread_local! {
        /// Null outside any fiber.
        static CURRENT: Cell<*mut Fiber> = const { Cell::new(ptr::null_mut()) };
    }

    /// Must not unwind, which `extern "C"` enforces by aborting.
    unsafe extern "C" fn hook(event: u32, token: *mut c_void, _error: i32) -> *mut c_void {
        let fresh = |slots| {
            Box::into_raw(Box::new(Fiber {
                host: CURRENT.with(|c| c.get()),
                slots,
            }))
        };
        // SAFETY: a non-null token is the `Fiber` this hook returned for the
        // fiber at an earlier event, freed only at its `JSPI_EXIT`.
        let fiber = unsafe { (token as *mut Fiber).as_mut() };
        match (event, fiber) {
            (JSPI_ENTER, _) => {
                let fiber = fresh(Some(RefCell::new(Vec::new())));
                CURRENT.with(|c| c.set(fiber));
                fiber as *mut c_void
            }
            (JSPI_SUSPEND, fiber) => {
                let fiber = match fiber {
                    Some(fiber) => fiber,
                    None => unsafe { &mut *fresh(None) },
                };
                CURRENT.with(|c| c.set(fiber.host));
                fiber as *mut Fiber as *mut c_void
            }
            (JSPI_RESUME, Some(fiber)) => {
                fiber.host = CURRENT.with(|c| c.get());
                CURRENT.with(|c| c.set(fiber));
                token
            }
            (JSPI_EXIT, Some(fiber)) => {
                CURRENT.with(|c| c.set(fiber.host));
                drop(unsafe { Box::from_raw(fiber) });
                token
            }
            _ => token,
        }
    }

    // Registered before `main`, itself a fiber, so its `JSPI_ENTER` is seen
    // and the top level keeps the thread's storage; a fiber first seen at a
    // suspension (`main` under a host that skips constructors) adopts the
    // thread's storage instead.
    #[used]
    #[link_section = ".init_array"]
    static CONSTRUCTOR: unsafe extern "C" fn() = register;

    unsafe extern "C" fn register() {
        thread_local!(static REGISTERED: Cell<bool> = const { Cell::new(false) });
        if !REGISTERED.with(|r| r.replace(true)) {
            // SAFETY: registers a per-thread callback with a libc function.
            unsafe { jspi_register(hook, JSPI_ENTER | JSPI_EXIT | JSPI_SUSPEND | JSPI_RESUME) };
        }
    }

    static SLOTS: AtomicUsize = AtomicUsize::new(0);

    pub(crate) struct LocalKey<T: 'static> {
        root: &'static std::thread::LocalKey<T>,
        init: fn() -> T,
        slot: AtomicUsize,
    }

    impl<T: 'static> LocalKey<T> {
        pub(crate) const fn new(root: &'static std::thread::LocalKey<T>, init: fn() -> T) -> Self {
            LocalKey {
                root,
                init,
                slot: AtomicUsize::new(usize::MAX),
            }
        }

        fn slot(&self) -> usize {
            match self.slot.load(Ordering::Relaxed) {
                usize::MAX => {
                    let slot = SLOTS.fetch_add(1, Ordering::Relaxed);
                    // A racing thread's slot is as good as ours.
                    match self.slot.compare_exchange(usize::MAX, slot, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => slot,
                        Err(taken) => taken,
                    }
                }
                slot => slot,
            }
        }

        /// The current fiber's value, or `None` for the thread's.
        fn fiber(&'static self) -> Option<&'static T> {
            // Also keeps the constructor's object linked.
            std::hint::black_box(&CONSTRUCTOR);
            unsafe { register() };
            // SAFETY: `CURRENT` is a live `Fiber`, freed only at its exit,
            // which cannot happen while its code runs.
            let fiber = unsafe { CURRENT.with(|c| c.get()).as_ref()? };
            let slots = fiber.slots.as_ref()?;
            let slot = self.slot();
            let mut slots = slots.borrow_mut();
            if slots.len() <= slot {
                slots.resize(slot + 1, None);
            }
            let (ptr, _) = *slots[slot].get_or_insert_with(|| {
                unsafe fn drop_box<T>(ptr: *mut ()) {
                    drop(unsafe { Box::from_raw(ptr as *mut T) });
                }
                (Box::into_raw(Box::new((self.init)())) as *mut (), drop_box::<T>)
            });
            // SAFETY: the box lives until the fiber's slots drop at exit; a
            // `with` from this fiber is over by then.
            Some(unsafe { &*(ptr as *const T) })
        }

        pub(crate) fn with<R>(&'static self, f: impl FnOnce(&T) -> R) -> R {
            match self.fiber() {
                Some(v) => f(v),
                None => self.root.with(f),
            }
        }

        pub(crate) fn try_with<R>(&'static self, f: impl FnOnce(&T) -> R) -> Result<R, AccessError> {
            match self.fiber() {
                Some(v) => Ok(f(v)),
                None => self.root.try_with(f),
            }
        }
    }
}
