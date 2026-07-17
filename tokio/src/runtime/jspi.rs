//! Minimal JSPI primitives for `wasm32-unknown-emscripten`.
//!
//! Tokio's suspension model is claimed per activation: `#[tokio::test]`
//! (and the JSPI export conventions) mark their promising activation
//! suspendable with a [`SuspendGuard`], and [`park`] — the one suspending
//! import the runtime issues — suspends the activation until [`signal`]
//! resolves it or the timeout elapses. Waits are keyed by their driver
//! stack's identity — any number of activations may be suspended
//! concurrently (suspensions don't block fresh continuations), each
//! resolved by its own key: the host timer, the epoll readiness callback,
//! or an external unpark.

use std::cell::Cell;
use std::time::Duration;

thread_local! {
    static SUSPENDABLE: Cell<bool> = const { Cell::new(false) };
}

/// Marks the `#[tokio::test]` promising activation as suspendable for the
/// body's extent; `Drop` clears the flag across panic unwinds. Internal to
/// the test expansion, not a user convention.
#[derive(Debug)]
pub struct SuspendGuard(());

impl SuspendGuard {
    /// Marks the current activation suspendable until drop.
    #[allow(clippy::new_without_default)]
    pub fn new() -> SuspendGuard {
        SUSPENDABLE.set(true);
        SuspendGuard(())
    }
}

impl Drop for SuspendGuard {
    fn drop(&mut self) {
        SUSPENDABLE.set(false);
    }
}

/// Whether the park leaf may suspend: a [`SuspendGuard`] is live.
pub(crate) fn can_suspend() -> bool {
    SUSPENDABLE.get()
}

// Emscripten EM_JS convention: `__em_js__<name>` data exports carry JS
// bodies into the objects, and `__asyncjs__` names get
// `WebAssembly.Suspending` treatment under `-sJSPI`. The static must be
// referenced from linked code (`anchor`) so its archive member is pulled
// in.

// The suspending wait: parks the calling activation until the wake slot is
// signalled or `ms` elapses (negative = no timer). Unit return, never
// rejects, `Asyncify.handleAsync` for runtime keepalive across the
// suspension.
const TOKIO_JSPI_WAIT: &str = "(id, ms)<::>{ return Asyncify.handleAsync(() => new Promise((resolve) => { const w = globalThis.__tokioDriverWakes ??= new Map(); const done = () => { w.delete(id); if (t !== null) clearTimeout(t); resolve(); }; const t = ms >= 0 ? setTimeout(done, ms) : null; w.set(id, done); })); }";

// Resolve the pending wait, if one is suspended; reports whether it did.
const TOKIO_JSPI_SIGNAL: &str = "(id)<::>{ const wake = globalThis.__tokioDriverWakes?.get(id); if (wake) { wake(); return 1; } return 0; }";

const fn em_js<const N: usize>(s: &str) -> [u8; N] {
    // NUL-terminated: N == s.len() + 1
    let mut a = [0u8; N];
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        a[i] = b[i];
        i += 1;
    }
    a
}

#[allow(non_upper_case_globals)]
#[no_mangle]
#[used]
static __em_js____asyncjs__tokio_jspi_wait: [u8; TOKIO_JSPI_WAIT.len() + 1] =
    em_js(TOKIO_JSPI_WAIT);

#[allow(non_upper_case_globals)]
#[no_mangle]
#[used]
static __em_js__tokio_jspi_signal: [u8; TOKIO_JSPI_SIGNAL.len() + 1] = em_js(TOKIO_JSPI_SIGNAL);

unsafe extern "C" {
    /// Reports the `ASYNCIFY` build mode: 0 = none, 1 = `asyncify`, 2 = JSPI.
    safe fn emscripten_has_asyncify() -> i32;
}

#[link(wasm_import_module = "env")]
unsafe extern "C-unwind" {
    #[link_name = "__asyncjs__tokio_jspi_wait"]
    safe fn tokio_jspi_wait_import(id: f64, ms: f64);
    #[link_name = "tokio_jspi_signal"]
    safe fn tokio_jspi_signal_import(id: f64) -> i32;
}

#[inline(never)]
fn anchor() {
    std::hint::black_box((
        __em_js____asyncjs__tokio_jspi_wait.as_ptr(),
        __em_js__tokio_jspi_signal.as_ptr(),
    ));
}

/// Whether JSPI suspension is available: linked with `-sJSPI`.
pub fn jspi_enabled() -> bool {
    emscripten_has_asyncify() == 2
}

/// Suspend the calling activation until [`signal`]​`(id)` or `timeout`
/// (`None` = until a signal). `id` is the waiter's stable identity (the
/// driver-stack allocation's address): any number of activations may be
/// suspended concurrently, each on its own key, resumed in any order.
/// (`id` is a wasm32 address: exact in f64.)
///
/// Across the suspension the runtime-entered flag is swapped out: the
/// activation is off the stack, so the thread is not inside a runtime, and
/// host callbacks may drive (hosted) runtimes meanwhile. Restored on resume,
/// on unwind too.
pub(crate) fn park(id: usize, timeout: Option<Duration>) {
    anchor();
    struct Restore(crate::runtime::context::EnterRuntime);
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::runtime::context::jspi_restore_runtime_after_park(self.0);
        }
    }
    let _restore = Restore(crate::runtime::context::jspi_exit_runtime_for_park());
    let ms = timeout.map_or(-1.0, |d| d.as_secs_f64() * 1000.0);
    tokio_jspi_wait_import(id as f64, ms);
}

/// Resolve the suspended [`park`] keyed `id`, if any; `true` if it woke.
pub(crate) fn signal(id: usize) -> bool {
    anchor();
    tokio_jspi_signal_import(id as f64) != 0
}
