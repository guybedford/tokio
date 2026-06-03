//! Blocking `block_on` harness for `#[tokio::test]` on emscripten — tests only.
//!
//! A libtest test fn must report synchronously, but the main wasm instance
//! can't block — so [`run_test`] hands an `extern "C" fn()` entry to a fresh
//! Node `worker_threads.Worker` that drives the body (via [`run_test_body`])
//! while the caller parks on `Atomics.wait`. The two wasm instances share only
//! the SAB carrying the status code and (on panic) the rendered message; see
//! `worker.js` for the layout. `#[tokio::main]` uses none of this — it schedules
//! on the event-loop runtime and returns.

use std::cell::RefCell;
use std::future::Future;
use std::panic;
use std::sync::OnceLock;

#[cfg(debug_assertions)]
#[allow(unused_imports)]
use super::ffi::__tokio_emscripten_debugger;
use super::ffi::{
    __tokio_emscripten_block_in_worker, __tokio_emscripten_worker_notify_done,
    __tokio_emscripten_worker_notify_failure, emscripten_force_exit,
};

thread_local! {
    /// Most recent rendered panic on this worker, captured by the panic hook.
    /// The hook fires for *every* panic, including ones later caught by
    /// `catch_unwind` (e.g. a task output panicking on drop), so it only records
    /// here; the drive-tick boundary decides which actually escaped and fail.
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

// Dispatched from the worker JS shim with a wasm-table index from
// `run_entry_in_worker`. Suppressed in `cfg(test)` so tokio's own unit-test
// binary doesn't see a duplicate `#[no_mangle]`.
#[cfg(not(test))]
#[no_mangle]
#[allow(unreachable_pub)]
pub extern "C" fn __tokio_emscripten_worker_invoke(fn_index: i32) {
    // SAFETY: `fn_index` came from an `extern "C" fn()` via `run_entry_in_worker`
    // in the same module; the table layout matches across the wasm instances.
    unsafe {
        // With an inspector attached (`TOKIO_EMSCRIPTEN_INSPECT=1`), pause so
        // "Step Into" lands in the test body. Elided in release.
        #[cfg(debug_assertions)]
        __tokio_emscripten_debugger();
        let f: extern "C" fn() = core::mem::transmute(fn_index as usize);
        f();
    }
}

/// Max captured panic-message bytes; truncated beyond.
const OUTCOME_BUFFER_CAPACITY: usize = 16 * 1024;

/// Mirrors the JS-side SAB layout.
#[repr(C)]
struct OutcomeRaw {
    status: i32,
    message_len: i32,
    message: [u8; OUTCOME_BUFFER_CAPACITY],
}

#[derive(Debug)]
enum WorkerOutcome {
    Ok,
    /// Rendered panic (message + location).
    Panicked(String),
    /// Exited without notifying (JS `process.exit`, or a trap past `onAbort`).
    UnexpectedExit(i32),
}

/// Parent half of the test harness: run a `#[tokio::test]` body on a fresh Node
/// worker, blocking until it finishes, and surface the result to libtest. A
/// worker-side panic is re-raised here at its original location; an unexpected
/// exit panics. `entry` (emitted by the macro) reconstructs the future inside
/// the worker — it can't cross the wasm-instance boundary — and runs it via
/// [`run_test_body`].
pub fn run_test(entry: extern "C" fn()) {
    match run_entry_in_worker(entry) {
        WorkerOutcome::Ok => {}
        // Re-raise with the worker's captured "panicked at …" rendering (the
        // user-code site, not the macro) so libtest records FAILED.
        WorkerOutcome::Panicked(message) => propagate_worker_panic(message),
        WorkerOutcome::UnexpectedExit(code) => {
            panic!("tokio: emscripten worker exited unexpectedly (status {code})")
        }
    }
}

/// Run `entry` in a fresh Node worker, blocking until it reports via the SAB.
fn run_entry_in_worker(entry: extern "C" fn()) -> WorkerOutcome {
    let idx = entry as *const () as usize as i32;

    let mut outcome: Box<OutcomeRaw> = Box::new(OutcomeRaw {
        status: 0,
        message_len: 0,
        message: [0; OUTCOME_BUFFER_CAPACITY],
    });

    let outcome_ptr = (&mut *outcome) as *mut OutcomeRaw as *mut u8;
    let status = unsafe {
        __tokio_emscripten_block_in_worker(
            idx,
            outcome_ptr,
            std::mem::size_of::<OutcomeRaw>() as i32,
        )
    };

    debug_assert_eq!(status, outcome.status, "JS protocol mismatch");

    if outcome.status == 0 {
        WorkerOutcome::Ok
    } else {
        let len = (outcome.message_len as usize).min(OUTCOME_BUFFER_CAPACITY);
        if len > 0 {
            let bytes = &outcome.message[..len];
            let msg = match std::str::from_utf8(bytes) {
                Ok(s) => s.to_owned(),
                Err(_) => String::from_utf8_lossy(bytes).into_owned(),
            };
            WorkerOutcome::Panicked(msg)
        } else {
            WorkerOutcome::UnexpectedExit(outcome.status)
        }
    }
}

/// Re-panic on the test thread, using a private payload type so the hook prints
/// the worker's pre-rendered site verbatim instead of the macro-expansion site.
fn propagate_worker_panic(captured: String) -> ! {
    struct WorkerPanicPayload(String);

    // Install once: the hook reads the message from the payload, so it needs no
    // per-call state. Re-wrapping on every failing test would grow the hook
    // chain unboundedly in the shared parent process.
    static HOOK: OnceLock<()> = OnceLock::new();
    HOOK.get_or_init(|| {
        let prev = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if let Some(payload) = info.payload().downcast_ref::<WorkerPanicPayload>() {
                eprintln!("{}", payload.0);
            } else {
                prev(info);
            }
        }));
    });
    panic::panic_any(WorkerPanicPayload(captured));
}

/// Maps a `#[tokio::test]` body's output to pass/fail (libtest's `Termination`
/// is too opaque to introspect on stable), so `-> Result<…>` / `?` tests run.
pub trait TestOutput {
    /// `Ok` = pass, `Err(rendered)` = fail.
    fn into_test_result(self) -> Result<(), String>;
    /// A success value of `Self`, returned by the libtest fn on the pass path.
    fn test_success() -> Self;
}

impl TestOutput for () {
    fn into_test_result(self) -> Result<(), String> {
        Ok(())
    }
    fn test_success() -> Self {}
}

impl<E: std::fmt::Debug> TestOutput for Result<(), E> {
    fn into_test_result(self) -> Result<(), String> {
        self.map_err(|e| format!("{e:?}"))
    }
    fn test_success() -> Self {
        Ok(())
    }
}

/// Worker half of the test harness: drive `future` on the worker-side runtime,
/// notifying the parent on success, an `Err` return, or a panic. `start_paused`
/// mirrors `#[tokio::test(start_paused = …)]`.
pub fn run_test_body<F>(future: F, start_paused: bool)
where
    F: Future + 'static,
    F::Output: TestOutput,
{
    install_worker_panic_hook();

    #[cfg(feature = "test-util")]
    crate::emscripten::event_loop::configure_start_paused(start_paused);
    #[cfg(not(feature = "test-util"))]
    let _ = start_paused;

    // A panic is caught by the task harness and arrives as `Err(JoinError)` (the
    // hook captured its location); an `Err` return arrives as `Ok(output)`.
    crate::emscripten::event_loop::schedule(future, |outcome| match outcome {
        Ok(output) => match output.into_test_result() {
            Ok(()) => unsafe {
                __tokio_emscripten_worker_notify_done(0);
                super::ffi::emscripten_force_exit(0);
            },
            Err(msg) => report_failure(&format!("Error: {msg}")),
        },
        Err(_join_err) => report_uncaught_worker_panic(),
    });
    crate::emscripten::event_loop::drive();
}

/// Install (once) a hook recording the rendered panic into [`LAST_PANIC`] so the
/// drive-tick boundary can report it *if* it escapes; chains to the previous
/// hook. It must not notify the parent itself — it also fires for panics tokio
/// legitimately catches (`catch_unwind`), which must not fail the test.
fn install_worker_panic_hook() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let prev = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let rendered = render_panic(info);
            LAST_PANIC.with(|c| *c.borrow_mut() = Some(rendered));
            prev(info);
        }));
    });
}

/// Report a panic that escaped a drive tick. It must never unwind across the
/// `extern "C"` boundary into the JS loop, so ship the hook-captured message.
fn report_uncaught_worker_panic() -> ! {
    let rendered = LAST_PANIC
        .with(|c| c.borrow_mut().take())
        .unwrap_or_else(|| "panicked".to_string());
    report_failure(&rendered)
}

/// Ship `message` to the parent with libtest's panic status, then force-exit.
fn report_failure(message: &str) -> ! {
    let bytes = message.as_bytes();
    let len = bytes.len().min(OUTCOME_BUFFER_CAPACITY) as i32;
    // SAFETY: JS reads `len` bytes from `bytes.as_ptr()` before returning;
    // `message` outlives the call.
    unsafe {
        __tokio_emscripten_worker_notify_failure(101, bytes.as_ptr(), len);
        emscripten_force_exit(101)
    }
}

/// Render `PanicInfo` like the default hook (which has no interception point).
#[allow(deprecated)]
fn render_panic(info: &panic::PanicInfo<'_>) -> String {
    use std::fmt::Write as _;

    let payload = info.payload();
    let message: &str = if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "Box<dyn Any>"
    };

    let mut out = String::new();
    if let Some(loc) = info.location() {
        let _ = writeln!(
            out,
            "panicked at {}:{}:{}:",
            loc.file(),
            loc.line(),
            loc.column(),
        );
    } else {
        out.push_str("panicked at <unknown>:\n");
    }
    out.push_str(message);
    out
}
