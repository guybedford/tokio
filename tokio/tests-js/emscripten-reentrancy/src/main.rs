//! Two JSPI exports over one ambient hosted runtime — the
//! `#[wasm_bindgen(jspi)]` convention, spelled out: locate the ambient
//! runtime (singular, lazy init), schedule the export's body as a root,
//! suspend the export's activation on its completion. Exports are pure
//! waiters; all driving belongs to the hosted continuation. `run.mjs` calls
//! `ex_fast` while `ex_slow` is suspended: two concurrently suspended
//! activations, resumed out of order.

use std::cell::OnceCell;
use std::time::Duration;

use tokio::runtime::HostedRuntime;

// The suspending wait on a root's completion (JSPI import: see lib.js and
// -sJSPI_IMPORTS), and the plain result channel around it.
extern "C-unwind" {
    fn test_await_completion(id: f64);
    fn test_complete(id: f64, val: i32);
    fn test_take_result(id: f64) -> i32;
}

const TEST_AWAIT_COMPLETION: extern "C-unwind" fn(f64) = {
    extern "C-unwind" fn shim(id: f64) {
        unsafe { test_await_completion(id) }
    }
    shim
};

thread_local! {
    /// The ambient runtime: singular init, shared by every export.
    static AMBIENT: OnceCell<HostedRuntime> = const { OnceCell::new() };
}

fn with_ambient<R>(f: impl FnOnce(&HostedRuntime) -> R) -> R {
    AMBIENT.with(|cell| {
        f(cell.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build_hosted_event_loop_runtime()
                .expect("failed to build the ambient hosted runtime")
        }))
    })
}

/// The `#[wasm_bindgen(jspi)]` convention body: schedule, then suspend on
/// completion. `id` keys this export activation's wait.
fn run_export(id: f64, sleep_ms: u64, result: i32) -> i32 {
    let body = move || {
            with_ambient(|rt| {
                rt.schedule(
                    async move {
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        result
                    },
                    move |out| {
                        let val = out.expect("root panicked");
                        unsafe { test_complete(id, val) };
                    },
                );
            });
            // Suspend this activation until the hosted drive delivers the
            // root's completion. The runtime is driven by the host loop, not
            // by this stack.
            // SAFETY: a genuine suspending import; resolves on completion,
            // returns unit, never rejects, re-enters no wasm.
            unsafe {
                jspi::blocking_call(TEST_AWAIT_COMPLETION, (id,));
            }
            unsafe { test_take_result(id) }
    };
    // SAFETY: this export is promising-entered (-sJSPI_EXPORTS) and the
    // capture-free closure spans the whole activation.
    unsafe { jspi::enter_promising(body) }
}

#[no_mangle]
pub extern "C" fn ex_slow() -> i32 {
    run_export(1.0, 30, 42)
}

#[no_mangle]
pub extern "C" fn ex_fast() -> i32 {
    run_export(2.0, 5, 7)
}

fn main() {}
