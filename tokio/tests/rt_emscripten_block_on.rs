//! `Runtime::block_on` on emscripten drives the scheduler synchronously to
//! a fixed point: it returns the output for futures that resolve without
//! suspending. A future that would have to await a timer or external wake
//! parks the stack via JSPI (see `rt_emscripten_jspi`); on a runtime built
//! with `emscripten_jspi(false)` it panics instead, since without JSPI the
//! host event loop cannot run while wasm is on the stack.

#![cfg(all(
    target_os = "emscripten",
    feature = "rt",
    feature = "time",
    feature = "macros"
))]

use std::time::Duration;

use tokio::runtime::Builder;

fn rt() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn block_on_returns_immediate_value() {
    let out = rt().block_on(async { 1 + 2 });
    assert_eq!(out, 3);
}

#[test]
fn block_on_drives_ready_spawned_tasks() {
    // Spawned tasks that complete synchronously must be driven to
    // completion within the same fixed-point pump.
    let out = rt().block_on(async {
        let a = tokio::spawn(async { 20 });
        let b = tokio::spawn(async { 22 });
        a.await.unwrap() + b.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn block_on_drives_yield_now() {
    // `yield_now` reschedules the root; the fixed-point loop must pick it
    // back up without suspending.
    let out = rt().block_on(async {
        tokio::task::yield_now().await;
        7
    });
    assert_eq!(out, 7);
}

/// A runtime with JSPI parking disabled: `block_on` must resolve synchronously
/// or panic, the only sound semantics on an engine without JSPI. Testable in a
/// JSPI build because the flag is per-runtime.
fn rt_no_jspi() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_all()
        .emscripten_jspi(false)
        .build()
        .unwrap()
}

#[test]
#[should_panic(expected = "Cannot block_on")]
fn no_jspi_block_on_panics_on_timer_wait() {
    // A real timer deadline can only be satisfied by returning to the host
    // event loop, which a non-JSPI `block_on` cannot do.
    rt_no_jspi().block_on(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
    });
}

#[test]
#[should_panic(expected = "Cannot block_on")]
fn no_jspi_block_on_panics_on_external_wait() {
    // A oneshot whose sender never fires (no in-runtime work can complete
    // it) leaves the future pending with no timer — the "suspend" case.
    // Without JSPI this must panic rather than hang.
    rt_no_jspi().block_on(async {
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let _ = rx.await;
    });
}
