//! `Runtime::block_on` on emscripten drives the scheduler synchronously to
//! a fixed point: it returns the output for futures that resolve without
//! suspending. What happens when a future would have to await a timer, an
//! external wake, or a host turn is decided by the link line: with `-sJSPI`
//! the stack parks on the host event loop (see `rt_emscripten_jspi`); without
//! it, `block_on` panics with a targeted error, since the host loop cannot
//! run while wasm is on the stack.
//!
//! This file runs in **both** CI lanes — the suite build (JSPI linked) and
//! the dedicated no-JSPI step — asserting the contract the binary was linked
//! for. The fixed-point cases never park, so they pass identically in both.

#![cfg(all(
    target_os = "emscripten",
    feature = "rt",
    feature = "time",
    feature = "macros"
))]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use tokio::runtime::Builder;

fn rt() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// The link-time capability tokio's runtime detects: 2 means `-sJSPI`.
fn jspi_linked() -> bool {
    extern "C" {
        fn emscripten_has_asyncify() -> std::ffi::c_int;
    }
    unsafe { emscripten_has_asyncify() == 2 }
}

/// Assert `f` panics with the targeted would-suspend message.
fn assert_panics_cannot_block_on(f: impl FnOnce()) {
    let err = catch_unwind(AssertUnwindSafe(f)).expect_err("expected a would-suspend panic");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("Cannot block_on"),
        "unexpected panic message: {msg}"
    );
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
fn timer_wait_follows_link_line() {
    // A real timer deadline can only be satisfied by returning to the host
    // event loop: parked and completed under `-sJSPI`, a targeted panic
    // without it.
    let wait = || {
        rt().block_on(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        })
    };
    if jspi_linked() {
        wait();
    } else {
        assert_panics_cannot_block_on(wait);
    }
}

#[test]
fn external_wait_panics_without_jspi() {
    // A oneshot whose sender never fires (no in-runtime work can complete
    // it) leaves the future pending with no timer — the "suspend" case.
    // Without JSPI this must panic rather than hang. (With JSPI it would
    // park forever, so the case is only exercisable in the no-JSPI lane.)
    if jspi_linked() {
        return;
    }
    assert_panics_cannot_block_on(|| {
        rt().block_on(async {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            let _ = rx.await;
        });
    });
}
