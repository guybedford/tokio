//! Regression tests for emscripten-specific behaviors documented in
//! `tokio/src/lib.rs` under "Emscripten support".

#![cfg(target_os = "emscripten")]

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinError;

#[tokio::test]
async fn join_error_is_panic_is_false_on_emscripten() {
    // panic=abort means JoinError can only represent cancellation. Pin the
    // contract documented in `tokio/src/lib.rs`.
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    handle.abort();
    let err: JoinError = handle.await.unwrap_err();
    assert!(err.is_cancelled());
    assert!(!err.is_panic());
}

#[tokio::test]
async fn join_error_try_into_panic_returns_self() {
    // `try_into_panic` on emscripten must always return `Err(self)` because
    // the task didn't produce a panic payload — JoinError is exclusively a
    // cancellation signal.
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    handle.abort();
    let err = handle.await.unwrap_err();
    let recovered = err.try_into_panic();
    assert!(
        recovered.is_err(),
        "try_into_panic must return Err on emscripten",
    );
}

#[tokio::test]
async fn handle_current_outside_spawn_works() {
    // Calling `Handle::current()` from a top-level test body (i.e. outside
    // any `tokio::spawn`-wrapped future) must work — there is no separate
    // runtime context machinery on emscripten.
    let h = Handle::current();
    assert_eq!(h.spawn(async { 11 }).await.unwrap(), 11);
}

#[tokio::test]
async fn handle_try_current_never_fails_on_emscripten() {
    // Native `try_current` returns Err when no runtime is running; on
    // emscripten the executor is always present.
    let _h = Handle::try_current().expect("emscripten runtime is always present");
}
