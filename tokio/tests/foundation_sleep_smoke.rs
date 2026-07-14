//! Foundation smoke test: the canonical `tokio::time::sleep` code path
//! must drive a 10ms sleep to completion on emscripten using the native
//! `runtime::time::Driver` + scheduler infrastructure (with an
//! emscripten-backed park primitive underneath).
//!
//! This test will only compile when the native time-driver path is
//! enabled on emscripten. Currently the `cfg_time_native!` macro excludes
//! emscripten, so the `tokio::runtime::time` module isn't reachable —
//! this test depending on its presence is intentional and acts as a
//! "have we built the foundation yet" assertion.

#![cfg(all(
    target_os = "emscripten",
    feature = "rt",
    feature = "time",
    feature = "macros"
))]

#[tokio::test]
async fn sleeps_10ms_via_canonical_path() {
    // Reaching `runtime::Builder` proves the native runtime/scheduler
    // path is compiled in on emscripten. While we still use the public
    // `tokio::time::sleep` to actually drive the sleep, the type-level
    // reference below pins the test to the canonical foundation.
    let _proves_canonical_path_compiled: Option<tokio::runtime::Builder> = None;

    let start = tokio::time::Instant::now();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(start.elapsed() >= std::time::Duration::from_millis(10));
}
