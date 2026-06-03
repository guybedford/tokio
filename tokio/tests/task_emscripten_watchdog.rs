//! Manual watchdog regression test. `worker.js` has a 60s watchdog that unblocks
//! the parent if the worker never calls back; verify it with a shortened one:
//!
//! ```text
//! TOKIO_EMSCRIPTEN_WATCHDOG_MS=500 \
//!     cargo test --target wasm32-unknown-emscripten ... \
//!         --test task_emscripten_watchdog -- --ignored
//! ```
//!
//! Expected: the test fails with a panic whose message contains
//! "worker did not respond". `#[ignore]` because the default 60s watchdog
//! would otherwise stall CI for a full minute.

#![cfg(target_os = "emscripten")]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct NeverReady;

impl Future for NeverReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

#[tokio::test]
#[ignore = "manual watchdog smoke test; see file header"]
async fn worker_watchdog_fires_when_future_never_completes() {
    NeverReady.await;
}
