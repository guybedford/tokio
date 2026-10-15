//! A `-sJSPI` build on an Emscripten older than tokio's JSPI minimum must not
//! suspend, and instead keeps the non-JSPI behavior: timed waits sleep and
//! only a wait without a deadline panics. `uname` is overridden below to
//! report such a version.

#![cfg(all(
    target_os = "emscripten",
    not(target_feature = "atomics"),
    feature = "rt",
    feature = "time",
    feature = "sync",
    feature = "macros"
))]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

use tokio::runtime::Builder;

const RELEASE: &[u8] = b"6.0.9\0";

#[repr(C)]
struct UtsName {
    sysname: [u8; 65],
    nodename: [u8; 65],
    release: [u8; 65],
    version: [u8; 65],
    machine: [u8; 65],
    domainname: [u8; 65],
}

/// Replaces libc's `uname`, which reports the linking Emscripten version.
#[no_mangle]
extern "C" fn uname(buf: *mut UtsName) -> i32 {
    // SAFETY: the caller passes a valid `struct utsname`.
    let buf = unsafe { &mut *buf };
    buf.release[..RELEASE.len()].copy_from_slice(RELEASE);
    0
}

extern "C" {
    /// Emscripten's `ASYNCIFY` build mode; 2 is JSPI.
    fn emscripten_has_asyncify() -> i32;
}

macro_rules! require_jspi {
    () => {
        // SAFETY: an Emscripten libc query with no arguments and no side effects.
        if unsafe { emscripten_has_asyncify() } != 2 {
            return;
        }
    };
}

fn rt() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn timer_wait_completes() {
    require_jspi!();
    let start = Instant::now();
    rt().block_on(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
    });
    assert!(start.elapsed() >= Duration::from_millis(10));
}

#[test]
fn timeout_elapses() {
    require_jspi!();
    let (_tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let res = rt().block_on(async { tokio::time::timeout(Duration::from_millis(10), rx).await });
    assert!(res.is_err());
}

#[test]
fn maintenance_yield_does_not_suspend() {
    require_jspi!();
    // Well past `event_interval`, so the scheduler's zero-duration
    // maintenance park runs mid-drive.
    let out = rt().block_on(async {
        let handles: Vec<_> = (0..1000u32)
            .map(|i| tokio::spawn(async move { i }))
            .collect();
        let mut sum = 0;
        for h in handles {
            sum += h.await.unwrap();
        }
        sum
    });
    assert_eq!(out, 499_500);
}

#[test]
fn wait_without_a_deadline_panics_with_version() {
    require_jspi!();
    if cfg!(not(panic = "unwind")) {
        return;
    }
    let err = catch_unwind(AssertUnwindSafe(|| {
        rt().block_on(std::future::pending::<()>());
    }))
    .expect_err("expected a would-suspend panic");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("cannot block") && msg.contains("6.0.10") && msg.contains("6.0.9"),
        "unexpected panic message: {msg}"
    );
}
