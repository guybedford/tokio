//! End-to-end `#[tokio::test]` smoke tests for emscripten: each runs in a Node
//! worker that drives the future and reports pass/fail to the parent via the
//! SAB. Each targets a different subsystem so a regression localises.

#![cfg(target_os = "emscripten")]

use std::time::Duration;

// -----------------------------------------------------------------------------
// Smoke: macro expansion + worker-thread plumbing
// -----------------------------------------------------------------------------

#[tokio::test]
async fn empty_async_test_completes() {}

#[tokio::test]
async fn returns_unit_explicitly() {
    // Same as `empty_async_test_completes` but with an explicit `()` return,
    // verifying the macro handles both implicit and explicit unit-typed
    // bodies.
    #[allow(clippy::unused_unit)]
    ()
}

// -----------------------------------------------------------------------------
// tokio::time
// -----------------------------------------------------------------------------

#[tokio::test]
async fn sleep_short_completes() {
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::test]
async fn sleep_elapses_at_least_the_requested_duration() {
    let start = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "sleep returned too early: {elapsed:?}"
    );
}

#[tokio::test]
async fn multiple_sequential_sleeps() {
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn timeout_does_not_fire_when_inner_completes_first() {
    let result = tokio::time::timeout(Duration::from_secs(1), async { "ok" }).await;
    assert_eq!(result.unwrap(), "ok");
}

#[tokio::test]
async fn timeout_fires_when_inner_is_slower() {
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        tokio::time::sleep(Duration::from_secs(60)),
    )
    .await;
    assert!(result.is_err(), "timeout should have fired");
}

// -----------------------------------------------------------------------------
// tokio::task::spawn
// -----------------------------------------------------------------------------

#[tokio::test]
async fn spawned_task_runs_to_completion() {
    let handle = tokio::spawn(async { 42 });
    let value = handle.await.unwrap();
    assert_eq!(value, 42);
}

#[tokio::test]
async fn spawned_task_can_sleep() {
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        7
    });
    assert_eq!(handle.await.unwrap(), 7);
}

#[tokio::test]
async fn many_concurrent_spawns_complete() {
    let mut handles = Vec::new();
    for i in 0..16 {
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            i * 2
        }));
    }
    let mut sum = 0i32;
    for h in handles {
        sum += h.await.unwrap();
    }
    // 2 * (0 + 1 + ... + 15) = 240
    assert_eq!(sum, 240);
}

#[tokio::test]
async fn yield_now_returns_control() {
    // `yield_now` only completes after the executor polls another task —
    // verifies the cooperative budget machinery isn't deadlocked. The
    // `!Send` `Rc` requires `spawn_local`, hence the `LocalSet`.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let flag = std::rc::Rc::new(std::cell::Cell::new(false));
            let flag_clone = flag.clone();
            let handle = tokio::task::spawn_local(async move {
                flag_clone.set(true);
            });
            tokio::task::yield_now().await;
            let _ = handle.await;
            assert!(flag.get(), "spawned task did not run across yield_now");
        })
        .await;
}

// -----------------------------------------------------------------------------
// tokio::sync (smoke; the primitives themselves are runtime-agnostic)
// -----------------------------------------------------------------------------

#[tokio::test]
async fn oneshot_round_trip() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        tx.send("hello").unwrap();
    });
    assert_eq!(rx.await.unwrap(), "hello");
}

#[tokio::test]
async fn mpsc_round_trip() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<i32>(4);
    tokio::spawn(async move {
        for i in 0..5 {
            tx.send(i).await.unwrap();
        }
    });
    let mut total = 0;
    while let Some(v) = rx.recv().await {
        total += v;
    }
    assert_eq!(total, 10);
}

#[tokio::test]
async fn dropping_many_long_sleeps_does_not_leak() {
    // Schedule a large number of long-deadline sleeps, poll each one once so
    // the timer entry actually registers with the driver's wheel, then drop
    // them. If `Drop` failed to deregister the entry and release its state,
    // this would balloon emscripten's HEAP. We snapshot heap size
    // before/after and assert growth stays within a small margin — enough
    // for transient allocator overhead, far less than 10 000 leaked timer
    // registrations would produce.
    use std::future::Future;
    use std::task::Context;

    extern "C" {
        fn emscripten_get_heap_size() -> usize;
    }

    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    // Warm the allocator: do one pass first so any initial heap growth
    // happens before we baseline. Without this, the first allocation may
    // trigger a 64 KiB page extension that biases the measurement.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..1_000 {
        let mut s = Box::pin(tokio::time::sleep(Duration::from_secs(86_400)));
        let _ = s.as_mut().poll(&mut cx);
        drop(s);
    }

    let before = unsafe { emscripten_get_heap_size() };

    for _ in 0..10_000 {
        let mut s = Box::pin(tokio::time::sleep(Duration::from_secs(86_400)));
        let _ = s.as_mut().poll(&mut cx);
        drop(s);
    }

    let after = unsafe { emscripten_get_heap_size() };
    let growth = after.saturating_sub(before);

    // A real leak (each `Sleep`'s registered timer state) would grow by at
    // least 10_000 × that allocation ≈ 240+ KiB. Allow 128 KiB for
    // incidental allocator slack across the loop.
    assert!(
        growth < 128 * 1024,
        "heap grew by {growth} bytes across 10k Sleep drops — suspect leak",
    );

    // After mass-dropping, a fresh short sleep must still complete normally —
    // verifying the executor and timer plumbing weren't corrupted.
    tokio::time::sleep(Duration::from_millis(5)).await;
}
