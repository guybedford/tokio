//! JSPI parking contracts: `block_on` suspends the stack on the host event
//! loop at its progression cliff, resumes on timer deadlines and external
//! wakes, and composes with the thread's event-loop runtime (which keeps being
//! driven by host turns while the stack is suspended).
//!
//! Plain `#[test]`s on the main instance: each `block_on` here is the real
//! suspension under test. (`#[tokio::test]` bodies run through this same
//! `block_on` path via the native macro expansion.)

#![cfg(all(
    target_os = "emscripten",
    feature = "rt",
    feature = "time",
    feature = "sync",
    feature = "macros"
))]

use std::time::Duration;

use tokio::runtime::Builder;

fn rt() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn block_on_sleep_parks_and_resumes() {
    let start = std::time::Instant::now();
    rt().block_on(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
    });
    assert!(
        start.elapsed() >= Duration::from_millis(15),
        "the park must actually wait out the timer deadline"
    );
}

#[test]
fn block_on_spawned_tasks_with_timers() {
    let out = rt().block_on(async {
        let a = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            20
        });
        let b = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            22
        });
        a.await.unwrap() + b.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn parked_block_on_resumes_on_cross_runtime_wake() {
    // The full park/wake round trip across runtimes: an event-loop root
    // (driven by host turns) completes a oneshot that a parked `block_on` on a
    // *different* runtime is suspended on. Covers: park at the cliff, the
    // event-loop runtime being driven while the stack is suspended, the wake
    // crossing runtimes through `request_pickup`, and resumption.
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();

    tokio::emscripten::event_loop::schedule(
        async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tx.send(11).unwrap();
        },
        |outcome| assert!(outcome.is_ok()),
    );
    tokio::emscripten::event_loop::drive();

    // Parks (the oneshot is pending and this runtime has no timers); the
    // event-loop root's host timer fires mid-suspension.
    let v = rt().block_on(async { rx.await.unwrap() });
    assert_eq!(v, 11);
}

#[test]
fn blocking_recv_parks_instead_of_panicking() {
    // `blocking_recv` reaches `ParkThread::park`, which suspends under JSPI.
    // The sender side runs on the event-loop runtime off a host timer.
    let (tx, rx) = tokio::sync::oneshot::channel::<&'static str>();

    tokio::emscripten::event_loop::schedule(
        async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            tx.send("woken").unwrap();
        },
        |outcome| assert!(outcome.is_ok()),
    );
    tokio::emscripten::event_loop::drive();

    assert_eq!(rx.blocking_recv().unwrap(), "woken");
}

#[test]
fn sequential_block_ons_share_the_thread() {
    // Each block_on parks and resumes independently; the thread's park/unpark
    // bookkeeping must balance across them.
    for i in 0..3u32 {
        let out = rt().block_on(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            i * 2
        });
        assert_eq!(out, i * 2);
    }
}
