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
fn block_on_yield_now_takes_a_host_turn() {
    // A deferred `yield_now` wake advances only at a park point (as on
    // native): the drive parks for a 0 ms host turn and resumes.
    let out = rt().block_on(async {
        tokio::task::yield_now().await;
        7
    });
    assert_eq!(out, 7);
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
