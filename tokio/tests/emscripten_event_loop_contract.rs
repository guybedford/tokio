//! Pins the hosted event-loop host-context contract: `schedule` queues only,
//! and wakes arriving from host context never drive inline. Plain `#[test]`s
//! so the bodies run on the main instance *outside* any drive — the path a
//! `#[tokio::test]` body (already inside a drive) cannot exercise.

#![cfg(all(
    target_os = "emscripten",
    tokio_unstable,
    feature = "rt",
    feature = "sync",
    feature = "macros"
))]

use std::cell::Cell;
use std::rc::Rc;

mod support {
    pub(crate) mod hosted_runtime;
}
use support::hosted_runtime::hosted_runtime;

#[test]
fn schedule_does_not_run_root_synchronously() {
    let rt = hosted_runtime();
    let ran = Rc::new(Cell::new(false));
    let done = Rc::new(Cell::new(false));

    let ran2 = ran.clone();
    let done2 = done.clone();
    rt.schedule(
        async move {
            ran2.set(true);
        },
        move |outcome| {
            assert!(outcome.is_ok());
            done2.set(true);
        },
    );

    assert!(
        !ran.get(),
        "schedule must queue only, not run the root on the caller's stack"
    );
    assert!(
        !done.get(),
        "on_complete must never be invoked before schedule returns"
    );

    rt.drive();
    assert!(ran.get(), "drive must run the scheduled root");
    assert!(done.get(), "drive must deliver on_complete");
}

#[test]
fn host_wake_does_not_drive_inline() {
    let rt = hosted_runtime();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let done = Rc::new(Cell::new(false));

    let done2 = done.clone();
    rt.schedule(
        async move {
            rx.await.unwrap();
        },
        move |outcome| {
            assert!(outcome.is_ok());
            done2.set(true);
        },
    );

    rt.drive();
    assert!(!done.get(), "root must be parked on the oneshot");

    // Waking from host context (outside any drive) must stay O(1): it queues
    // the task and arms a pick-up, never running it on this stack.
    tx.send(()).unwrap();
    assert!(
        !done.get(),
        "a host-context wake must not drive the runtime inline"
    );

    rt.drive();
    assert!(
        done.get(),
        "the queued wake must be observed by the next drive"
    );
}

// Two hosted runtimes complete interleaved roots, including a cross-runtime
// oneshot wake riding the pick-up latch: sending from a drive of `a` must
// re-drive `b` from host context, never inline.
#[test]
fn two_runtimes_interleave_with_cross_wake() {
    let a = hosted_runtime();
    let b = hosted_runtime();
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();

    let b_done = Rc::new(Cell::new(0u32));
    let b_done2 = b_done.clone();
    b.schedule(async move { rx.await.unwrap() }, move |out| {
        b_done2.set(out.unwrap())
    });
    b.drive();
    assert_eq!(b_done.get(), 0, "b is parked on the oneshot");

    let a_done = Rc::new(Cell::new(false));
    let a_done2 = a_done.clone();
    a.schedule(
        async move {
            tx.send(42).unwrap();
        },
        move |out| {
            assert!(out.is_ok());
            a_done2.set(true);
        },
    );
    a.drive();
    assert!(a_done.get(), "a's root completed in its drive");
    assert_eq!(
        b_done.get(),
        0,
        "the cross-runtime wake must ride a pick-up, not drive b inline"
    );

    b.drive();
    assert_eq!(b_done.get(), 42, "b observed the cross-runtime send");
}

// `block_on` on a hosted runtime is rejected eagerly, like a nested runtime:
// its wait point is the host loop, so no stack can hold the result — even for
// a future that would resolve without parking.
#[test]
fn block_on_hosted_runtime_panics() {
    let rt = hosted_runtime();
    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.local().block_on(async { 1 + 2 })
    }))
    .expect_err("block_on on a hosted runtime must panic");
    let msg = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("hosted event-loop runtime"),
        "unexpected panic message: {msg}"
    );
}
