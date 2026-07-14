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

    assert!(rt.drive(), "drive from host context must run a pick-up");
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
