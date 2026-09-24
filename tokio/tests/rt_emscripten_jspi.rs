//! JSPI suspension contracts. With `-sJSPI` a would-block wait suspends the
//! activation until a host timer fires or a later activation unparks it;
//! without it the wait panics (see `rt_emscripten_block_on`), so every test
//! here returns early unless the build linked JSPI.
//!
//! The JSPI lanes link `-sJSPI_EXPORTS=tokio_test_reenter,tokio_test_reenter_local`
//! so those exports are promising and a host call to one starts a sibling
//! fiber. The fiber-hook tests further need `--cfg tokio_unstable_jspi_hooks`
//! and a link with `-sJSPI_HOOKS` or `-sREENTRANT_JSPI`, probed at runtime so
//! one binary serves every lane.
//!
//! NOTE: This is the only Emscripten test file with real timer tests.

#![cfg(all(
    target_os = "emscripten",
    not(target_feature = "atomics"),
    feature = "rt",
    feature = "time",
    feature = "sync",
    feature = "macros"
))]

use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::task::LocalSet;
use tokio::time::{sleep, Instant};

fn rt() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

extern "C" {
    /// Emscripten's `ASYNCIFY` build mode; 2 is JSPI.
    fn emscripten_has_asyncify() -> i32;
}

fn jspi_linked() -> bool {
    // SAFETY: an Emscripten libc query with no arguments and no side effects.
    unsafe { emscripten_has_asyncify() == 2 }
}

macro_rules! require_jspi {
    () => {
        if !jspi_linked() {
            return;
        }
    };
}

fn is_nested_runtime_panic(e: &Box<dyn std::any::Any + Send>) -> bool {
    e.downcast_ref::<&str>()
        .map(|m| m.contains("Cannot start a runtime from within a runtime"))
        .unwrap_or(false)
}

/// Built with `--cfg tokio_unstable_jspi_hooks` and linked with
/// `-sJSPI_HOOKS`: every suspension is a leave of the runtime.
#[cfg(tokio_unstable_jspi_hooks)]
fn hooks_linked() -> bool {
    use std::ffi::c_void;
    extern "C" {
        fn jspi_register(
            hook: unsafe extern "C" fn(u32, *mut c_void, i32) -> *mut c_void,
            mask: u32,
        ) -> i32;
    }
    unsafe extern "C" fn noop(_: u32, token: *mut c_void, _: i32) -> *mut c_void {
        token
    }
    // SAFETY: registers a hook for no events.
    unsafe { jspi_register(noop, 0) == 0 }
}

#[cfg(not(tokio_unstable_jspi_hooks))]
fn hooks_linked() -> bool {
    false
}

extern "C" {
    fn emscripten_run_script(script: *const std::ffi::c_char);
    fn emscripten_run_script_int(script: *const std::ffi::c_char) -> i32;
    fn emscripten_promise_create() -> *mut std::ffi::c_void;
    fn emscripten_promise_destroy(promise: *mut std::ffi::c_void);
}

extern "C-unwind" {
    fn emscripten_promise_await_unchecked(promise: *mut std::ffi::c_void) -> *mut std::ffi::c_void;
}

fn run_js(script: &str) {
    let script = std::ffi::CString::new(script).unwrap();
    // SAFETY: a NUL-terminated script evaluated on the host.
    unsafe { emscripten_run_script(script.as_ptr()) }
}

/// Linked with `-sREENTRANT_JSPI`, where fibers have their own shadow stacks
/// and may run while a sibling is suspended: the reentrant lane sets
/// `TOKIO_REENTRANT_JSPI`.
fn reentrant_linked() -> bool {
    let script = std::ffi::CString::new(
        "typeof process != 'undefined' && process.env.TOKIO_REENTRANT_JSPI ? 1 : 0",
    )
    .unwrap();
    // SAFETY: a NUL-terminated script evaluated on the host.
    unsafe { emscripten_run_script_int(script.as_ptr()) != 0 }
}

/// Suspends the calling activation until `js`, run on the host after the
/// suspension, has settled; the value it settles with comes back.
fn suspend_on(js: &str) -> i32 {
    // SAFETY: a fresh promise handle, destroyed below.
    let promise = unsafe { emscripten_promise_create() };
    run_js(&format!(
        "Promise.resolve().then(() => {js}).then((v) => _emscripten_promise_resolve({}, 0, v))",
        promise as usize
    ));
    // SAFETY: the handle is live; the caller has checked `jspi_linked`.
    let value = unsafe { emscripten_promise_await_unchecked(promise) } as i32;
    // SAFETY: created above, awaited once.
    unsafe { emscripten_promise_destroy(promise) };
    value
}

// Promising export: a sibling fiber driving its own runtime through a park.
#[no_mangle]
pub extern "C" fn tokio_test_reenter() -> i32 {
    rt().block_on(async {
        sleep(Duration::from_millis(5)).await;
        42
    })
}

// Promising export: a sibling `LocalSet` that parks.
#[no_mangle]
pub extern "C" fn tokio_test_reenter_local() -> i32 {
    let rt = rt();
    let local = LocalSet::new();
    rt.block_on(local.run_until(async {
        let task = tokio::task::spawn_local(async {
            sleep(Duration::from_millis(5)).await;
            40
        });
        task.await.unwrap() + 2
    }))
}

// Non-promising export called during a task-issued suspension: a `block_on`
// that needs no park. Without the fiber hooks the runtime is still entered
// there, so it panics as nested.
#[no_mangle]
pub extern "C" fn tokio_test_reenter_sync() -> i32 {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let res = std::panic::catch_unwind(|| rt().block_on(async { 1 }));
    std::panic::set_hook(hook);
    match res {
        Ok(v) => v,
        Err(e) if is_nested_runtime_panic(&e) => -1,
        Err(_) => -2,
    }
}

// A suspension issued from task code leaves the runtime only with the fiber
// hooks; without them a sibling `block_on` during it is nested.
#[tokio::test]
async fn task_suspension_leaves_only_with_hooks() {
    require_jspi!();
    if cfg!(not(panic = "unwind")) {
        return;
    }
    let code = tokio::spawn(async { suspend_on("wasmExports.tokio_test_reenter_sync()") })
        .await
        .unwrap();
    assert_eq!(code, if hooks_linked() { 1 } else { -1 });
}

// The `lookup_host` shape: a suspending import called from task code, during
// which a sibling fiber drives its own runtime to completion. The task must
// resume as itself, on its own runtime.
#[tokio::test]
async fn sibling_fiber_during_task_suspension() {
    require_jspi!();
    if !hooks_linked() {
        return;
    }
    let out = tokio::spawn(async {
        let id = tokio::task::id();
        let b = suspend_on("wasmExports.tokio_test_reenter()");
        assert_eq!(tokio::task::id(), id);
        // B's runtime is gone; this spawn only works if we are back on ours.
        b + tokio::spawn(async { 1 }).await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(out, 43);
}

// A `LocalSet` entered in fiber A survives fiber B entering and parking its
// own `LocalSet` while A is suspended from task code.
#[test]
fn local_set_survives_sibling_fiber() {
    require_jspi!();
    if !hooks_linked() {
        return;
    }
    let rt = rt();
    let local = LocalSet::new();
    let out = rt.block_on(local.run_until(async {
        let first = tokio::task::spawn_local(async { 1 });
        let b = suspend_on("wasmExports.tokio_test_reenter_local()");
        let second = tokio::task::spawn_local(async { 2 });
        first.await.unwrap() + second.await.unwrap() + b
    }));
    assert_eq!(out, 45);
}

// A runs while B is suspended, then B resumes. Needs each fiber on its own
// shadow stack (`-sREENTRANT_JSPI`); the reentrant lane opts in.
#[test]
fn interleaved_suspended_runtimes() {
    require_jspi!();
    if !hooks_linked() || !reentrant_linked() {
        return;
    }
    run_js(
        "globalThis.tokioReenter = new Promise((resolve) => \
         setTimeout(() => resolve(wasmExports.tokio_test_reenter()), 5))",
    );
    let sum = rt().block_on(async {
        sleep(Duration::from_millis(20)).await;
        5
    });
    assert_eq!(sum, 5);
    assert_eq!(suspend_on("globalThis.tokioReenter"), 42);
}

#[test]
fn nested_block_on_still_panics() {
    require_jspi!();
    if cfg!(not(panic = "unwind")) {
        return;
    }
    let outer = rt();
    let res = outer.block_on(async {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(|| rt().block_on(async { 1 }));
        std::panic::set_hook(hook);
        res
    });
    assert!(is_nested_runtime_panic(&res.unwrap_err()));
}

#[test]
fn block_on_yield_now_takes_a_host_turn() {
    require_jspi!();
    let out = rt().block_on(async {
        tokio::task::yield_now().await;
        7
    });
    assert_eq!(out, 7);
}

#[tokio::test]
async fn root_sleep_parks_and_resumes() {
    require_jspi!();
    let start = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        start.elapsed() >= Duration::from_millis(15),
        "the park must actually wait out the timer deadline"
    );
}

#[tokio::test]
async fn root_spawned_tasks_with_timers() {
    require_jspi!();
    let out = async {
        let a = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            20
        });
        let b = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            22
        });
        a.await.unwrap() + b.await.unwrap()
    }
    .await;
    assert_eq!(out, 42);
}

#[tokio::test]
async fn sequential_parks_inside_one_root() {
    require_jspi!();
    // Each park must suspend and resume independently; leaf bookkeeping
    // must balance across them.
    for i in 0..3u32 {
        let start = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(start.elapsed() >= Duration::from_millis(1), "park {i}");
    }
}

#[tokio::test]
async fn root_park_resumes_on_timer_driven_wake() {
    require_jspi!();
    // The spawned task's timer bounds the driver park; on resume it sends
    // and wakes the root future.
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        tx.send(11).unwrap();
    });
    assert_eq!(rx.await.unwrap(), 11);
}

extern "C" {
    fn emscripten_async_call(
        func: extern "C" fn(*mut std::ffi::c_void),
        arg: *mut std::ffi::c_void,
        millis: i32,
    );
}

/// Run `f` from a fresh wasm activation after a host timeout.
fn host_callback(millis: i32, f: impl FnOnce() + 'static) {
    extern "C" fn trampoline(arg: *mut std::ffi::c_void) {
        // SAFETY: `arg` is the `Box<Box<dyn FnOnce()>>` leaked below, and
        // Emscripten invokes the callback exactly once.
        let f = unsafe { Box::from_raw(arg as *mut Box<dyn FnOnce()>) };
        f();
    }
    let f: Box<Box<dyn FnOnce()>> = Box::new(Box::new(f));
    // SAFETY: an Emscripten API scheduling `trampoline(arg)` on the host loop.
    unsafe { emscripten_async_call(trampoline, Box::into_raw(f) as *mut _, millis) }
}

// A host callback is a fresh activation entering tokio while the root
// activation is parked with no deadline; its send must resume the park.
#[test]
fn host_activation_wakes_park_without_deadline() {
    require_jspi!();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
    host_callback(10, move || tx.try_send(11).unwrap());
    let out = rt().block_on(async { rx.recv().await.unwrap() });
    assert_eq!(out, 11);
}

// The park is bounded by a far timer; the host callback's send must resume
// it at once rather than at that deadline.
#[test]
fn host_activation_wakes_timed_park_early() {
    require_jspi!();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
    host_callback(10, move || tx.try_send(11).unwrap());
    let start = Instant::now();
    let out = rt().block_on(async {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(out, 11);
    assert!(start.elapsed() < Duration::from_secs(5));
}

// A spawned task woken from a host activation, with the root awaiting it.
#[test]
fn host_activation_wakes_spawned_task() {
    require_jspi!();
    let notify = Arc::new(Notify::new());
    let n = notify.clone();
    host_callback(10, move || n.notify_one());
    let out = rt().block_on(async {
        tokio::spawn(async move {
            notify.notified().await;
            5
        })
        .await
        .unwrap()
    });
    assert_eq!(out, 5);
}

// A self-rewaking task must not starve the real host timer: the
// event-interval park yields a 0ms host turn so the timer still fires.
#[tokio::test]
async fn greedy_task_does_not_starve_host_timer() {
    require_jspi!();
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    sleep(Duration::from_millis(5)).await;
}

// When a nearer timer fires, the next park must re-arm for a still-pending
// farther timer rather than dropping it.
#[tokio::test]
async fn farther_timer_survives_nearer_timer_firing() {
    require_jspi!();
    let start = Instant::now();

    let notify = Arc::new(Notify::new());
    let n = notify.clone();
    let near = tokio::spawn(async move {
        sleep(Duration::from_millis(5)).await;
        n.notify_one();
    });
    let waiter = tokio::spawn(async move {
        notify.notified().await;
    });

    sleep(Duration::from_millis(25)).await;
    assert!(
        start.elapsed() >= Duration::from_millis(25),
        "farther timer did not hold its deadline"
    );

    near.await.unwrap();
    waiter.await.unwrap();
}

// A host activation can spawn onto the parked runtime: `tokio::spawn` sees
// the entered runtime's handle, and the spawn unparks the root to run it.
// With the fiber hooks the parked runtime's context is not on the thread,
// so the host activation spawns through the handle it holds.
#[test]
fn host_activation_spawns_onto_parked_runtime() {
    require_jspi!();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(2);
    let tx2 = tx.clone();
    let runtime = rt();
    let handle = runtime.handle().clone();
    let handle1 = if hooks_linked() {
        Some(handle.clone())
    } else {
        None
    };
    host_callback(10, move || {
        match handle1 {
            Some(handle1) => handle1.spawn(async move { tx.send(1).await.unwrap() }),
            None => tokio::spawn(async move { tx.send(1).await.unwrap() }),
        };
        handle.spawn(async move { tx2.send(2).await.unwrap() });
    });
    let out = runtime.block_on(async { rx.recv().await.unwrap() + rx.recv().await.unwrap() });
    assert_eq!(out, 3);
}
