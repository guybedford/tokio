#![cfg(all(target_os = "emscripten", feature = "rt", feature = "macros"))]

#[tokio::main(flavor = "current_thread")]
async fn run() {
    let v = tokio::spawn(async { 41u32 }).await.unwrap() + 1;
    assert_eq!(v, 42);
}

#[test]
fn current_thread_main_unit_via_worker() {
    // `#[tokio::main(flavor = "current_thread")]` with a `()` body drives the
    // body to completion in a worker (same path as `#[tokio::test]`); an
    // in-body assertion failure would marshal back as a panic.
    run();
}
