#![cfg(all(
    target_os = "emscripten",
    feature = "rt",
    feature = "macros",
    feature = "time"
))]

#[tokio::main(flavor = "current_thread")]
async fn run() {
    let v = tokio::spawn(async { 41u32 }).await.unwrap() + 1;
    assert_eq!(v, 42);
}

#[tokio::main(flavor = "current_thread")]
async fn run_with_value() -> u32 {
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    7
}

#[test]
fn current_thread_main_blocks_to_completion() {
    // `#[tokio::main(flavor = "current_thread")]` uses the native expansion on
    // emscripten: `block_on` drives the body to completion (parking the stack
    // via JSPI when it suspends) before returning.
    run();
}

#[test]
fn current_thread_main_returns_value() {
    // Return values work — main blocks until the body resolves, so there is
    // no marshalling boundary.
    assert_eq!(run_with_value(), 7);
}
