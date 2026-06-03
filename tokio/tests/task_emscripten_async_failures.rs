//! Intentionally-failing async tests, an `#[ignore]`d fixture for eyeballing the
//! harness's panic rendering (`-- --ignored`). Not `#[should_panic]` — that
//! would swallow the very message we want to inspect.

#![cfg(target_os = "emscripten")]

#[tokio::test]
#[ignore = "intentionally fails; run with --ignored to inspect output"]
async fn panics_with_message() {
    panic!("this is the panic message we want to see");
}

#[tokio::test]
#[ignore = "intentionally fails; run with --ignored to inspect output"]
async fn asserts_eq_fails() {
    let x = 1;
    let y = 2;
    assert_eq!(x, y, "x and y should be equal but aren't");
}

#[tokio::test]
#[ignore = "intentionally fails; run with --ignored to inspect output"]
async fn fails_after_sleep() {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    panic!("panic after sleep");
}

#[tokio::test]
#[ignore = "intentionally fails; run with --ignored to inspect output"]
async fn fails_after_spawn_await() {
    let handle = tokio::spawn(async {
        panic!("panic inside spawned task");
    });
    handle.await.unwrap();
}

#[tokio::test]
async fn passes_alongside_failures() {
    // Sanity: this should pass and is not ignored, so it runs in CI.
    assert_eq!(2 + 2, 4);
}
