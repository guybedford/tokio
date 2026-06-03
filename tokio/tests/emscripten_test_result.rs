#![cfg(target_os = "emscripten")]
#![warn(rust_2018_idioms)]

use std::io;

// A `Result`-returning test (the `?` pattern) must compile and run on emscripten:
// `Ok` passes, and the worker harness converts `Err` into a rendered failure.
#[tokio::test]
async fn result_ok_passes() -> Result<(), io::Error> {
    tokio::task::yield_now().await;
    let _: () = Ok::<(), io::Error>(())?;
    Ok(())
}

// Same, with an explicit error type, exercising `?` across an await.
#[tokio::test]
async fn result_question_mark() -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::yield_now().await;
    let n: u32 = "42".parse()?;
    assert_eq!(n, 42);
    Ok(())
}
