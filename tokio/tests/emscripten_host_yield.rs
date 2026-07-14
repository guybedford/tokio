#![cfg(target_os = "emscripten")]
#![warn(rust_2018_idioms)]

use std::time::Duration;

// A self-rewaking task (`yield_now`) keeps the drive progressing forever; on the
// shared host loop that would starve this real timer and hang. The poll-budget
// escape valve bounds the drive so the timer still fires.
#[tokio::test]
async fn greedy_task_does_not_starve_host_timer() {
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
}
