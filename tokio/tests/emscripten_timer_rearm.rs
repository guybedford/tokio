#![cfg(target_os = "emscripten")]
#![warn(rust_2018_idioms)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{sleep, Instant};

// When a nearer timer fires, the drive forgets the spent id and must re-arm for
// a still-pending farther timer. Pins that the farther timer isn't dropped
// across that re-arm, even with a wake (the `Notify`) handled in the same drive.
#[tokio::test]
async fn farther_timer_survives_nearer_timer_firing() {
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

    // Hangs if the forget+re-arm dropped this still-pending timer.
    sleep(Duration::from_millis(25)).await;
    assert!(
        start.elapsed() >= Duration::from_millis(25),
        "farther timer did not hold its deadline"
    );

    near.await.unwrap();
    waiter.await.unwrap();
}
