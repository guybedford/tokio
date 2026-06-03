//! Coverage for emscripten implementations of `JoinSet`, `LocalSet`,
//! `Handle`, and `task_local!`.

#![cfg(target_os = "emscripten")]

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::{spawn_local, JoinSet, LocalSet};

#[tokio::test]
async fn handle_current_spawn_round_trip() {
    let h = Handle::current();
    let value = h.spawn(async { 7 }).await.unwrap();
    assert_eq!(value, 7);
}

#[tokio::test]
async fn handle_try_current_succeeds() {
    let h = Handle::try_current().expect("emscripten runtime always present");
    let _enter = h.enter();
    assert_eq!(h.spawn(async { 1 + 1 }).await.unwrap(), 2);
}

#[tokio::test]
async fn local_set_run_until_returns_value() {
    let local = LocalSet::new();
    let v = local.run_until(async { 42 }).await;
    assert_eq!(v, 42);
}

#[tokio::test]
async fn spawn_local_runs_to_completion() {
    let local = LocalSet::new();
    let value = local
        .run_until(async {
            let handle = spawn_local(async { 99 });
            handle.await.unwrap()
        })
        .await;
    assert_eq!(value, 99);
}

#[tokio::test]
async fn join_set_collects_results() {
    let mut set = JoinSet::new();
    for i in 0..5 {
        set.spawn(async move { i * 10 });
    }
    let mut values = Vec::new();
    while let Some(res) = set.join_next().await {
        values.push(res.unwrap());
    }
    values.sort();
    assert_eq!(values, vec![0, 10, 20, 30, 40]);
}

#[tokio::test]
async fn join_set_join_all_returns_all_values() {
    let mut set = JoinSet::new();
    for i in 0..3 {
        set.spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            i
        });
    }
    let mut values = set.join_all().await;
    values.sort();
    assert_eq!(values, vec![0, 1, 2]);
}

#[tokio::test]
async fn join_set_abort_all_cancels_outstanding() {
    let mut set: JoinSet<i32> = JoinSet::new();
    for _ in 0..3 {
        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            unreachable!("should have been aborted")
        });
    }
    set.abort_all();
    let mut cancelled = 0;
    while let Some(res) = set.join_next().await {
        assert!(
            res.as_ref().is_err_and(|e| e.is_cancelled()),
            "expected cancellation, got {res:?}"
        );
        cancelled += 1;
    }
    assert_eq!(cancelled, 3);
}

#[tokio::test]
async fn join_set_try_join_next_returns_none_when_empty() {
    let mut set: JoinSet<()> = JoinSet::new();
    assert!(set.try_join_next().is_none());
}

#[tokio::test]
async fn join_set_shutdown_drains() {
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..4 {
        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
    }
    set.shutdown().await;
    assert!(set.is_empty());
}

tokio::task_local! {
    static REQUEST_ID: u32;
}

#[tokio::test]
async fn task_local_scope_isolates_value() {
    let result = REQUEST_ID
        .scope(42, async { REQUEST_ID.with(|v| *v * 2) })
        .await;
    assert_eq!(result, 84);
}

#[tokio::test]
async fn task_local_nested_scopes() {
    REQUEST_ID
        .scope(1, async {
            assert_eq!(REQUEST_ID.with(|v| *v), 1);
            REQUEST_ID
                .scope(2, async {
                    assert_eq!(REQUEST_ID.with(|v| *v), 2);
                })
                .await;
            assert_eq!(REQUEST_ID.with(|v| *v), 1);
        })
        .await;
}
