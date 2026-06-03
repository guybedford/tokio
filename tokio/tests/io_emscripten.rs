//! Standard I/O tests for emscripten.
//!
//! `tokio::io::{stdout, stderr}` round through emscripten's libc, which
//! ultimately calls the JS `print`/`printErr` hooks configured by the worker
//! shim. Since the worker silences stdout/stderr by default to keep libtest
//! output clean, these tests mostly check that writes don't fail —
//! observable output verification is left to manual `--nocapture` runs.
//!
//! `tokio::io::stdin` is not connected in the worker harness; a read returns
//! promptly with an error rather than hanging. The contract worth pinning is
//! "stdin never deadlocks the worker", not a specific errno.

#![cfg(all(target_os = "emscripten", feature = "io-std"))]

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn stdout_write_completes() {
    let mut out = tokio::io::stdout();
    out.write_all(b"hello from stdout\n").await.unwrap();
    out.flush().await.unwrap();
}

#[tokio::test]
async fn stderr_write_completes() {
    let mut err = tokio::io::stderr();
    err.write_all(b"hello from stderr\n").await.unwrap();
    err.flush().await.unwrap();
}

#[tokio::test]
async fn stdout_large_multichunk_write_completes() {
    // Exercises the BufWriter chunking inside `Stdout` (writes larger than
    // the internal buffer force multiple underlying writes).
    let mut out = tokio::io::stdout();
    let data = vec![b'x'; 64 * 1024];
    out.write_all(&data).await.unwrap();
    out.flush().await.unwrap();
}

#[tokio::test]
async fn stdout_interleaved_writes_complete() {
    let mut out = tokio::io::stdout();
    for i in 0..16 {
        out.write_all(format!("line {i}\n").as_bytes())
            .await
            .unwrap();
    }
    out.flush().await.unwrap();
}

#[tokio::test]
async fn stdin_read_does_not_hang() {
    // stdin is not piped into the worker; a read must return promptly
    // (Ok(0) EOF or an I/O error) rather than blocking the worker. The
    // 60s worker watchdog would fail the test if this ever deadlocked.
    let mut stdin = tokio::io::stdin();
    let mut buf = [0u8; 32];
    let _ = stdin.read(&mut buf).await;
}
