//! Filesystem tests on emscripten.
//!
//! Round-trips against MEMFS (emscripten's default in-memory FS): write a
//! file, read it back, stat it, list a directory, remove it. Verifies that
//! `tokio::fs::*` works end-to-end on emscripten, where the underlying
//! "blocking" call is run inline by the synchronous `spawn_blocking` shim.

#![cfg(all(target_os = "emscripten", feature = "fs"))]

use tokio::fs;

#[tokio::test]
async fn write_then_read_round_trip() {
    let path = "/tmp/tokio_emscripten_round_trip.txt";
    fs::write(path, b"hello, emscripten\n").await.unwrap();
    let bytes = fs::read(path).await.unwrap();
    assert_eq!(&bytes[..], b"hello, emscripten\n");
}

#[tokio::test]
async fn read_to_string_round_trip() {
    let path = "/tmp/tokio_emscripten_string.txt";
    fs::write(path, "string contents").await.unwrap();
    let s = fs::read_to_string(path).await.unwrap();
    assert_eq!(s, "string contents");
}

#[tokio::test]
async fn metadata_reports_file_size() {
    let path = "/tmp/tokio_emscripten_size.bin";
    let payload = vec![0xAAu8; 1024];
    fs::write(path, &payload).await.unwrap();
    let meta = fs::metadata(path).await.unwrap();
    assert_eq!(meta.len(), 1024);
    assert!(meta.is_file());
}

#[tokio::test]
async fn create_dir_then_list_entries() {
    let dir = "/tmp/tokio_emscripten_dir";
    let _ = fs::remove_dir_all(dir).await; // best-effort cleanup
    fs::create_dir(dir).await.unwrap();
    fs::write(format!("{dir}/a"), b"a").await.unwrap();
    fs::write(format!("{dir}/b"), b"b").await.unwrap();

    let mut entries = fs::read_dir(dir).await.unwrap();
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn remove_file_then_read_errors() {
    let path = "/tmp/tokio_emscripten_remove.txt";
    fs::write(path, b"transient").await.unwrap();
    fs::remove_file(path).await.unwrap();
    let err = fs::read(path).await.expect_err("file should be gone");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}
