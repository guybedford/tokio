//! `tokio::net::TcpStream` (emscripten, `PollEvented`-backed over the reactor)
//! end-to-end: connect + typed read/write, including a vectored write
//! (`writev` over sockfs), against the ws echo server hosted by
//! `ci/emscripten_socket_entry.mjs`.
#![cfg(all(target_os = "emscripten", feature = "net"))]

use std::io::IoSlice;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

// Must match `ci/emscripten_socket_entry.mjs`.
const ECHO_PORT: u16 = 31_852;

#[tokio::test(flavor = "current_thread")]
async fn tcp_stream_connect_write_read() {
    let mut stream = TcpStream::connect(("127.0.0.1", ECHO_PORT))
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    stream.set_nodelay(true).unwrap();

    let msg = b"hello via tokio::net::TcpStream";
    stream.write_all(msg).await.expect("write_all");

    let mut buf = vec![0u8; msg.len()];
    stream.read_exact(&mut buf).await.expect("read_exact");
    assert_eq!(&buf, msg, "echoed payload mismatch");
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_stream_vectored_write() {
    let mut stream = TcpStream::connect(("127.0.0.1", ECHO_PORT))
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    assert!(
        stream.is_write_vectored(),
        "should advertise vectored writes"
    );

    let parts: [&[u8]; 3] = [b"vec", b"tored ", b"writev over sockfs"];
    let expected: Vec<u8> = parts.concat();
    let bufs = [
        IoSlice::new(parts[0]),
        IoSlice::new(parts[1]),
        IoSlice::new(parts[2]),
    ];
    let n = stream.write_vectored(&bufs).await.expect("write_vectored");
    assert_eq!(
        n,
        expected.len(),
        "writev should send all bytes for a small payload"
    );

    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await.expect("read_exact");
    assert_eq!(buf, expected, "echoed vectored payload mismatch");
}
