//! End-to-end check of the emscripten I/O reactor via `tokio::io::unix::AsyncFd`:
//! a non-blocking TCP `connect`/`send`/`recv` against the echo server in
//! `ci/emscripten_socket_entry.mjs`, over websocket-emulated sockets. Each step
//! is driven by a sockfs event (`open` -> writable, `message` -> readable)
//! firing the reactor and waking the task.
//!
//! Run with `CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUNNER` =
//! `ci/emscripten_socket_entry.mjs` and `ws` resolvable by Node.
#![cfg(all(target_os = "emscripten", feature = "net"))]

use std::io;
use std::mem;
use std::os::fd::RawFd;

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

unsafe fn set_nonblocking(fd: RawFd) {
    let flags = libc::fcntl(fd, libc::F_GETFL, 0);
    assert!(flags >= 0, "F_GETFL: {}", io::Error::last_os_error());
    let r = libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    assert!(r == 0, "F_SETFL: {}", io::Error::last_os_error());
}

fn loopback_addr(port: u16) -> libc::sockaddr_in {
    let mut addr: libc::sockaddr_in = unsafe { mem::zeroed() };
    addr.sin_family = libc::AF_INET as _;
    addr.sin_port = port.to_be();
    // 127.0.0.1 in network byte order.
    addr.sin_addr = libc::in_addr {
        s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
    };
    addr
}

unsafe fn tcp_socket() -> RawFd {
    let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
    set_nonblocking(fd);
    fd
}

// Must match `ci/emscripten_socket_entry.mjs`.
const ECHO_PORT: u16 = 31_852;

#[tokio::test(flavor = "current_thread")]
async fn async_fd_connect_send_recv() {
    unsafe {
        let addr_len = mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;

        // Non-blocking connect returns `EINPROGRESS`; completion arrives as the
        // sockfs `open` event -> write-readiness -> wake.
        let client = tcp_socket();
        let caddr = loopback_addr(ECHO_PORT);
        let r = libc::connect(
            client,
            &caddr as *const _ as *const libc::sockaddr,
            addr_len,
        );
        if r != 0 {
            let err = io::Error::last_os_error();
            assert_eq!(
                err.raw_os_error(),
                Some(libc::EINPROGRESS),
                "connect: {err}"
            );
        }

        let client_afd = AsyncFd::with_interest(client, Interest::READABLE | Interest::WRITABLE)
            .unwrap_or_else(|e| panic!("register client fd: {e}"));

        // Hangs forever if the reactor never wakes on the `open` event.
        client_afd
            .writable()
            .await
            .expect("writable")
            .retain_ready();

        // Confirm the socket actually connected (no pending SO_ERROR).
        let mut soerr: libc::c_int = 0;
        let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
        let gr = libc::getsockopt(
            client,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut soerr as *mut _ as *mut libc::c_void,
            &mut len,
        );
        assert_eq!(gr, 0, "getsockopt: {}", io::Error::last_os_error());
        assert_eq!(soerr, 0, "connect failed with SO_ERROR={soerr}");

        // Send, then await the echo: arrival is the sockfs `message` event ->
        // read-readiness -> wake.
        let msg = b"ping over emscripten sockets";
        let sent = libc::send(client, msg.as_ptr() as *const _, msg.len(), 0);
        assert_eq!(
            sent,
            msg.len() as isize,
            "send: {}",
            io::Error::last_os_error()
        );

        let mut buf = [0u8; 64];
        let got = loop {
            let mut guard = client_afd.readable().await.expect("readable");
            let n = libc::recv(client, buf.as_mut_ptr() as *mut _, buf.len(), 0);
            if n > 0 {
                break n as usize;
            }
            let err = io::Error::last_os_error();
            assert_eq!(err.raw_os_error(), Some(libc::EAGAIN), "recv: {err}");
            guard.clear_ready();
        };
        assert_eq!(&buf[..got], msg, "echoed payload mismatch");

        libc::close(client);
    }
}
