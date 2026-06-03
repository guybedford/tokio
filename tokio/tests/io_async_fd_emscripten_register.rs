//! Registration semantics: a fd registers once. Like mio, a second registration
//! must fail rather than silently displace the first (which would also let a
//! reused fd inherit stale readiness).
#![cfg(all(target_os = "emscripten", feature = "net"))]

use std::io;
use std::os::fd::RawFd;

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

unsafe fn tcp_socket() -> RawFd {
    let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
    fd
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_fd_registration_is_rejected() {
    let fd = unsafe { tcp_socket() };

    let first =
        AsyncFd::with_interest(fd, Interest::READABLE).expect("first registration should succeed");

    let err = match AsyncFd::with_interest(fd, Interest::READABLE) {
        Ok(_) => panic!("second registration of the same fd must fail"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "got: {err}");

    // Dropping the first deregisters the fd, after which it can be registered
    // again.
    drop(first);
    let third = AsyncFd::with_interest(fd, Interest::READABLE)
        .expect("registration after deregistration should succeed");
    drop(third);

    unsafe { libc::close(fd) };
}
