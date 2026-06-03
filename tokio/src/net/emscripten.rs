//! `tokio::net` TCP socket type for `wasm32-unknown-emscripten`. emscripten's
//! sockfs emulates TCP over WebSockets; this drives a non-blocking libc socket
//! whose readiness arrives via the reactor's sockfs callbacks, built on the
//! shared [`ReactorStream`] (a `PollEvented` over the reactor `Source`) — the
//! same plumbing the emscripten `unix` socket types use.
//!
//! Only the outbound client path is implemented (`connect` + read/write);
//! server-side `accept` is unsupported on sockfs.
//!
//! [`ReactorStream`]: crate::net::reactor_stream::ReactorStream

use crate::io::{AsyncRead, AsyncWrite, ReadBuf};
use crate::net::reactor_stream::ReactorStream;
use crate::net::ToSocketAddrs;

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A TCP stream, the emscripten counterpart of [`crate::net::TcpStream`].
pub struct TcpStream {
    inner: ReactorStream,
}

impl TcpStream {
    /// Opens a TCP connection to `addr` (resolved via emscripten's `getaddrinfo`).
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
        let mut last_err = None;
        for addr in crate::net::to_socket_addrs(addr).await? {
            match TcpStream::connect_addr(addr).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any address",
            )
        }))
    }

    async fn connect_addr(addr: SocketAddr) -> io::Result<TcpStream> {
        let (domain, storage, len) = socket_addr(&addr);
        // SAFETY: standard libc socket creation.
        let fd = unsafe { libc::socket(domain, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // From here `fd` is owned by `inner` (closed on drop) on every path.
        set_nonblocking(fd)?;

        // SAFETY: `storage`/`len` describe a valid sockaddr for `connect`.
        let r = unsafe { libc::connect(fd, &storage as *const _ as *const libc::sockaddr, len) };
        if r != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::EINPROGRESS) {
                // SAFETY: `fd` is open and unregistered; close it before bailing.
                unsafe { libc::close(fd) };
                return Err(err);
            }
        }

        let stream = TcpStream {
            inner: ReactorStream::from_raw_fd(fd)?,
        };

        // Connect completes as the sockfs `open` event -> write-readiness.
        poll_fn(|cx| stream.inner.poll_write_ready(cx)).await?;

        let mut so_error: libc::c_int = 0;
        let mut opt_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: reads `SO_ERROR` into `so_error`/`opt_len`.
        let gr = unsafe {
            libc::getsockopt(
                stream.inner.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut so_error as *mut _ as *mut libc::c_void,
                &mut opt_len,
            )
        };
        if gr != 0 {
            return Err(io::Error::last_os_error());
        }
        if so_error != 0 {
            return Err(io::Error::from_raw_os_error(so_error));
        }

        Ok(stream)
    }

    /// No-op on emscripten: sockfs has no `setsockopt`, and its WebSocket
    /// transport has no Nagle's algorithm, so `TCP_NODELAY` is moot.
    pub fn set_nodelay(&self, _nodelay: bool) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: `recv` only writes into the buffer's unfilled region.
        unsafe { self.inner.poll_read(cx, buf) }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.inner.poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // sockfs `shutdown(2)` is `ENOSYS` and WebSockets have no half-close; the
        // fd `close` on drop is the only teardown.
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream").field("fd", &self.inner.as_raw_fd()).finish()
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsFd for TcpStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the fd is owned by `self.inner` for `self`'s lifetime.
        unsafe { BorrowedFd::borrow_raw(self.as_raw_fd()) }
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: standard `F_GETFL`/`F_SETFL` on a valid fd.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let r = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Build a `sockaddr_storage` (and its length + domain) from a `SocketAddr`.
fn socket_addr(addr: &SocketAddr) -> (libc::c_int, libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: zeroed storage is a valid (empty) sockaddr we fully initialize.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            let sin = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            (
                libc::AF_INET,
                storage,
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(v6) => {
            let sin6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            (
                libc::AF_INET6,
                storage,
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}
