//! `tokio::net::unix` socket types for `wasm32-unknown-emscripten`, the
//! reactor-backed counterpart of the mio implementation.
//!
//! Each type is a thin shell over the shared [`ReactorStream`] (a
//! `PollEvented` over the emscripten reactor [`Source`]), exactly like the
//! emscripten [`TcpStream`](crate::net::TcpStream). Addressing, `connect`,
//! `bind`, and `accept` reuse `std::os::unix::net` (which compiles on
//! emscripten); only the async readiness layer is bespoke.
//!
//! Emscripten reports `cfg(unix)`, so these exist to keep `unix` code compiling
//! (matching `std`, which also exposes `UnixStream` here). Whether AF_UNIX is
//! usable at runtime depends on emscripten's sockfs; unsupported operations
//! surface as ordinary `io::Error`s.
//!
//! [`ReactorStream`]: crate::net::reactor_stream::ReactorStream
//! [`Source`]: crate::runtime::io::Source

use crate::io::{AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};
use crate::net::reactor_stream::ReactorStream;
use crate::net::unix::{SocketAddr, UCred};

use std::fmt;
use std::io::{self};
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, RawFd};
use std::os::unix::net as std_net;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Borrow `fd` as a `std` socket of type `T` for the duration of `f` without
/// taking ownership — the fd is released (not closed) before returning.
fn with_std<T: FromRawFd + IntoRawFd, R>(fd: RawFd, f: impl FnOnce(&T) -> R) -> R {
    // SAFETY: `fd` is owned by the reactor `Source` for the call; we hand it
    // back via `into_raw_fd` so it is never double-closed.
    let sock = unsafe { T::from_raw_fd(fd) };
    let ret = f(&sock);
    let _ = sock.into_raw_fd();
    ret
}

/// Register an already-`set_nonblocking` `std` socket with the reactor.
fn register<T: IntoRawFd>(sock: T) -> io::Result<ReactorStream> {
    ReactorStream::from_raw_fd(sock.into_raw_fd())
}

// ===== UnixStream =====

/// A Unix stream, the emscripten counterpart of [`crate::net::UnixStream`].
pub struct UnixStream {
    inner: ReactorStream,
}

impl UnixStream {
    /// Connects to the socket at `path`.
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<UnixStream> {
        // AF_UNIX connect is local and completes immediately, so the `std`
        // (blocking) connect doesn't meaningfully stall the host loop.
        UnixStream::from_std(std_net::UnixStream::connect(path)?)
    }

    /// Connects to a pathname [`SocketAddr`]. Abstract/unnamed addresses are
    /// unsupported on emscripten.
    pub async fn connect_addr(addr: &SocketAddr) -> io::Result<UnixStream> {
        match addr.as_pathname() {
            Some(path) => UnixStream::connect(path).await,
            None => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "only pathname AF_UNIX addresses are supported on emscripten",
            )),
        }
    }

    /// Creates an unnamed pair of connected streams.
    pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
        let (a, b) = std_net::UnixStream::pair()?;
        Ok((UnixStream::from_std(a)?, UnixStream::from_std(b)?))
    }

    /// Registers a `std` stream with the reactor (setting it non-blocking).
    pub fn from_std(stream: std_net::UnixStream) -> io::Result<UnixStream> {
        stream.set_nonblocking(true)?;
        Ok(UnixStream {
            inner: register(stream)?,
        })
    }

    /// Deregisters the stream and returns the inner `std` socket.
    pub fn into_std(self) -> io::Result<std_net::UnixStream> {
        // SAFETY: we own the fd, just released from the reactor.
        Ok(unsafe { std_net::UnixStream::from_raw_fd(self.inner.into_raw_fd()?) })
    }

    /// Returns the local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        with_std::<std_net::UnixStream, _>(self.inner.as_raw_fd(), |s| s.local_addr())
            .map(SocketAddr::from)
    }

    /// Returns the peer address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        with_std::<std_net::UnixStream, _>(self.inner.as_raw_fd(), |s| s.peer_addr())
            .map(SocketAddr::from)
    }

    /// Peer credentials are not available on emscripten.
    pub fn peer_cred(&self) -> io::Result<UCred> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peer credentials are not available on emscripten",
        ))
    }

    /// Returns any pending `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        with_std::<std_net::UnixStream, _>(self.inner.as_raw_fd(), |s| s.take_error())
    }

    /// Waits for any of `interest` to become ready.
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.ready(interest).await
    }

    /// Waits for the socket to become readable.
    pub async fn readable(&self) -> io::Result<()> {
        self.ready(Interest::READABLE).await.map(|_| ())
    }

    /// Waits for the socket to become writable.
    pub async fn writable(&self) -> io::Result<()> {
        self.ready(Interest::WRITABLE).await.map(|_| ())
    }

    /// Polls for read readiness.
    pub fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx)
    }

    /// Polls for write readiness.
    pub fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx)
    }

    /// Tries to read data, returning `WouldBlock` if not ready.
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.try_read(buf)
    }

    /// Tries a vectored read.
    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.try_read_vectored(bufs)
    }

    /// Tries to write data.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.inner.try_write(buf)
    }

    /// Tries a vectored write.
    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.inner.try_write_vectored(bufs)
    }

    /// Runs `f` once the socket is ready for `interest`.
    pub async fn async_io<R>(
        &self,
        interest: Interest,
        f: impl FnMut() -> io::Result<R>,
    ) -> io::Result<R> {
        self.inner.async_io(interest, f).await
    }

    /// Tries `f` against current readiness.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        self.inner.try_io(interest, f)
    }

    /// Splits into borrowed read/write halves.
    pub fn split(&mut self) -> (super::ReadHalf<'_>, super::WriteHalf<'_>) {
        super::split::split(self)
    }

    /// Splits into owned read/write halves.
    pub fn into_split(self) -> (super::OwnedReadHalf, super::OwnedWriteHalf) {
        super::split_owned::split_owned(self)
    }

    cfg_io_util! {
        /// Tries to read data into `buf`, advancing it by the amount read.
        pub fn try_read_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            self.inner.try_read_buf(buf)
        }
    }

    // ===== shared with `split` / `split_owned` =====

    pub(crate) fn poll_read_priv(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: `recv` only writes into the buffer's unfilled region.
        unsafe { self.inner.poll_read(cx, buf) }
    }

    pub(crate) fn poll_write_priv(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write(cx, buf)
    }

    pub(super) fn poll_write_vectored_priv(
        &self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_write_vectored(cx, bufs)
    }

    pub(super) fn shutdown_std(&self, how: Shutdown) -> io::Result<()> {
        // Best-effort: emscripten sockfs `shutdown(2)` may be unsupported.
        with_std::<std_net::UnixStream, _>(self.inner.as_raw_fd(), |s| s.shutdown(how))
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_priv(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self.shutdown_std(Shutdown::Write);
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixStream")
            .field("fd", &self.inner.as_raw_fd())
            .finish()
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsFd for UnixStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the fd is owned by `self.inner` for `self`'s lifetime.
        unsafe { BorrowedFd::borrow_raw(self.as_raw_fd()) }
    }
}

// ===== UnixListener =====

/// A Unix listener, the emscripten counterpart of [`crate::net::UnixListener`].
pub struct UnixListener {
    inner: ReactorStream,
}

impl UnixListener {
    /// Binds a new listener to `path`.
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<UnixListener> {
        UnixListener::from_std(std_net::UnixListener::bind(path)?)
    }

    /// Registers a `std` listener with the reactor (setting it non-blocking).
    pub fn from_std(listener: std_net::UnixListener) -> io::Result<UnixListener> {
        listener.set_nonblocking(true)?;
        Ok(UnixListener {
            inner: register(listener)?,
        })
    }

    /// Deregisters the listener and returns the inner `std` socket.
    pub fn into_std(self) -> io::Result<std_net::UnixListener> {
        // SAFETY: we own the fd, just released from the reactor.
        Ok(unsafe { std_net::UnixListener::from_raw_fd(self.inner.into_raw_fd()?) })
    }

    /// Returns the local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        with_std::<std_net::UnixListener, _>(self.inner.as_raw_fd(), |s| s.local_addr())
            .map(SocketAddr::from)
    }

    /// Returns any pending `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        with_std::<std_net::UnixListener, _>(self.inner.as_raw_fd(), |s| s.take_error())
    }

    /// Accepts a new incoming connection.
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        let fd = self.inner.as_raw_fd();
        self.inner
            .async_io(Interest::READABLE, || {
                let (stream, addr) = with_std::<std_net::UnixListener, _>(fd, |l| l.accept())?;
                Ok((UnixStream::from_std(stream)?, SocketAddr::from(addr)))
            })
            .await
    }

    /// Polls to accept a new incoming connection.
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<io::Result<(UnixStream, SocketAddr)>> {
        let fd = self.inner.as_raw_fd();
        self.inner.poll_read_io(cx, || {
            let (stream, addr) = with_std::<std_net::UnixListener, _>(fd, |l| l.accept())?;
            Ok((UnixStream::from_std(stream)?, SocketAddr::from(addr)))
        })
    }
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixListener")
            .field("fd", &self.inner.as_raw_fd())
            .finish()
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsFd for UnixListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the fd is owned by `self.inner` for `self`'s lifetime.
        unsafe { BorrowedFd::borrow_raw(self.as_raw_fd()) }
    }
}

// ===== UnixDatagram =====

/// A Unix datagram socket, the emscripten counterpart of
/// [`crate::net::UnixDatagram`].
pub struct UnixDatagram {
    inner: ReactorStream,
}

impl UnixDatagram {
    /// Binds to `path`.
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<UnixDatagram> {
        UnixDatagram::from_std(std_net::UnixDatagram::bind(path)?)
    }

    /// Creates an unbound datagram socket.
    pub fn unbound() -> io::Result<UnixDatagram> {
        UnixDatagram::from_std(std_net::UnixDatagram::unbound()?)
    }

    /// Creates an unnamed pair of connected datagram sockets.
    pub fn pair() -> io::Result<(UnixDatagram, UnixDatagram)> {
        let (a, b) = std_net::UnixDatagram::pair()?;
        Ok((UnixDatagram::from_std(a)?, UnixDatagram::from_std(b)?))
    }

    /// Registers a `std` datagram socket with the reactor (non-blocking).
    pub fn from_std(socket: std_net::UnixDatagram) -> io::Result<UnixDatagram> {
        socket.set_nonblocking(true)?;
        Ok(UnixDatagram {
            inner: register(socket)?,
        })
    }

    /// Deregisters and returns the inner `std` socket.
    pub fn into_std(self) -> io::Result<std_net::UnixDatagram> {
        // SAFETY: we own the fd, just released from the reactor.
        Ok(unsafe { std_net::UnixDatagram::from_raw_fd(self.inner.into_raw_fd()?) })
    }

    /// Connects to `path`.
    pub fn connect<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        with_std::<std_net::UnixDatagram, _>(self.inner.as_raw_fd(), |s| s.connect(path))
    }

    /// Returns the local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        with_std::<std_net::UnixDatagram, _>(self.inner.as_raw_fd(), |s| s.local_addr())
            .map(SocketAddr::from)
    }

    /// Returns the peer address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        with_std::<std_net::UnixDatagram, _>(self.inner.as_raw_fd(), |s| s.peer_addr())
            .map(SocketAddr::from)
    }

    /// Returns any pending `SO_ERROR`.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        with_std::<std_net::UnixDatagram, _>(self.inner.as_raw_fd(), |s| s.take_error())
    }

    /// Sends on a connected socket.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        let fd = self.inner.as_raw_fd();
        self.inner
            .async_io(Interest::WRITABLE, || {
                with_std::<std_net::UnixDatagram, _>(fd, |s| s.send(buf))
            })
            .await
    }

    /// Receives on a connected socket.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let fd = self.inner.as_raw_fd();
        self.inner
            .async_io(Interest::READABLE, || {
                with_std::<std_net::UnixDatagram, _>(fd, |s| s.recv(buf))
            })
            .await
    }

    /// Sends to `target`.
    pub async fn send_to<P: AsRef<Path>>(&self, buf: &[u8], target: P) -> io::Result<usize> {
        let fd = self.inner.as_raw_fd();
        let target = target.as_ref();
        self.inner
            .async_io(Interest::WRITABLE, || {
                with_std::<std_net::UnixDatagram, _>(fd, |s| s.send_to(buf, target))
            })
            .await
    }

    /// Receives, returning the sender address.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let fd = self.inner.as_raw_fd();
        let (n, addr) = self
            .inner
            .async_io(Interest::READABLE, || {
                with_std::<std_net::UnixDatagram, _>(fd, |s| s.recv_from(buf))
            })
            .await?;
        Ok((n, SocketAddr::from(addr)))
    }
}

impl fmt::Debug for UnixDatagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixDatagram")
            .field("fd", &self.inner.as_raw_fd())
            .finish()
    }
}

impl AsRawFd for UnixDatagram {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsFd for UnixDatagram {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the fd is owned by `self.inner` for `self`'s lifetime.
        unsafe { BorrowedFd::borrow_raw(self.as_raw_fd()) }
    }
}
