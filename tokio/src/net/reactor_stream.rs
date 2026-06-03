//! Shared plumbing for the emscripten socket types.
//!
//! `TcpStream`, `UnixStream`, `UnixListener`, and `UnixDatagram` on emscripten
//! are all a [`PollEvented`] over the reactor [`Source`], differing only in how
//! the fd is created (`connect`/`bind`/`socketpair`) and the address/`accept`
//! glue. This collects the common readiness + byte-I/O surface so each public
//! type is a thin shell. Internal and emscripten-only; never exposed publicly.

use crate::io::{Interest, PollEvented, ReadBuf, Ready};
use crate::runtime::io::Source;

use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::task::{Context, Poll};

/// A reactor-registered socket fd: the common core of every emscripten socket
/// type.
pub(crate) struct ReactorStream {
    io: PollEvented<Source>,
}

impl ReactorStream {
    /// Registers an already-non-blocking `fd` with the reactor, taking ownership
    /// (the fd is closed on drop unless reclaimed via [`into_raw_fd`]).
    ///
    /// [`into_raw_fd`]: Self::into_raw_fd
    pub(crate) fn from_raw_fd(fd: RawFd) -> io::Result<ReactorStream> {
        Ok(ReactorStream {
            io: PollEvented::new(Source::from_raw_fd(fd))?,
        })
    }

    /// Deregisters from the reactor and releases the fd without closing it.
    pub(crate) fn into_raw_fd(self) -> io::Result<RawFd> {
        Ok(self.io.into_inner()?.into_raw_fd())
    }

    pub(crate) fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }

    // ===== readiness =====

    pub(crate) async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        Ok(self.io.registration().readiness(interest).await?.ready)
    }

    pub(crate) fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.registration().poll_read_ready(cx).map_ok(|_| ())
    }

    pub(crate) fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.registration().poll_write_ready(cx).map_ok(|_| ())
    }

    // ===== generic readiness-gated ops (accept, send_to, recv_from, …) =====

    pub(crate) fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        self.io.registration().try_io(interest, f)
    }

    pub(crate) async fn async_io<R>(
        &self,
        interest: Interest,
        f: impl FnMut() -> io::Result<R>,
    ) -> io::Result<R> {
        self.io.registration().async_io(interest, f).await
    }

    pub(crate) fn poll_read_io<R>(
        &self,
        cx: &mut Context<'_>,
        f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        self.io.registration().poll_read_io(cx, f)
    }

    // ===== stream byte I/O =====

    pub(crate) fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || (&*self.io).read(buf))
    }

    pub(crate) fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.try_io(Interest::READABLE, || (&*self.io).read_vectored(bufs))
    }

    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || (&*self.io).write(buf))
    }

    pub(crate) fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.try_io(Interest::WRITABLE, || (&*self.io).write_vectored(bufs))
    }

    cfg_io_util! {
        pub(crate) fn try_read_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            self.try_io(Interest::READABLE, || {
                let dst = buf.chunk_mut();
                let dst = unsafe {
                    &mut *(dst as *mut _ as *mut [std::mem::MaybeUninit<u8>] as *mut [u8])
                };
                // SAFETY: `read` fills `n` initialized bytes, which we commit.
                let n = (&*self.io).read(dst)?;
                unsafe {
                    buf.advance_mut(n);
                }
                Ok(n)
            })
        }
    }

    /// # Safety
    /// The same contract as [`PollEvented::poll_read`]: `recv` only writes into
    /// the buffer's unfilled region.
    pub(crate) unsafe fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: forwarded from the caller's contract.
        unsafe { self.io.poll_read(cx, buf) }
    }

    pub(crate) fn poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.io.poll_write(cx, buf)
    }

    pub(crate) fn poll_write_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.io.poll_write_vectored(cx, bufs)
    }
}
