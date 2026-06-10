#![cfg(not(loom))]

//! TCP/UDP/Unix bindings for `tokio`.
//!
//! This module contains the TCP/UDP/Unix networking types, similar to the standard
//! library, which can be used to implement networking protocols.
//!
//! # Organization
//!
//! * [`TcpListener`] and [`TcpStream`] provide functionality for communication over TCP
//! * [`UdpSocket`] provides functionality for communication over UDP
//! * [`UnixListener`] and [`UnixStream`] provide functionality for communication over a
//!   Unix Domain Stream Socket **(available on Unix only)**
//! * [`UnixDatagram`] provides functionality for communication
//!   over Unix Domain Datagram Socket **(available on Unix only)**
//! * [`tokio::net::unix::pipe`] for FIFO pipes **(available on Unix only)**
//! * [`tokio::net::windows::named_pipe`] for Named Pipes **(available on Windows only)**
//!
//! For IO resources not available in `tokio::net`, you can use [`AsyncFd`].
//!
//! [`TcpListener`]: TcpListener
//! [`TcpStream`]: TcpStream
//! [`UdpSocket`]: UdpSocket
//! [`UnixListener`]: UnixListener
//! [`UnixStream`]: UnixStream
//! [`UnixDatagram`]: UnixDatagram
//! [`tokio::net::unix::pipe`]: unix::pipe
//! [`tokio::net::windows::named_pipe`]: windows::named_pipe
//! [`AsyncFd`]: crate::io::unix::AsyncFd

mod addr;
pub use addr::ToSocketAddrs;

// Name resolution is `std::net` + `spawn_blocking`, not mio, and rides
// emscripten's `getaddrinfo` — so it's available there too, as is the
// `to_socket_addrs` resolver shim (used by the emscripten `TcpStream` below).
cfg_net! {
    mod lookup_host;
    pub use lookup_host::lookup_host;

    cfg_not_wasip1! {
        pub(crate) use addr::to_socket_addrs;
    }
}

// The mio/socket2-backed socket types are not implemented on emscripten; it gets
// an `AsyncFd`-backed `TcpStream` over the sockfs reactor instead.
cfg_net_not_emscripten! {
    pub mod tcp;
    pub use tcp::listener::TcpListener;
    pub use tcp::stream::TcpStream;
    cfg_not_wasip1! {
        pub use tcp::socket::TcpSocket;

        mod udp;
        #[doc(inline)]
        pub use udp::UdpSocket;
    }
}

// Shared plumbing for the emscripten socket types (TCP + Unix).
#[cfg(all(feature = "net", target_os = "emscripten"))]
mod emscripten;
#[cfg(all(feature = "net", target_os = "emscripten"))]
mod reactor_stream;
#[cfg(all(feature = "net", target_os = "emscripten"))]
pub use emscripten::TcpStream;

cfg_net_unix_not_emscripten! {
    pub mod unix;
    pub use unix::datagram::socket::UnixDatagram;
    pub use unix::listener::UnixListener;
    pub use unix::stream::UnixStream;
    pub use unix::socket::UnixSocket;
}

// emscripten gets the reactor-backed `unix` socket types (no mio); same public
// API minus `UnixSocket`/`pipe`, which aren't ported yet.
#[cfg(all(feature = "net", target_os = "emscripten"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "net"))))]
pub mod unix;
#[cfg(all(feature = "net", target_os = "emscripten"))]
pub use unix::emscripten::{UnixDatagram, UnixListener, UnixStream};

cfg_net_windows! {
    pub mod windows;
}
