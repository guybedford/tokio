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

cfg_net! {
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

// Async name resolution over emscripten's `emscripten_dns_lookup_async`, used by
// the `ToSocketAddrs` string impls in `addr` (the sync `getaddrinfo` path is
// `EAI_AGAIN` for hostnames under `-sNODERAWSOCKETS`).
#[cfg(all(feature = "net", target_os = "emscripten"))]
pub(crate) mod emscripten_dns;

cfg_net_unix! {
    pub mod unix;
    pub use unix::listener::UnixListener;
    pub use unix::stream::UnixStream;
}

cfg_net_unix! {
    pub use unix::socket::UnixSocket;
}

// Emscripten's node-backed AF_UNIX is stream-only: no datagram sockets.
cfg_net_unix! {
    #[cfg(not(target_os = "emscripten"))]
    pub use unix::datagram::socket::UnixDatagram;
}

cfg_net_windows! {
    pub mod windows;
}
