//! Asynchronous name resolution for `wasm32-unknown-emscripten`.
//!
//! Under `-sNODERAWSOCKETS` a synchronous `getaddrinfo` of a real hostname
//! returns `EAI_AGAIN` (there is no blocking resolver on the single host
//! thread), so the `spawn_blocking` + `std::net` path the other targets use
//! never resolves. Instead emscripten exposes an async getaddrinfo:
//! `emscripten_dns_lookup_async` kicks off a `node:dns` resolution and returns a
//! pollable fd that becomes readable once it completes; `emscripten_dns_lookup_result`
//! then yields the `addrinfo` list. We drive that fd's readiness through the
//! reactor with [`AsyncFd`], so resolution yields to the host event loop instead
//! of blocking it.

use crate::io::unix::AsyncFd;
use crate::io::Interest;

use std::ffi::CString;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::vec;

extern "C" {
    // Async getaddrinfo: same (node, service, hint) inputs as getaddrinfo;
    // returns a pollable fd readable once resolution completes, or -1.
    fn emscripten_dns_lookup_async(
        node: *const c_char,
        service: *const c_char,
        hint: *const libc::addrinfo,
    ) -> c_int;
    // Reads the outcome once the fd is readable: 0 on success (writing the
    // addrinfo list head to `*res`, freed with `freeaddrinfo`), else an EAI_*
    // code (EAI_AGAIN if not yet complete).
    fn emscripten_dns_lookup_result(fd: c_int, res: *mut *mut libc::addrinfo) -> c_int;
}

/// Resolve `host` to a list of `SocketAddr`s carrying `port`, via emscripten's
/// async DNS. The returned addresses' ports come from the addrinfo list (0 when
/// no service was requested), so we stamp `port` onto each.
pub(crate) async fn resolve(host: String, port: u16) -> io::Result<vec::IntoIter<SocketAddr>> {
    let cname = CString::new(host)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "host contained a NUL byte"))?;

    // SAFETY: `cname` is a valid C string for the duration of the call; a null
    // service/hint requests the defaults.
    let fd = unsafe { emscripten_dns_lookup_async(cname.as_ptr(), ptr::null(), ptr::null()) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    // From here `fd` must be closed on every path. AsyncFd does not own it.
    let result = drive_lookup(fd, port).await;
    // SAFETY: `fd` is the descriptor we own; the AsyncFd registered on it is
    // dropped inside `drive_lookup` before we get here.
    unsafe { libc::close(fd) };
    result
}

async fn drive_lookup(fd: RawFd, port: u16) -> io::Result<vec::IntoIter<SocketAddr>> {
    {
        // Wait for the lookup fd to become readable through the reactor, then
        // drop the registration before reading the result.
        let afd = AsyncFd::with_interest(fd, Interest::READABLE)?;
        afd.readable().await?.retain_ready();
    }

    let mut res: *mut libc::addrinfo = ptr::null_mut();
    // SAFETY: `fd` is readable (resolution complete); `res` receives the list head.
    let rc = unsafe { emscripten_dns_lookup_result(fd, &mut res) };
    if rc != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("emscripten_dns_lookup_result failed: EAI {rc}"),
        ));
    }

    let mut addrs = Vec::new();
    let mut cur = res;
    while !cur.is_null() {
        // SAFETY: `cur` walks the non-null nodes of the addrinfo list.
        let ai = unsafe { &*cur };
        if let Some(addr) = sockaddr_to_socketaddr(ai.ai_addr, ai.ai_family, port) {
            addrs.push(addr);
        }
        cur = ai.ai_next;
    }
    // SAFETY: `res` is the list head minted by the lookup; free the whole chain.
    unsafe { libc::freeaddrinfo(res) };

    Ok(addrs.into_iter())
}

/// Convert an `addrinfo` address into a `SocketAddr` with `port`, or `None` for
/// an unsupported family or a null pointer.
fn sockaddr_to_socketaddr(sa: *const libc::sockaddr, family: c_int, port: u16) -> Option<SocketAddr> {
    if sa.is_null() {
        return None;
    }
    match family {
        libc::AF_INET => {
            // SAFETY: family is AF_INET, so `sa` points to a `sockaddr_in`.
            let sin = unsafe { &*(sa as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            // SAFETY: family is AF_INET6, so `sa` points to a `sockaddr_in6`.
            let sin6 = unsafe { &*(sa as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Some(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)))
        }
        _ => None,
    }
}
