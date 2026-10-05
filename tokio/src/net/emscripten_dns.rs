//! Name resolution on `wasm32-unknown-emscripten`.
//!
//! The synchronous `getaddrinfo` has nothing to block on there (under
//! `-sNODERAWSOCKETS` a name lookup fails with `EAI_AGAIN`), so the
//! blocking-pool resolver the other targets use never resolves a name.
//! Emscripten's asynchronous `getaddrinfo` instead returns a promise, settled
//! on the calling thread's host loop, whose handler wakes the awaiting task.

use crate::loom::sync::Mutex;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::future::poll_fn;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ptr;
use std::sync::Arc;
use std::task::{Poll, Waker};
use std::vec;

type Promise = *mut c_void;
type PromiseCallback =
    unsafe extern "C-unwind" fn(*mut *mut c_void, *mut c_void, *mut c_void) -> c_int;

const EM_PROMISE_FULFILL: c_int = 0;

extern "C" {
    /// A promise fulfilled with a `getaddrinfo` list, or rejected with the
    /// `EAI_*` code as its reason, on the calling thread, never on the
    /// caller's stack. A pending lookup holds the Emscripten runtime alive.
    fn emscripten_dns_lookup_async(
        name: *const c_char,
        service: *const c_char,
        hints: *const libc::addrinfo,
    ) -> Promise;
    fn emscripten_promise_then(
        promise: Promise,
        on_fulfilled: PromiseCallback,
        on_rejected: Option<PromiseCallback>,
        user_data: *mut c_void,
    ) -> Promise;
    fn emscripten_promise_destroy(promise: Promise);
}

/// A lookup in flight, shared by the awaiting future and the promise
/// handler; neither holds the promise.
struct Lookup {
    port: u16,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    waker: Option<Waker>,
    done: Option<Result<Vec<SocketAddr>, c_int>>,
}

/// Resolves `host` to its addresses, each carrying `port`.
pub(crate) async fn resolve(host: String, port: u16) -> io::Result<vec::IntoIter<SocketAddr>> {
    let name = CString::new(host)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "host contains a NUL byte"))?;
    let lookup = Arc::new(Lookup {
        port,
        state: Mutex::new(State::default()),
    });
    // The handler's reference, reclaimed when it runs; the future may be
    // dropped first.
    let user_data = Arc::into_raw(lookup.clone()) as *mut c_void;
    // SAFETY: `name` is read before the call returns; null service and hints
    // request the defaults. The handler is chained before the promise can
    // settle, and the handles are ours to destroy once chained.
    unsafe {
        let promise = emscripten_dns_lookup_async(name.as_ptr(), ptr::null(), ptr::null());
        let chained = emscripten_promise_then(promise, fulfilled, Some(rejected), user_data);
        emscripten_promise_destroy(chained);
        emscripten_promise_destroy(promise);
    }
    let done = poll_fn(|cx| {
        let mut state = lookup.state.lock();
        match state.done.take() {
            Some(done) => Poll::Ready(done),
            None => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    })
    .await;
    match done {
        Ok(addrs) => Ok(addrs.into_iter()),
        Err(code) => {
            // SAFETY: `gai_strerror` returns a static string.
            let msg = unsafe { CStr::from_ptr(libc::gai_strerror(code)) };
            Err(io::Error::new(
                io::ErrorKind::Other,
                msg.to_string_lossy().into_owned(),
            ))
        }
    }
}

unsafe extern "C-unwind" fn fulfilled(
    _result: *mut *mut c_void,
    user_data: *mut c_void,
    value: *mut c_void,
) -> c_int {
    let res = value as *mut libc::addrinfo;
    // SAFETY: the reference `resolve` leaked for this handler.
    let lookup = unsafe { Arc::from_raw(user_data as *const Lookup) };
    // SAFETY: a list the lookup allocated, walked once and freed.
    let addrs = unsafe { collect(res, lookup.port) };
    // SAFETY: the same list, no longer referenced.
    unsafe { libc::freeaddrinfo(res) };
    complete(&lookup, Ok(addrs));
    EM_PROMISE_FULFILL
}

unsafe extern "C-unwind" fn rejected(
    _result: *mut *mut c_void,
    user_data: *mut c_void,
    value: *mut c_void,
) -> c_int {
    // SAFETY: the reference `resolve` leaked for this handler.
    let lookup = unsafe { Arc::from_raw(user_data as *const Lookup) };
    complete(&lookup, Err(value as isize as c_int));
    EM_PROMISE_FULFILL
}

fn complete(lookup: &Lookup, done: Result<Vec<SocketAddr>, c_int>) {
    let waker = {
        let mut state = lookup.state.lock();
        state.done = Some(done);
        state.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// The addresses of an `addrinfo` list, each with `port`.
///
/// # Safety
///
/// `ai` is null or the head of a list `getaddrinfo` produced.
unsafe fn collect(mut ai: *const libc::addrinfo, port: u16) -> Vec<SocketAddr> {
    let mut addrs = Vec::new();
    while !ai.is_null() {
        // SAFETY: a non-null node of the list, whose `ai_addr` matches `ai_family`.
        let a = unsafe { &*ai };
        match a.ai_family {
            libc::AF_INET => {
                let sin = unsafe { &*(a.ai_addr as *const libc::sockaddr_in) };
                let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
                addrs.push(SocketAddr::new(ip.into(), port));
            }
            libc::AF_INET6 => {
                let sin6 = unsafe { &*(a.ai_addr as *const libc::sockaddr_in6) };
                let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                addrs.push(SocketAddr::new(ip.into(), port));
            }
            _ => {}
        }
        ai = a.ai_next;
    }
    addrs
}
