//! Host-resolved DNS on `wasm32-unknown-emscripten` without threads.
//!
//! Under `-sNODERAWSOCKETS`, `getaddrinfo` is a `node:dns` lookup that only a
//! stack able to wait on the host loop (JSPI, or a proxied `pthread`) can
//! complete, so the blocking pool's `std::net::ToSocketAddrs` cannot serve a
//! hosted event loop. The lookup instead goes to the host with a callback,
//! through an `EM_JS` binding carried here as an exported `__em_js__` static
//! (Emscripten reads the `(params)<::>{ body }` source from the Wasm at link
//! time) until Emscripten itself provides `emscripten_dns_lookup_callback`.
//! A Rust static cannot declare JavaScript library dependencies, so the body
//! uses only the runtime's `HEAPU8` and `wasmExports`, and the runtime hold is
//! taken here in Rust. It resolves through Emscripten's own `node:dns` helper
//! where the build has one; without it, `getaddrinfo` is Emscripten's
//! synchronous fake DNS and the caller keeps using it.
//!
//! Each address is written as text into the request's buffer and parsed on
//! this side; completion wakes the waiting task.

use std::ffi::c_void;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::vec;

type AddrCallback = extern "C" fn(*mut c_void, usize);
type DoneCallback = extern "C" fn(*mut c_void, isize);

/// Room for any textual address (at most 45 bytes) and the error codes
/// echoed back.
const BUF_LEN: usize = 64;

const SRC: &str = "(name, len, buf, cap, on_addr, on_done, arg)<::>{\
    if (typeof nodeSockHelpers != 'object') return 0;\
    var table = wasmExports['__indirect_function_table'];\
    var write = (s) => {\
        for (var i = 0; i < s.length; i++) HEAPU8[buf + i] = s.charCodeAt(i);\
        return s.length;\
    };\
    var fail = (code) => table.get(on_done)(arg, -write(String(code).slice(0, cap)));\
    var host = new TextDecoder().decode(HEAPU8.subarray(name, name + len));\
    try {\
        nodeSockHelpers.getDns().lookup(host, { all: true }, (err, addrs) => {\
            if (err) return fail(err.code || 'EAI_FAIL');\
            for (var a of addrs) {\
                if (a.address.length <= cap) table.get(on_addr)(arg, write(a.address));\
            }\
            table.get(on_done)(arg, 0);\
        });\
    } catch (e) {\
        fail(e.code || 'EAI_FAIL');\
    }\
    return 1;\
}";

#[used]
#[no_mangle]
#[allow(non_upper_case_globals)]
static __em_js__tokio_dns_lookup: [u8; SRC.len() + 1] = {
    let src = SRC.as_bytes();
    let mut out = [0u8; SRC.len() + 1];
    let mut i = 0;
    while i < src.len() {
        out[i] = src[i];
        i += 1;
    }
    out
};

// The explicit import module lets `wasm-ld` leave the symbol undefined for
// Emscripten to bind, as `EM_JS`'s `import_name` attribute does.
#[link(wasm_import_module = "env")]
extern "C" {
    /// Resolves `name` (not NUL-terminated) on the host: `on_addr(arg, len)`
    /// once per address written as text into `buf` (`cap` bytes), then
    /// `on_done(arg, status)`, `status < 0` an error whose code of `-status`
    /// bytes is in `buf`. Completion may be synchronous. Returns 0, calling
    /// neither, where the host does not resolve names.
    fn tokio_dns_lookup(
        name: *const u8,
        len: usize,
        buf: *mut u8,
        cap: usize,
        on_addr: AddrCallback,
        on_done: DoneCallback,
        arg: *mut c_void,
    ) -> i32;
}

extern "C" {
    fn emscripten_runtime_keepalive_push();
    fn emscripten_runtime_keepalive_pop();
}

#[derive(Debug)]
struct Shared {
    port: u16,
    addrs: Vec<SocketAddr>,
    done: Option<io::Result<()>>,
    waker: Option<Waker>,
    /// Written by the host before each callback, read under the lock within it.
    buf: [u8; BUF_LEN],
}

/// A lookup in flight on the host. Dropping it lets the lookup complete
/// unobserved.
#[derive(Debug)]
pub(crate) struct Lookup {
    shared: Arc<Mutex<Shared>>,
}

impl Lookup {
    /// Starts a lookup on the host, or `None` where the build has no
    /// `node:dns` resolver and `getaddrinfo` answers synchronously.
    pub(crate) fn start(host: &str, port: u16) -> Option<Lookup> {
        let shared = Arc::new(Mutex::new(Shared {
            port,
            addrs: Vec::new(),
            done: None,
            waker: None,
            buf: [0; BUF_LEN],
        }));
        let buf = shared.lock().unwrap().buf.as_mut_ptr();
        // The linker pulls the rlib member holding the binding's source only
        // for a reference.
        std::hint::black_box(&__em_js__tokio_dns_lookup);
        // The host's reference, taken back in `on_done`.
        let arg = Arc::into_raw(shared.clone()) as *mut c_void;
        // SAFETY: `host` is read before the call returns; `buf` is inside the
        // `Arc` the host holds until `on_done`, where the hold is released.
        let started = unsafe {
            emscripten_runtime_keepalive_push();
            tokio_dns_lookup(
                host.as_ptr(),
                host.len(),
                buf,
                BUF_LEN,
                on_addr,
                on_done,
                arg,
            )
        };
        if started == 0 {
            // SAFETY: not started, so the host kept neither `arg` nor the hold.
            unsafe {
                drop(Arc::from_raw(arg as *const Mutex<Shared>));
                emscripten_runtime_keepalive_pop();
            }
            return None;
        }
        Some(Lookup { shared })
    }
}

impl Future for Lookup {
    type Output = io::Result<vec::IntoIter<SocketAddr>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.shared.lock().unwrap();
        match shared.done.take() {
            Some(Ok(())) => Poll::Ready(Ok(std::mem::take(&mut shared.addrs).into_iter())),
            Some(Err(err)) => Poll::Ready(Err(err)),
            None => {
                match &mut shared.waker {
                    Some(waker) => waker.clone_from(cx.waker()),
                    None => shared.waker = Some(cx.waker().clone()),
                }
                Poll::Pending
            }
        }
    }
}

extern "C" fn on_addr(arg: *mut c_void, len: usize) {
    // SAFETY: `arg` is the host's `Arc` from `Lookup::new`, alive until `on_done`.
    let shared = unsafe { &*(arg as *const Mutex<Shared>) };
    let mut shared = shared.lock().unwrap();
    let text = std::str::from_utf8(&shared.buf[..len.min(BUF_LEN)]).unwrap_or("");
    if let Ok(ip) = text.parse::<IpAddr>() {
        let port = shared.port;
        shared.addrs.push(SocketAddr::new(ip, port));
    }
}

extern "C" fn on_done(arg: *mut c_void, status: isize) {
    // SAFETY: the host's `Arc` from `Lookup::new`, released here.
    let shared = unsafe { Arc::from_raw(arg as *const Mutex<Shared>) };
    let waker = {
        let mut shared = shared.lock().unwrap();
        shared.done = Some(if status >= 0 {
            Ok(())
        } else {
            let len = status.unsigned_abs().min(BUF_LEN);
            let code = String::from_utf8_lossy(&shared.buf[..len]).into_owned();
            Err(io::Error::other(format!(
                "failed to lookup address information: {code}"
            )))
        });
        shared.waker.take()
    };
    drop(shared);
    if let Some(waker) = waker {
        waker.wake();
    }
    // SAFETY: pairs with the push in `Lookup::new`.
    unsafe { emscripten_runtime_keepalive_pop() };
}
