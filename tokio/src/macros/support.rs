cfg_macros! {
    pub use crate::future::maybe_done::maybe_done;

    pub use std::future::poll_fn;

    pub use crate::macros::join::{BiasedRotator, Rotator, RotatorSelect, SelectNormal, SelectBiased};

    #[doc(hidden)]
    pub fn thread_rng_n(n: u32) -> u32 {
        crate::runtime::context::thread_rng_n(n)
    }

    cfg_coop! {
        #[doc(hidden)]
        #[inline]
        pub fn poll_budget_available(cx: &mut Context<'_>) -> Poll<()> {
            crate::task::coop::poll_budget_available(cx)
        }
    }

    cfg_not_coop! {
        #[doc(hidden)]
        #[inline]
        pub fn poll_budget_available(_: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
}

pub use std::future::{Future, IntoFuture};
pub use std::pin::Pin;
pub use std::result::Result;
pub use std::task::{ready, Context, Poll};

// `#[tokio::test]` on emscripten can't block the host event loop, so its
// expansion runs the test body on a Node worker and blocks the test thread on
// it (see `crate::emscripten::test_worker`).
#[cfg(all(target_os = "emscripten", feature = "rt"))]
#[doc(hidden)]
pub use crate::emscripten::test_worker::{
    run_test as emscripten_run_test, run_test_body as emscripten_run_test_body,
    TestOutput as EmscriptenTestOutput,
};
