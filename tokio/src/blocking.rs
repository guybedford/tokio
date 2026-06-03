// Native `rt`: the threadpool-backed `spawn_blocking`. Emscripten has no OS
// threads, so it gets the shim below.
#[cfg(all(feature = "rt", not(target_os = "emscripten")))]
pub(crate) use crate::runtime::spawn_blocking;
#[cfg(all(feature = "rt", not(target_os = "emscripten")))]
pub(crate) use crate::task::JoinHandle;

#[cfg(all(feature = "rt", not(target_os = "emscripten"), feature = "fs"))]
#[allow(unused_imports)]
pub(crate) use crate::runtime::spawn_mandatory_blocking;

// Emscripten + `rt`: no threadpool, so run the closure as an ordinary spawned
// task and return its `JoinHandle` (callers like `fs`/`io-std` are unchanged;
// emscripten's blocking syscalls complete synchronously when polled). Running it
// inside the task gives it a task context, so `task::id()` etc. match native.
cfg_rt_emscripten! {
    pub(crate) use crate::task::JoinHandle;

    pub(crate) fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        crate::runtime::Handle::current().spawn(async move { f() })
    }

    cfg_fs! {
        #[allow(dead_code)]
        pub(crate) fn spawn_mandatory_blocking<F, R>(f: F) -> Option<JoinHandle<R>>
        where
            F: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            Some(spawn_blocking(f))
        }
    }
}

cfg_not_rt! {
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    pub(crate) fn spawn_blocking<F, R>(_f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        assert_send_sync::<JoinHandle<std::cell::Cell<()>>>();
        panic!("requires the `rt` Tokio feature flag")
    }

    cfg_fs! {
        pub(crate) fn spawn_mandatory_blocking<F, R>(_f: F) -> Option<JoinHandle<R>>
        where
            F: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            panic!("requires the `rt` Tokio feature flag")
        }
    }

    pub(crate) struct JoinHandle<R> {
        _p: std::marker::PhantomData<R>,
    }

    unsafe impl<T: Send> Send for JoinHandle<T> {}
    unsafe impl<T: Send> Sync for JoinHandle<T> {}

    impl<R> Future for JoinHandle<R> {
        type Output = Result<R, std::io::Error>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            unreachable!()
        }
    }

    impl<T> fmt::Debug for JoinHandle<T>
    where
        T: fmt::Debug,
    {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt.debug_struct("JoinHandle").finish()
        }
    }

    fn assert_send_sync<T: Send + Sync>() {
    }
}
