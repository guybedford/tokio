#![warn(rust_2018_idioms)]
// The schedule/drive contract with no JSPI anywhere. This target is built and
// run WITHOUT `-sJSPI` (its own CI step), so any attempt to suspend the wasm
// stack traps: scheduled roots can progress only through host-loop drives —
// timer re-arms and pick-ups — never through a `block_on` park.
//
// Two hosted runtimes coexist on the thread, each driven by its own host
// turns, with a cross-runtime wake (a oneshot completed on one runtime waking
// a root parked on the other) riding the pick-up latch.
//
// `harness = false`: `main` schedules the roots and returns into the host
// event loop; in-flight roots hold the runtime keepalive until they complete,
// and an `atexit` hook turns "exited without completing" into a hard failure
// rather than a false pass.

#[cfg(all(
    target_os = "emscripten",
    tokio_unstable,
    feature = "rt",
    feature = "time",
    feature = "sync"
))]
mod support {
    pub(crate) mod hosted_runtime;
}

fn main() {
    #[cfg(all(
        target_os = "emscripten",
        tokio_unstable,
        feature = "rt",
        feature = "time",
        feature = "sync"
    ))]
    emscripten::main();
}

#[cfg(all(
    target_os = "emscripten",
    tokio_unstable,
    feature = "rt",
    feature = "time",
    feature = "sync"
))]
mod emscripten {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::runtime::HostedRuntime;

    static COMPLETED_A: AtomicBool = AtomicBool::new(false);
    static COMPLETED_B: AtomicBool = AtomicBool::new(false);

    extern "C" fn verify_completed() {
        let (a, b) = (
            COMPLETED_A.load(Ordering::SeqCst),
            COMPLETED_B.load(Ordering::SeqCst),
        );
        if !a || !b {
            eprintln!("FAIL: scheduled roots did not complete before runtime exit (a={a} b={b})");
            std::process::abort();
        }
        println!("ok: two hosted runtimes completed via host drives (no JSPI)");
    }

    fn hosted() -> &'static HostedRuntime {
        // The runtimes must outlive `main`: their armed host callbacks are
        // what drive the roots. Leak them — the process exits when the roots
        // complete and the keepalives drop.
        Box::leak(Box::new(crate::support::hosted_runtime::hosted_runtime()))
    }

    pub(super) fn main() {
        // SAFETY: standard atexit registration of a C function.
        unsafe { libc::atexit(verify_completed) };

        let rt_a = hosted();
        let rt_b = hosted();

        let (tx, rx) = tokio::sync::oneshot::channel::<u32>();

        rt_a.schedule(
            async move {
                // Cross a real timer deadline: the drive returns
                // `Driven::Timer`, the glue arms a host `setTimeout` for this
                // runtime, and only its callback can resume us.
                tokio::time::sleep(Duration::from_millis(20)).await;

                // Spawned-task progress and a deferred wake (`yield_now`)
                // both ride subsequent drives of the same fixed point.
                let handle = tokio::spawn(async {
                    tokio::task::yield_now().await;
                    7
                });
                let seven = handle.await.unwrap();

                // A second timer after task churn: the re-arm path.
                tokio::time::sleep(Duration::from_millis(5)).await;

                // Wake the root parked on the *other* runtime: the send fires
                // mid-drive of this runtime, so the wake latches a pick-up
                // and B is driven on its own host turn after this drive exits.
                tx.send(seven).unwrap();
            },
            |result| {
                result.unwrap();
                COMPLETED_A.store(true, Ordering::SeqCst);
            },
        );

        rt_b.schedule(
            async move {
                // Interleave B's own timer with A's before parking on the
                // cross-runtime oneshot.
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert_eq!(rx.await.unwrap(), 7);
            },
            |result| {
                result.unwrap();
                COMPLETED_B.store(true, Ordering::SeqCst);
            },
        );

        rt_a.drive();
        rt_b.drive();
        // Fall through into the host event loop: from here on, each runtime
        // advances only on its own timer callbacks and pick-ups.
    }
}
