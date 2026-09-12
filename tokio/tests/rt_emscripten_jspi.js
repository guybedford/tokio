// JS side of `rt_emscripten_jspi.rs`, linked with `--js-library` in the JSPI
// lane. `tokio_test_reenter*` are promising exports (`-sJSPI_EXPORTS`), so
// calling one from the host starts a sibling activation.
addToLibrary({
  tokio_test_schedule_reenter: (ms) => {
    globalThis.tokioReenter = new Promise((resolve) =>
      setTimeout(() => resolve(wasmExports.tokio_test_reenter()), ms));
  },

  tokio_test_await_reenter__async: true,
  tokio_test_await_reenter__deps: ['$Asyncify'],
  tokio_test_await_reenter: () => Asyncify.handleAsync(() => globalThis.tokioReenter),

  // Plain JS frame between the promising activation and the export, so a
  // suspension inside it has no suspender and throws.
  tokio_test_call_unsuspendable: () => {
    try {
      wasmExports.tokio_test_unsuspendable();
      return 0;
    } catch (e) {
      return e instanceof WebAssembly.SuspendError ? 1 : 2;
    }
  },

  // Sibling promising fibers entered from a microtask while the caller is
  // suspended in a task-issued suspension.
  tokio_test_reenter_await__async: true,
  tokio_test_reenter_await__deps: ['$Asyncify'],
  tokio_test_reenter_await: () => Asyncify.handleAsync(async () => {
    await null;
    return wasmExports.tokio_test_reenter();
  }),
  tokio_test_reenter_local_await__async: true,
  tokio_test_reenter_local_await__deps: ['$Asyncify'],
  tokio_test_reenter_local_await: () => Asyncify.handleAsync(async () => {
    await null;
    return wasmExports.tokio_test_reenter_local();
  }),

  // Set by the reentrant CI lane, whose fibers have their own shadow stacks.
  tokio_test_reentrant_jspi: () =>
    typeof process != 'undefined' && process.env.TOKIO_REENTRANT_JSPI ? 1 : 0,

  tokio_test_reenter_sync_call__async: true,
  tokio_test_reenter_sync_call__deps: ['$Asyncify'],
  tokio_test_reenter_sync_call: () => Asyncify.handleAsync(async () => {
    await null;
    return wasmExports.tokio_test_reenter_sync();
  }),
});
