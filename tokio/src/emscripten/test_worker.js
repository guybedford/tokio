// JS library for the tokio emscripten worker-thread primitive.
//
// The parent calls `__tokio_emscripten_block_in_worker`, which allocates a
// SharedArrayBuffer, spawns a Node `worker_threads.Worker`, and `Atomics.wait`s
// on the SAB until the worker notifies; it then copies the outcome to
// `outcome_ptr` and returns the status. SAB layout (Int32Array):
//
//   [0]      ready    — 0 = running, 1 = parent may read
//   [1]      status   — 0 = ok, 101 = panic, other = unexpected exit
//   [2]      msg len   — UTF-8 byte count in [3..]
//   [3..N]   message   — UTF-8 panic rendering, up to (byteLength - 12)
//
// The worker (Rust) calls `__tokio_emscripten_worker_notify_{done,failure}`,
// which write the SAB and `Atomics.notify`. `Module.onAbort` covers aborts that
// bypass our panic hook so the parent never hangs.

mergeInto(LibraryManager.library, {
    __tokio_emscripten_block_in_worker__sig: 'iiii',
    __tokio_emscripten_block_in_worker: function(fnIndex, outcomePtr, outcomeCap) {
        if (typeof require !== 'function') {
            throw new Error("tokio: emscripten worker-thread primitive currently requires Node.js");
        }
        const { Worker } = require('node:worker_threads');

        // 12-byte header + payload matching Rust's OUTCOME_BUFFER_CAPACITY; the
        // copy back is bounded by outcomeCap regardless.
        const PAYLOAD_CAP = 16 * 1024;
        const HEADER_BYTES = 12;
        const sab = new SharedArrayBuffer(HEADER_BYTES + PAYLOAD_CAP);
        const i32 = new Int32Array(sab);
        const u8 = new Uint8Array(sab);

        // The runner sets TOKIO_EMSCRIPTEN_MODULE_PATH to this binary's `.js`
        // factory so the worker can re-import it.
        const scriptPath = process.env.TOKIO_EMSCRIPTEN_MODULE_PATH || process.argv[1];

        // Worker bootstrap source (IIFE so async setup is awaitable).
        const workerSource =
            "const { workerData } = require('node:worker_threads');\n" +
            "const { pathToFileURL } = require('node:url');\n" +
            "const i32 = new Int32Array(workerData.sab);\n" +
            "const u8 = new Uint8Array(workerData.sab);\n" +
            "const notifyRaw = (status, payloadBytes) => {\n" +
            "  if (Atomics.load(i32, 0) !== 0) return; // already notified\n" +
            "  const cap = u8.length - 12;\n" +
            "  const len = Math.min(payloadBytes ? payloadBytes.length : 0, cap);\n" +
            "  if (len > 0) u8.set(payloadBytes.subarray(0, len), 12);\n" +
            "  Atomics.store(i32, 1, status);\n" +
            "  Atomics.store(i32, 2, len);\n" +
            "  Atomics.store(i32, 0, 1);\n" +
            "  Atomics.notify(i32, 0, 1);\n" +
            "};\n" +
            "(async () => {\n" +
            "  try {\n" +
            "    const factoryUrl = pathToFileURL(workerData.scriptPath).href;\n" +
            "    const factory = (await import(factoryUrl)).default;\n" +
            "    const Module = await factory({\n" +
            "      noInitialRun: true,\n" +
            "      tokioEmscriptenWorkerSab: workerData.sab,\n" +
            "      tokioEmscriptenWorkerNotify: notifyRaw,\n" +
            "      onAbort: () => notifyRaw(102, null),\n" +
            // Worker stdout/stderr is dropped by default (the parent already has
            // the panic info, and it would interleave with libtest output);
            // `TOKIO_EMSCRIPTEN_WORKER_VERBOSE=1` re-enables it.
            "      print: process.env.TOKIO_EMSCRIPTEN_WORKER_VERBOSE === '1' ? ((s) => process._rawDebug(s)) : (() => {}),\n" +
            "      printErr: process.env.TOKIO_EMSCRIPTEN_WORKER_VERBOSE === '1' ? ((s) => process._rawDebug(s)) : (() => {}),\n" +
            "    });\n" +
            "    Module.___tokio_emscripten_worker_invoke(workerData.fnIndex);\n" +
            "  } catch (e) {\n" +
            "    const enc = new TextEncoder();\n" +
            "    notifyRaw(103, enc.encode('worker-shim error: ' + (e.stack || String(e))));\n" +
            "  }\n" +
            "})();\n";

        // `TOKIO_EMSCRIPTEN_INSPECT=1` opens a Node inspector (port 9229) in the
        // worker, paused at entry; attach via `chrome://inspect`.
        const inspect = process.env.TOKIO_EMSCRIPTEN_INSPECT === '1';
        const inspectorBootstrap = inspect
            ? "require('node:inspector').open(9229, '127.0.0.1', true);\n"
            : "";

        const worker = new Worker(inspectorBootstrap + workerSource, {
            eval: true,
            workerData: { scriptPath, sab, fnIndex },
        });

        // Parent-side notify fallback. The 'error'/'exit' callbacks can't run
        // while we're in Atomics.wait, so they fire before the wait or after the
        // watchdog; either way, synthesize a status if not yet notified.
        const parentNotify = (status, payload) => {
            if (Atomics.load(i32, 0) !== 0) return;
            const enc = new TextEncoder();
            const bytes = payload ? enc.encode(payload) : null;
            const cap = u8.length - 12;
            const len = bytes ? Math.min(bytes.length, cap) : 0;
            if (len > 0) u8.set(bytes.subarray(0, len), 12);
            Atomics.store(i32, 1, status);
            Atomics.store(i32, 2, len);
            Atomics.store(i32, 0, 1);
            Atomics.notify(i32, 0, 1);
        };

        worker.on('error', (e) => {
            parentNotify(104, 'worker error: ' + (e && (e.stack || String(e))));
        });
        worker.on('exit', (code) => {
            if (code !== 0) {
                parentNotify(105, 'worker exited with code ' + code);
            }
        });

        // Watchdog: a finite wait so the parent unblocks even if the worker
        // hangs before notifying. 60s >> any test; override with
        // TOKIO_EMSCRIPTEN_WATCHDOG_MS.
        const WATCHDOG_MS = Number(process.env.TOKIO_EMSCRIPTEN_WATCHDOG_MS) || 60000;
        const waitResult = Atomics.wait(i32, 0, 0, WATCHDOG_MS);
        if (waitResult === 'timed-out' && Atomics.load(i32, 0) === 0) {
            parentNotify(106, 'worker did not respond within ' + (WATCHDOG_MS / 1000) + 's');
        }

        // Copy outcome into the parent's `OutcomeRaw` (status i32, len i32, then
        // inline message bytes).
        const status = i32[1];
        const len = Math.min(i32[2], outcomeCap - 8);
        HEAP32[outcomePtr >> 2] = status;
        HEAP32[(outcomePtr + 4) >> 2] = len;
        if (len > 0) {
            HEAPU8.set(u8.subarray(12, 12 + len), outcomePtr + 8);
        }
        return status;
    },

    __tokio_emscripten_worker_notify_done__sig: 'vi',
    __tokio_emscripten_worker_notify_done: function(status) {
        const notify = Module.tokioEmscriptenWorkerNotify;
        if (!notify) {
            throw new Error("tokio: notify_done called outside a worker context");
        }
        notify(status, null);
    },

    // Synchronous `debugger;` trampoline. Under TOKIO_EMSCRIPTEN_INSPECT=1 it's
    // the next pause point after Resume; since it's called from the shim about
    // to invoke the test, one "Step Into" lands in the test body. Else a no-op.
    __tokio_emscripten_debugger__sig: 'v',
    __tokio_emscripten_debugger: function() { debugger; },

    // Stderr write via `process._rawDebug` (synchronous, uncaptured), bypassing
    // the worker's silenced `printErr`. Useful for tracing a hanging test.
    __tokio_emscripten_rawdebug__sig: 'vii',
    __tokio_emscripten_rawdebug: function(msgPtr, msgLen) {
        if (msgLen <= 0) return;
        const bytes = HEAPU8.subarray(msgPtr, msgPtr + msgLen);
        const s = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
        if (typeof process !== 'undefined' && typeof process._rawDebug === 'function') {
            process._rawDebug('[tokio-emscripten] ' + s);
        } else {
            console.error('[tokio-emscripten] ' + s); // browser fallback
        }
    },

    __tokio_emscripten_worker_notify_failure__sig: 'viii',
    __tokio_emscripten_worker_notify_failure: function(status, msgPtr, msgLen) {
        const notify = Module.tokioEmscriptenWorkerNotify;
        if (!notify) {
            throw new Error("tokio: notify_failure called outside a worker context");
        }
        // Copy from wasm memory before notifying: `notify` writes the SAB (via
        // `Uint8Array.set`, which copies) in this same JS turn, before the parent
        // reads it.
        const bytes = msgLen > 0 ? HEAPU8.subarray(msgPtr, msgPtr + msgLen) : null;
        notify(status, bytes);
    },
});
