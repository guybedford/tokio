// Preloaded (--require) into each emscripten test binary's node process. A
// clean emscripten exit (EXIT_RUNTIME) sets `process.exitCode` and lets the
// loop drain — so at beforeExit, an *unset* exit code means node's loop
// drained while `main` was still suspended (e.g. a JSPI `block_on` parked
// forever with no wake source): a deadlock, which must fail rather than
// silently exit 0.
process.on("beforeExit", () => {
    if (process.exitCode === undefined) {
        console.error("emscripten test: event loop drained before main exited (deadlock?)");
        process.exit(7);
    }
});
