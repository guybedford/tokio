// Cargo test runner for wasm32-unknown-emscripten. The binary is a plain
// emscripten script, so `node <binary>` alone works; this wrapper only adds
// CI hardening:
//  * stdin detached onto the null device — emscripten's stdin read is a
//    synchronous fd-0 read that would otherwise block node's loop (no JSPI
//    park, no watchdog) whenever a test touches stdin with a terminal or
//    pipe attached;
//  * a deadlock guard (see emscripten_deadlock_guard.cjs);
//  * a watchdog so a binary hanging with a live event loop fails in bounded
//    time instead of stalling the suite.
//
// Driven by cargo via CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUNNER.

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const [, , binaryPath, ...args] = process.argv;
if (!binaryPath) {
    console.error("emscripten_entry.mjs: missing test binary path");
    process.exit(2);
}

const guard = join(dirname(fileURLToPath(import.meta.url)), "emscripten_deadlock_guard.cjs");

const child = spawn(process.execPath, ["--require", guard, binaryPath, ...args], {
    stdio: ["ignore", "inherit", "inherit"],
});

const watchdogMs = Number(process.env.TOKIO_EMSCRIPTEN_TEST_TIMEOUT_MS || 120_000);
const watchdog = setTimeout(() => {
    console.error(`emscripten_entry.mjs: watchdog: ${binaryPath} still running after ${watchdogMs}ms`);
    child.kill();
    process.exitCode = 8;
}, watchdogMs);
watchdog.unref();

child.on("exit", (code, signal) => {
    clearTimeout(watchdog);
    if (process.exitCode === undefined) {
        process.exitCode = code ?? (signal ? 8 : 0);
    }
});
