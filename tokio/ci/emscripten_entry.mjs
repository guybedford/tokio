// Cargo test runner for wasm32-unknown-emscripten. The binary is built
// `-sMODULARIZE -sEXPORT_ES6`, so the `.js` exports a factory; this loads it,
// runs it with the libtest args, and propagates the exit code. Also sets
// `TOKIO_EMSCRIPTEN_MODULE_PATH` so worker shims can re-import the same factory.
//
// Driven by cargo via CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUNNER.

import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

const [, , binaryPath, ...libtestArgs] = process.argv;
if (!binaryPath) {
    console.error("emscripten_entry.mjs: missing test binary path");
    process.exit(2);
}

const resolvedBinary = resolve(binaryPath);
process.env.TOKIO_EMSCRIPTEN_MODULE_PATH = resolvedBinary;

const factory = (await import(pathToFileURL(resolvedBinary).href)).default;

await factory({
    arguments: libtestArgs,
    onExit(code) {
        process.exit(code);
    },
});
