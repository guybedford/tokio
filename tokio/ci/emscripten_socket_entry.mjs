// Cargo test runner for the emscripten socket test. Like `emscripten_entry.mjs`,
// but first stands up a WebSocket echo server (sockfs emulates TCP over
// WebSockets) so the in-wasm test exercises the reactor's `connect`/`send`/`recv`
// against a real peer, without hosting a server itself (sockfs `accept` is
// unsupported). The echo server runs in its own worker, since the tokio test's
// worker blocks the main thread on `Atomics.wait`.

import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { Worker } from "node:worker_threads";

const ECHO_PORT = 31852;

const [, , binaryPath, ...libtestArgs] = process.argv;
if (!binaryPath) {
    console.error("emscripten_socket_entry.mjs: missing test binary path");
    process.exit(2);
}

const echoSource = `
const { parentPort, workerData } = require('node:worker_threads');
const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ host: '127.0.0.1', port: workerData.port });
wss.on('connection', (ws) => ws.on('message', (d) => ws.send(d, { binary: true })));
wss.on('listening', () => parentPort.postMessage('listening'));
wss.on('error', (e) => parentPort.postMessage('error: ' + e));
`;
const echo = new Worker(echoSource, { eval: true, workerData: { port: ECHO_PORT } });
await new Promise((res, rej) => {
    echo.once("message", (m) => (m === "listening" ? res() : rej(new Error(m))));
    echo.once("error", rej);
});

const resolvedBinary = resolve(binaryPath);
process.env.TOKIO_EMSCRIPTEN_MODULE_PATH = resolvedBinary;

const factory = (await import(pathToFileURL(resolvedBinary).href)).default;

await factory({
    arguments: libtestArgs,
    onExit(code) {
        echo.terminate();
        process.exit(code);
    },
});
