// Cargo test runner for the emscripten socket test. Like `emscripten_entry.mjs`,
// but first stands up an echo server so the in-wasm test exercises the
// reactor's `connect`/`send`/`recv` against a real peer, without hosting a
// server itself. By default that's a WebSocket echo (stock emscripten sockfs
// emulates TCP over WebSockets); with `TOKIO_EMSCRIPTEN_RAW_SOCKETS=1` it's a
// raw `node:net` TCP echo instead, matching binaries linked with
// `-sNODERAWSOCKETS` (real TCP via node's net module). The echo server runs in
// its own worker so it stays responsive regardless of what the main thread is
// doing.

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Worker } from "node:worker_threads";

const ECHO_PORT = 31852;

const [, , binaryPath, ...args] = process.argv;
if (!binaryPath) {
    console.error("emscripten_socket_entry.mjs: missing test binary path");
    process.exit(2);
}

const wsEchoSource = `
const { parentPort, workerData } = require('node:worker_threads');
const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ host: '127.0.0.1', port: workerData.port });
wss.on('connection', (ws) => ws.on('message', (d) => ws.send(d, { binary: true })));
wss.on('listening', () => parentPort.postMessage('listening'));
wss.on('error', (e) => parentPort.postMessage('error: ' + e));
`;
const tcpEchoSource = `
const { parentPort, workerData } = require('node:worker_threads');
const net = require('node:net');
const server = net.createServer((sock) => sock.pipe(sock));
server.on('error', (e) => parentPort.postMessage('error: ' + e));
server.listen(workerData.port, '127.0.0.1', () => parentPort.postMessage('listening'));
`;
const echoSource = process.env.TOKIO_EMSCRIPTEN_RAW_SOCKETS ? tcpEchoSource : wsEchoSource;
const echo = new Worker(echoSource, { eval: true, workerData: { port: ECHO_PORT } });
await new Promise((res, rej) => {
    echo.once("message", (m) => (m === "listening" ? res() : rej(new Error(m))));
    echo.once("error", rej);
});

const guard = join(dirname(fileURLToPath(import.meta.url)), "emscripten_deadlock_guard.cjs");

const child = spawn(process.execPath, ["--require", guard, binaryPath, ...args], {
    stdio: ["ignore", "inherit", "inherit"],
});

const watchdogMs = Number(process.env.TOKIO_EMSCRIPTEN_TEST_TIMEOUT_MS || 120_000);
const watchdog = setTimeout(() => {
    console.error(`emscripten_socket_entry.mjs: watchdog: ${binaryPath} still running after ${watchdogMs}ms`);
    child.kill();
    process.exitCode = 8;
}, watchdogMs);
watchdog.unref();

child.on("exit", (code, signal) => {
    clearTimeout(watchdog);
    echo.terminate();
    if (process.exitCode === undefined) {
        process.exitCode = code ?? (signal ? 8 : 0);
    }
});
