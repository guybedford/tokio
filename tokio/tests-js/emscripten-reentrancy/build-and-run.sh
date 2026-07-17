#!/usr/bin/env bash
# Build the two-export JSPI reentrancy module and run its JS-boundary
# assertions (see run.mjs). Requires an emscripten toolchain on PATH.
set -euo pipefail
cd "$(dirname "$0")"
RUSTFLAGS="--cfg tokio_unstable \
  -C link-args=-sALLOW_MEMORY_GROWTH=1 \
  -C link-args=-sJSPI \
  -C link-args=-sSTACK_SIZE=1048576 \
  -C link-args=-sJSPI_EXPORTS=ex_slow,ex_fast \
  -C link-args=-sJSPI_IMPORTS=test_await_completion \
  -C link-args=-sMODULARIZE \
  -C link-args=-sEXPORT_ES6 \
  -C link-args=-sEXPORTED_FUNCTIONS=_main,_ex_slow,_ex_fast \
  -C link-arg=--js-library=$(pwd)/lib.js" \
  cargo build --target wasm32-unknown-emscripten
cp target/wasm32-unknown-emscripten/debug/emscripten-reentrancy.js module.mjs
cp target/wasm32-unknown-emscripten/debug/emscripten_reentrancy.wasm .
node run.mjs
