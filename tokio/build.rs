//! Build script.
//!
//! The emscripten `#[tokio::test]` harness needs `worker.js` passed to emcc as
//! `--js-library`. We deliberately don't emit `cargo:rustc-link-arg` (it would
//! force the arg onto every downstream emscripten artifact, even ones that never
//! touch the worker FFI); the consumer supplies it via `RUSTFLAGS`. The
//! `rerun-if-changed` below just invalidates tokio's own test build on edits.
//! A no-op on every other target.

fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "emscripten" {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let worker_js = format!("{manifest_dir}/src/emscripten/test_worker.js");
        println!("cargo:rerun-if-changed={worker_js}");
    }
}
