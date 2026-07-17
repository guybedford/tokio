//! Builds an emscripten hosted event-loop runtime for tests.

use tokio::runtime::HostedRuntime;

pub fn hosted_runtime() -> HostedRuntime {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    builder
        .build_hosted_event_loop_runtime()
        .expect("failed to build the hosted event-loop runtime")
}
