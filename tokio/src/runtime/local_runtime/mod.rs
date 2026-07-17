mod runtime;

mod options;

#[cfg(all(target_os = "emscripten", tokio_unstable))]
mod hosted_runtime;
#[cfg(all(target_os = "emscripten", tokio_unstable))]
pub use hosted_runtime::HostedRuntime;

pub use options::LocalOptions;
pub use runtime::LocalRuntime;
pub(crate) use runtime::LocalRuntimeScheduler;
