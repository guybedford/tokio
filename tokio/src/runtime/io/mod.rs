#![cfg_attr(
    not(all(feature = "rt", feature = "net", feature = "io-uring", tokio_unstable)),
    allow(dead_code)
)]
mod driver;
use driver::{Direction, Tick};
pub(crate) use driver::ReadyEvent;

// emscripten has no mio; a callback-driven reactor backs the driver there.
#[cfg(not(target_os = "emscripten"))]
pub(crate) use driver::{Driver, Handle};
#[cfg(target_os = "emscripten")]
mod emscripten;
#[cfg(target_os = "emscripten")]
pub(crate) use emscripten::{Driver, Handle};
#[cfg(all(target_os = "emscripten", feature = "net"))]
pub(crate) use emscripten::Source;

mod registration;
pub(crate) use registration::Registration;

mod registration_set;
use registration_set::RegistrationSet;

mod scheduled_io;
use scheduled_io::ScheduledIo;

mod metrics;
use metrics::IoDriverMetrics;

#[cfg(not(target_os = "emscripten"))]
use crate::util::ptr_expose::PtrExposeDomain;
#[cfg(not(target_os = "emscripten"))]
static EXPOSE_IO: PtrExposeDomain<ScheduledIo> = PtrExposeDomain::new();
