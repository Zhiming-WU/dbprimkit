//! A simple appender implementation with rotation support. Payloads are persisted
//! in appending way, and new rotation is created if some conditions are met (
//! e.g. the rotation size reaches a threshold) and the conditions can be customized.

#[cfg(feature = "async")]
pub mod r#async;
pub mod sync;
