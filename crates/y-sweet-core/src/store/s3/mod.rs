pub mod server;
pub mod wasm;

#[cfg(target_arch = "wasm32")]
pub use wasm::{S3Config, WasmS3Store};

#[cfg(not(target_arch = "wasm32"))]
pub use server::{ServerS3Config, ServerS3Store};
