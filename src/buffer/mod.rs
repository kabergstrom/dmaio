#[doc(hidden)]
mod buffer;
#[doc(hidden)]
mod bufpool;
#[doc(hidden)]
mod handle;
#[doc(hidden)]
mod read;
#[doc(hidden)]
#[cfg(test)]
mod test;
#[doc(hidden)]
mod write;

pub(crate) use buffer::BufferHeader;
#[doc(inline)]
pub use buffer::{BufferRef, RawBufferRef};
#[doc(inline)]
pub use bufpool::{default_pool, BufferHeaderInit, BufferPool, BufferPoolHeader};
#[doc(inline)]
pub use handle::{BufferHandle, RawBufferHandle};
