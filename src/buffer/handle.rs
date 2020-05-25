use super::buffer::{BufferHeader, BufferRef, RawBufferRef};
use super::bufpool::{BufferPool, BufferPoolHeader};
use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::NonNull;

/// Typed reference to a buffer and its header. Does not offer access to its contents.
pub struct BufferHandle<T> {
    pub(super) handle: RawBufferHandle,
    pub(super) marker: core::marker::PhantomData<T>,
}
/// Typed reference to a buffer and its header. Does not offer access to its contents.
pub struct RawBufferHandle {
    /// pointer to the buffer's data segment
    ptr: NonNull<u8>,
}
unsafe impl Send for RawBufferHandle {}
unsafe impl Sync for RawBufferHandle {}
impl Clone for RawBufferHandle {
    fn clone(&self) -> Self {
        unsafe { make_handle(self.ptr) }
    }
}
impl RawBufferHandle {
    /// Upgrades the `RawBufferHandle` into a `RawBufferRef` if there are no other references with exclusive access.
    pub fn try_upgrade(mut self) -> core::result::Result<RawBufferRef, Self> {
        // figure out if nothing else has a real ref on the buffer, and if so, turn this into a BufferRef
        // otherwise return None
        match self.buffer_header_mut().ref_lock.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                let ptr = self.ptr;
                core::mem::forget(self);
                Ok(unsafe { RawBufferRef::from_raw(ptr) })
            }
            Err(_) => Err(self),
        }
    }
    pub(crate) fn buffer_header_mut(&mut self) -> &mut BufferHeader {
        unsafe {
            let header_ptr = super::buffer::buffer_header_ptr_mut(self.ptr);
            &mut *header_ptr
        }
    }
    pub(crate) fn buffer_header(&self) -> &BufferHeader {
        unsafe {
            let header_ptr = super::buffer::buffer_header_ptr(self.ptr);
            &*header_ptr
        }
    }
}
impl<T> BufferHandle<T> {
    /// Upgrades the `BufferHandle` into a `BufferRef` if there are no other references with exclusive access.
    pub fn try_upgrade(mut self) -> core::result::Result<BufferRef<T>, Self> {
        unsafe {
            match self.handle.try_upgrade() {
                Ok(buf_ref) => Ok(BufferRef::from_raw(buf_ref)),
                Err(old_ref) => {
                    println!("failed to upgrade buffer");
                    Err(BufferHandle {
                        handle: old_ref,
                        marker: core::marker::PhantomData,
                    })
                }
            }
        }
    }
    pub unsafe fn user_header_ptr(&self) -> *const T {
        unsafe {
            self.handle
                .ptr
                .as_ptr()
                .sub(self.handle.buffer_header().header_size as usize) as *const T
        }
    }
}
impl Drop for RawBufferHandle {
    fn drop(&mut self) {
        let ptr = self.ptr;
        let mut header = self.buffer_header_mut();
        if header.ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
            unsafe { super::buffer::drop_buffer(ptr, header) };
        }
    }
}
impl<T> Clone for BufferHandle<T> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            marker: core::marker::PhantomData,
        }
    }
}

pub(super) unsafe fn make_handle(ptr: NonNull<u8>) -> RawBufferHandle {
    assert!(
        (&*super::buffer::buffer_header_ptr(ptr))
            .ref_count
            .fetch_add(1, Ordering::Relaxed)
            > 0
    );
    RawBufferHandle { ptr: ptr }
}
