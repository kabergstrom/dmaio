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
use core::ptr::{null_mut, NonNull};

/// Typed reference to a buffer and its header. Does not offer access to its contents.
pub struct BufferHandle<T> {
    pub(super) handle: RawBufferHandle,
    pub(super) marker: core::marker::PhantomData<T>,
}
/// Typed reference to a buffer and its header. Does not offer access to its contents.
#[derive(Debug)]
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
    pub fn try_upgrade(
        mut self,
        waker: Option<&core::task::Waker>,
    ) -> core::result::Result<RawBufferRef, Self> {
        // figure out if nothing else has a real ref on the buffer, and if so, turn this into a BufferRef
        // otherwise return None
        // safe to get buffer header since we only access the atomic variable on it
        match unsafe { self.buffer_header() }.ref_lock.compare_exchange(
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
            Err(_) => {
                if let Some(waker) = waker {
                    unsafe { self.buffer_header() }
                        .ref_lock_waker
                        .register(waker);
                    return self.try_upgrade(None);
                }
                Err(self)
            }
        }
    }
    pub(super) fn buffer_ref_count(&self) -> usize {
        unsafe { self.buffer_header() }
            .ref_count
            .load(Ordering::Relaxed)
    }
    pub(super) unsafe fn buffer_header_mut(&mut self) -> &mut BufferHeader {
        unsafe {
            let header_ptr = super::buffer::buffer_header_ptr_mut(self.ptr);
            &mut *header_ptr
        }
    }
    pub(super) unsafe fn buffer_header(&self) -> &BufferHeader {
        unsafe {
            let header_ptr = super::buffer::buffer_header_ptr(self.ptr);
            &*header_ptr
        }
    }
    pub(super) fn data_ptr(&self) -> NonNull<u8> {
        self.ptr
    }
}
impl<T> BufferHandle<T> {
    /// Upgrades the `BufferHandle` into a `BufferRef` if there are no other references with exclusive access.
    pub fn try_upgrade(
        mut self,
        waker: Option<&core::task::Waker>,
    ) -> core::result::Result<BufferRef<T>, Self> {
        unsafe {
            match self.handle.try_upgrade(waker) {
                Ok(buf_ref) => Ok(BufferRef::from_raw(buf_ref)),
                Err(old_ref) => Err(BufferHandle {
                    handle: old_ref,
                    marker: core::marker::PhantomData,
                }),
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
        unsafe { super::buffer::drop_buffer(ptr) };
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

// ptr must point to the data segment of an allocated buffer
pub(super) unsafe fn make_handle(ptr: NonNull<u8>) -> RawBufferHandle {
    assert!(
        (&*super::buffer::buffer_header_ptr(ptr))
            .ref_count
            .fetch_add(1, Ordering::Relaxed)
            > 0
    );
    RawBufferHandle { ptr: ptr }
}
