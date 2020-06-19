use super::buffer::{BufferHeader, BufferRef, RawBufferRef};
use super::bufpool::{BufferPool, BufferPoolHeader};
use super::chain_iter::{
    ChainIterForward, ChainIterForwardRaw, ChainIterReverse, ChainIterReverseRaw,
};
use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::Result;
use core::mem::MaybeUninit;
use core::{
    future::Future,
    ptr::{null_mut, NonNull},
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

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
        unsafe {
            let header = &*super::buffer::buffer_header_ptr(self.ptr);
            // figure out if nothing else has a real ref on the buffer, and if so, turn this into a BufferRef
            // otherwise return None
            // safe to get buffer header since we only access the atomic variable on it
            match header.ref_lock.compare_exchange(
                false,
                true,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let ptr = self.ptr;
                    core::mem::forget(self);
                    Ok(RawBufferRef::from_raw(ptr))
                }
                Err(_) => {
                    if let Some(waker) = waker {
                        header.ref_lock_waker.register(waker);
                        return self.try_upgrade(None);
                    }
                    Err(self)
                }
            }
        }
    }
    pub(super) fn buffer_ref_count(&self) -> usize {
        unsafe {
            let header = &*super::buffer::buffer_header_ptr(self.ptr);
            let owning_buffer_header = if let Some(owner) = header.chain_owner {
                &*super::buffer::buffer_header_ptr(owner)
            } else {
                header
            };
            header.ref_count.load(Ordering::Relaxed)
        }
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

    pub fn chain_owner(&self) -> Option<RawBufferHandle> {
        unsafe {
            self.buffer_header()
                .chain_owner
                .map(|o| unsafe { make_handle(o) })
        }
    }
    pub fn chain_iter_forward(&self) -> ChainIterForwardRaw {
        ChainIterForwardRaw::new(self.clone())
    }
    pub fn chain_iter_reverse(&self) -> ChainIterReverseRaw {
        ChainIterReverseRaw::new(self.clone())
    }
    pub fn chain_begin(&self) -> RawBufferHandle {
        unsafe { make_handle(super::chain_iter::chain_begin_ptr(self.ptr)) }
    }
    pub fn chain_end(&self) -> RawBufferHandle {
        unsafe { make_handle(super::chain_iter::chain_end_ptr(self.ptr)) }
    }
}

struct BufferUpgradeFuture<T> {
    handle: Option<BufferHandle<T>>,
}

// impl<T> Future for BufferUpgradeFuture<T> {
//     type Output = BufferRef<T>;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         // pin FML
//         let mut buffer_handle = self.et_mut().handle.take().unwrap();
//         match buffer_handle.try_upgrade(Some(cx.waker())) {
//             Ok(buf_ref) => Poll::Ready(buf_ref),
//             Err(original_handle) => {
//                 self.handle.replace(original_handle);
//                 Poll::Pending
//             }
//         }
//     }
// }

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
    pub fn raw(&self) -> &RawBufferHandle {
        &self.handle
    }
    pub unsafe fn user_header_ptr(&self) -> *const T {
        unsafe {
            self.handle
                .ptr
                .as_ptr()
                .sub(self.handle.buffer_header().header_size as usize) as *const T
        }
    }

    pub fn chain_owner(&self) -> Option<BufferHandle<T>> {
        unsafe {
            self.raw().buffer_header().chain_owner.map(|o| unsafe {
                Self {
                    handle: make_handle(o),
                    marker: core::marker::PhantomData,
                }
            })
        }
    }
    pub fn chain_iter_forward(&self) -> ChainIterForward<T> {
        ChainIterForward::new(self.clone())
    }
    pub fn chain_iter_reverse(&self) -> ChainIterReverse<T> {
        ChainIterReverse::new(self.clone())
    }
    pub fn chain_begin(&self) -> Self {
        unsafe {
            Self {
                handle: make_handle(super::chain_iter::chain_begin_ptr(self.raw().ptr)),
                marker: core::marker::PhantomData,
            }
        }
    }
    pub fn chain_end(&self) -> Self {
        unsafe {
            Self {
                handle: make_handle(super::chain_iter::chain_end_ptr(self.raw().ptr)),
                marker: core::marker::PhantomData,
            }
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

impl<T> PartialEq<BufferHandle<T>> for BufferHandle<T> {
    fn eq(&self, other: &BufferHandle<T>) -> bool {
        self.handle.ptr == other.handle.ptr
    }
}

impl<T> PartialEq<BufferRef<T>> for BufferHandle<T> {
    fn eq(&self, other: &BufferRef<T>) -> bool {
        self.handle.ptr == other.raw().ptr
    }
}

impl<T> PartialEq<RawBufferHandle> for BufferHandle<T> {
    fn eq(&self, other: &RawBufferHandle) -> bool {
        self.handle.ptr == other.ptr
    }
}

impl<T> PartialEq<BufferHandle<T>> for RawBufferHandle {
    fn eq(&self, other: &BufferHandle<T>) -> bool {
        self.ptr == other.handle.ptr
    }
}

impl<T> PartialEq<BufferRef<T>> for RawBufferHandle {
    fn eq(&self, other: &BufferRef<T>) -> bool {
        self.ptr == other.raw().ptr
    }
}

impl PartialEq<RawBufferHandle> for RawBufferHandle {
    fn eq(&self, other: &RawBufferHandle) -> bool {
        self.ptr == other.ptr
    }
}

// ptr must point to the data segment of an allocated buffer
pub(super) unsafe fn make_handle(ptr: NonNull<u8>) -> RawBufferHandle {
    let header = &*super::buffer::buffer_header_ptr(ptr);
    let owning_buffer_header = if let Some(owner) = header.chain_owner {
        &*super::buffer::buffer_header_ptr(owner)
    } else {
        header
    };
    assert!(
        owning_buffer_header
            .ref_count
            .fetch_add(1, Ordering::Relaxed)
            > 0
    );
    RawBufferHandle { ptr: ptr }
}
