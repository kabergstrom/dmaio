use super::buffer::RawBufferRef;
use super::handle::{BufferHandle, RawBufferHandle};
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

pub struct ChainIterForward<T> {
    current: Option<BufferHandle<T>>,
}
impl<T> ChainIterForward<T> {
    pub(super) fn new(handle: BufferHandle<T>) -> Self {
        Self {
            current: Some(handle),
        }
    }
}

impl<T> Iterator for ChainIterForward<T> {
    type Item = BufferHandle<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            self.current = unsafe {
                curr.raw().buffer_header().next.map(|ptr| BufferHandle {
                    handle: super::handle::make_handle(ptr),
                    marker: core::marker::PhantomData,
                })
            };
            Some(curr)
        } else {
            None
        }
    }
}

pub struct ChainIterReverse<T> {
    current: Option<BufferHandle<T>>,
}
impl<T> ChainIterReverse<T> {
    pub(super) fn new(handle: BufferHandle<T>) -> Self {
        Self {
            current: Some(handle),
        }
    }
}

impl<T> Iterator for ChainIterReverse<T> {
    type Item = BufferHandle<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            self.current = unsafe {
                curr.raw().buffer_header().prev.map(|ptr| BufferHandle {
                    handle: super::handle::make_handle(ptr),
                    marker: core::marker::PhantomData,
                })
            };
            Some(curr)
        } else {
            None
        }
    }
}

pub struct ChainIterForwardRaw {
    current: Option<RawBufferHandle>,
}
impl ChainIterForwardRaw {
    pub(super) fn new(handle: RawBufferHandle) -> Self {
        Self {
            current: Some(handle),
        }
    }
}

impl Iterator for ChainIterForwardRaw {
    type Item = RawBufferHandle;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            self.current = unsafe {
                curr.buffer_header()
                    .next
                    .map(|ptr| super::handle::make_handle(ptr))
            };
            Some(curr)
        } else {
            None
        }
    }
}

pub struct ChainIterReverseRaw {
    current: Option<RawBufferHandle>,
}
impl ChainIterReverseRaw {
    pub(super) fn new(handle: RawBufferHandle) -> Self {
        Self {
            current: Some(handle),
        }
    }
}

impl Iterator for ChainIterReverseRaw {
    type Item = RawBufferHandle;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            self.current = unsafe {
                curr.buffer_header()
                    .prev
                    .map(|ptr| super::handle::make_handle(ptr))
            };
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) struct ChainIterForwardPtr {
    current: Option<NonNull<u8>>,
}
impl ChainIterForwardPtr {
    pub(super) unsafe fn new(ptr: NonNull<u8>) -> Self {
        Self { current: Some(ptr) }
    }
}

impl Iterator for ChainIterForwardPtr {
    type Item = NonNull<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            self.current = unsafe { &*super::buffer::buffer_header_ptr(curr) }.next;
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) struct ChainIterReversePtr {
    current: Option<NonNull<u8>>,
}
impl ChainIterReversePtr {
    pub(super) unsafe fn new(ptr: NonNull<u8>) -> Self {
        Self { current: Some(ptr) }
    }
}
impl Iterator for ChainIterReversePtr {
    type Item = NonNull<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            // safe since we keep a borrow with the BufferRef
            self.current = unsafe { &*super::buffer::buffer_header_ptr(curr) }.prev;
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) unsafe fn chain_begin_ptr(ptr: NonNull<u8>) -> NonNull<u8> {
    let mut buffer_iter = ptr;
    loop {
        if let Some(prev) = (&*super::buffer::buffer_header_ptr(buffer_iter)).prev {
            buffer_iter = prev;
        } else {
            break buffer_iter;
        }
    }
}
pub(super) unsafe fn chain_end_ptr(ptr: NonNull<u8>) -> NonNull<u8> {
    let mut buffer_iter = ptr;
    loop {
        if let Some(next) = (&*super::buffer::buffer_header_ptr(buffer_iter)).next {
            buffer_iter = next;
        } else {
            break buffer_iter;
        }
    }
}
