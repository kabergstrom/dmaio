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

pub(super) struct ChainIterForward<'a> {
    current: Option<RawBufferHandle>,
    _marker: core::marker::PhantomData<&'a ()>,
}
impl<'a> ChainIterForward<'a> {
    pub(super) fn new(buffer: &'a RawBufferRef) -> Self {
        ChainIterForward {
            current: Some(buffer.chain_begin()),
            _marker: core::marker::PhantomData,
        }
    }
    pub(super) unsafe fn from_handle(handle: RawBufferHandle) -> Self {
        Self {
            current: Some(handle),
            _marker: core::marker::PhantomData,
        }
    }
}
impl<'a> Iterator for ChainIterForward<'a> {
    type Item = RawBufferHandle;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            // safe since we keep a borrow with the BufferRef
            self.current = unsafe { curr.buffer_header() }.next.clone();
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) struct ChainIterForwardPtr<'a> {
    current: Option<NonNull<u8>>,
    _marker: core::marker::PhantomData<&'a ()>,
}
impl<'a> ChainIterForwardPtr<'a> {
    pub(super) unsafe fn new(ptr: NonNull<u8>) -> Self {
        Self {
            current: Some(ptr),
            _marker: core::marker::PhantomData,
        }
    }
}
impl<'a> Iterator for ChainIterForwardPtr<'a> {
    type Item = NonNull<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            // safe since we keep a borrow with the BufferRef
            self.current = unsafe { &*super::buffer::buffer_header_ptr(curr) }
                .next
                .as_ref()
                .map(|handle| handle.data_ptr());
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) struct ChainIterReversePtr<'a> {
    current: Option<NonNull<u8>>,
    _marker: core::marker::PhantomData<&'a ()>,
}
impl<'a> ChainIterReversePtr<'a> {
    pub(super) unsafe fn new(ptr: NonNull<u8>) -> Self {
        Self {
            current: Some(ptr),
            _marker: core::marker::PhantomData,
        }
    }
}
impl<'a> Iterator for ChainIterReversePtr<'a> {
    type Item = NonNull<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(curr) = self.current.take() {
            // safe since we keep a borrow with the BufferRef
            self.current = unsafe { &*super::buffer::buffer_header_ptr(curr) }
                .prev
                .as_ref()
                .map(|handle| handle.data_ptr());
            Some(curr)
        } else {
            None
        }
    }
}

pub(super) fn chain_begin_ptr(ptr: NonNull<u8>) -> NonNull<u8> {
    let mut buffer_iter = ptr;
    loop {
        if let Some(prev) = unsafe { &*super::buffer::buffer_header_ptr(buffer_iter) }
            .prev
            .as_ref()
            .map(|handle| handle.data_ptr())
        {
            buffer_iter = prev;
        } else {
            break buffer_iter;
        }
    }
}
pub(super) fn chain_end_ptr(ptr: NonNull<u8>) -> NonNull<u8> {
    let mut buffer_iter = ptr;
    loop {
        if let Some(next) = unsafe { &*super::buffer::buffer_header_ptr(buffer_iter) }
            .next
            .as_ref()
            .map(|handle| handle.data_ptr())
        {
            buffer_iter = next;
        } else {
            break buffer_iter;
        }
    }
}
