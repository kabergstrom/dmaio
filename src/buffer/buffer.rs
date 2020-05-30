use super::bufpool::{BufferPool, BufferPoolHeader};
use super::handle::{BufferHandle, RawBufferHandle};
use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::NonNull;

/// Typed reference to a buffer and its header. Offers exclusive access to its contents
pub struct BufferRef<T> {
    pub(super) buffer_ref: RawBufferRef,
    pub(super) _marker: core::marker::PhantomData<T>,
}
/// Reference to a buffer that offers exclusive access to its contents
pub struct RawBufferRef {
    /// pointer to the buffer's data segment
    ptr: NonNull<u8>,
}
unsafe impl Send for RawBufferRef {}
unsafe impl Sync for RawBufferRef {}

#[derive(Default, Debug)]
#[doc(hidden)]
/// Internal header segment that is stored packed before the data segment
pub(crate) struct BufferHeader {
    pub ref_count: AtomicUsize, // number of references to the buffer by RawBufferRefs and BufferHandles
    pub next: Option<RawBufferHandle>, // pointer to the next buffer in the chain
    pub prev: Option<RawBufferHandle>, // pointer to the previous buffer in the chain
    pub bufpool: Option<NonNull<BufferPoolHeader>>, // pointer to the header of the pool which owns this buffer
    pub ref_lock_waker: futures_core::task::__internal::AtomicWaker,
    pub data_start_offset: u32, // offset from the start of the data segment
    pub data_size: u32, // size of data in the data segment, starting from data_ptr + data_start_offset
    pub header_size: u16, // size of the header segment
    pub ref_lock: AtomicBool, // exclusive lock for BufferRef - true if there exists a BufferRef for the buffer
}
impl BufferHeader {
    pub fn data_segment_layout(&self) -> Layout {
        unsafe {
            (&*self
                .bufpool
                .expect("no buffer pool pointer set for an active buffer reference")
                .as_ptr())
                .buffer_layout()
        }
    }
    /// Available space in this buffer's data segment (capacity - start offset)
    pub fn data_capacity(&self) -> u32 {
        self.data_segment_layout().size() as u32 - self.data_start_offset
    }
    /// Offset in the data segment where valid data starts.
    pub fn data_start_offset(&self) -> u32 {
        self.data_start_offset
    }
    fn data_start_offset_mut(&mut self) -> &mut u32 {
        &mut self.data_start_offset
    }
    /// Size of valid data in data segment
    pub fn data_size(&self) -> u32 {
        self.data_size
    }
    pub unsafe fn set_data_size(&mut self, size: u32) {
        self.data_size = size;
    }
    pub(super) fn data_size_mut(&mut self) -> &mut u32 {
        &mut self.data_size
    }
}
impl<T> BufferRef<T> {
    pub fn raw_mut(&mut self) -> &mut RawBufferRef {
        &mut self.buffer_ref
    }
    pub fn raw(&self) -> &RawBufferRef {
        &self.buffer_ref
    }
    pub unsafe fn into_raw(self) -> RawBufferRef {
        self.buffer_ref
    }
    pub unsafe fn from_raw(raw: RawBufferRef) -> Self {
        Self {
            buffer_ref: raw,
            _marker: core::marker::PhantomData,
        }
    }
    pub fn header_mut<V>(&mut self) -> &mut V
    where
        T: AsMut<V>,
    {
        self.user_header_mut().as_mut()
    }
    pub fn header<V>(&self) -> &V
    where
        T: AsRef<V>,
    {
        self.user_header().as_ref()
    }
    fn user_header(&self) -> &T {
        unsafe { &*(self.buffer_ref.header_ptr() as *const T) }
    }
    fn user_header_mut(&mut self) -> &mut T {
        unsafe { &mut *(self.buffer_ref.header_ptr() as *mut T) }
    }
    pub fn parts(&self) -> (&T, &RawBufferRef) {
        (self.user_header(), &self.buffer_ref)
    }
    pub fn parts_mut(&mut self) -> (&mut T, &mut RawBufferRef) {
        (
            unsafe { &mut *(self.buffer_ref.header_ptr() as *mut T) },
            &mut self.buffer_ref,
        )
    }
    pub fn reserve_front(&mut self, bytes_to_reserve: usize) -> usize {
        let raw = self.raw_mut();
        let data_ptr = raw.data_ptr();
        let header = raw.buffer_header_mut();
        let bytes_remaining = header.data_capacity() - header.data_size();
        let to_reserve = core::cmp::min(bytes_remaining as usize, bytes_to_reserve);
        if to_reserve > 0 && header.data_size() > 0 {
            unsafe {
                let copy_src = data_ptr.as_ptr().add(header.data_start_offset() as usize);
                let copy_dst = data_ptr
                    .as_ptr()
                    .add(header.data_start_offset() as usize + to_reserve);
                core::ptr::copy(copy_src, copy_dst, to_reserve);
            }
        }
        *header.data_start_offset_mut() += to_reserve as u32;
        to_reserve
    }
    pub fn prepend_write(&mut self, buf: &[u8]) -> usize {
        let bytes_remaining = self.raw().buffer_header().data_start_offset();
        let to_write = core::cmp::min(bytes_remaining as usize, buf.len());
        if to_write > 0 {
            unsafe {
                let write_start = self
                    .buffer_ref
                    .data_ptr()
                    .as_ptr()
                    .add(self.raw().buffer_header().data_start_offset() as usize - to_write);
                core::ptr::copy(buf.as_ptr(), write_start, to_write);
            }
            *self.raw_mut().buffer_header_mut().data_start_offset_mut() -= to_write as u32;
        }
        to_write
    }
    pub fn append_buffer(&mut self, mut buffer: Self) {
        self.buffer_ref.append_buffer(buffer.buffer_ref);
    }
    pub fn prepend_buffer(&mut self, mut buffer: Self) {
        self.buffer_ref.prepend_buffer(buffer.buffer_ref);
    }
    pub fn reader<'a>(&'a self) -> impl std::io::Read + 'a {
        self.buffer_ref.reader()
    }
    pub fn make_handle(&self) -> BufferHandle<T> {
        BufferHandle {
            handle: self.buffer_ref.make_handle(),
            marker: core::marker::PhantomData,
        }
    }
}

impl RawBufferRef {
    pub unsafe fn into_raw(self) -> NonNull<u8> {
        let ret_val = self.ptr;
        core::mem::forget(self);
        ret_val
    }
    pub unsafe fn from_raw(ptr: NonNull<u8>) -> Self {
        Self { ptr }
    }
    pub(crate) fn buffer_header_mut(&mut self) -> &mut BufferHeader {
        unsafe {
            let header_ptr = buffer_header_ptr_mut(self.ptr);
            &mut *header_ptr
        }
    }
    pub(crate) fn buffer_header(&self) -> &BufferHeader {
        unsafe {
            let header_ptr = buffer_header_ptr(self.ptr);
            &*header_ptr
        }
    }
    pub fn data_ptr(&self) -> NonNull<u8> {
        self.ptr
    }
    pub(crate) unsafe fn header_ptr(&self) -> *mut u8 {
        self.ptr
            .as_ptr()
            .sub(self.buffer_header().header_size as usize)
    }
    pub fn reader<'a>(&'a self) -> impl std::io::Read + 'a {
        super::read::BufferReader {
            buffer: self,
            chain_buffer_pos: 0,
            chain_iter: Some(self.chain_begin()),
        }
    }
    // pub fn writer<'a>(&'a mut self) -> impl std::io::Write + 'a {
    //     super::write::BufferWriter::new(self)
    // }
    pub fn make_handle(&self) -> RawBufferHandle {
        unsafe { super::handle::make_handle(self.ptr) }
    }
    fn buffer_ref_count(&self) -> usize {
        unsafe { self.buffer_header() }
            .ref_count
            .load(Ordering::Relaxed)
    }
    pub(super) fn chain_begin(&self) -> RawBufferHandle {
        let mut buffer_iter = self.make_handle();
        loop {
            if let Some(prev) = unsafe { buffer_iter.buffer_header() }.prev.clone() {
                buffer_iter = prev;
            } else {
                break buffer_iter;
            }
        }
    }
    fn chain_end(&self) -> RawBufferHandle {
        let mut buffer_iter = self.make_handle();
        loop {
            if let Some(next) = unsafe { buffer_iter.buffer_header() }.next.clone() {
                buffer_iter = next;
            } else {
                break buffer_iter;
            }
        }
    }
    fn validate_exclusive_access(&mut self) -> bool {
        todo!("add a 'chain owner' reference count so that it's legal to keep handles to any part of the chain. 
        make sure to prevent upgrades to refs on buffers where chain owner != self");
        {
            let mut buffer_iter = self.make_handle();
            loop {
                if let Some(next) = unsafe { buffer_iter.buffer_header() }.next.clone() {
                    let ref_count = next.buffer_ref_count();
                    let expected_ref_count = if unsafe { next.buffer_header() }.next.is_some() {
                        3
                    } else {
                        2
                    };
                    if ref_count != expected_ref_count {
                        return false;
                    }
                    buffer_iter = next;
                } else {
                    break;
                }
            }
            let mut buffer_iter = self.make_handle();
            loop {
                if let Some(prev) = unsafe { buffer_iter.buffer_header() }.prev.clone() {
                    let ref_count = prev.buffer_ref_count();
                    let expected_ref_count = if unsafe { prev.buffer_header() }.prev.is_some() {
                        3
                    } else {
                        2
                    };
                    if ref_count != expected_ref_count {
                        return false;
                    }
                    buffer_iter = prev;
                } else {
                    break;
                }
            }
        }
        return true;
    }
    pub fn prepend_buffer(&mut self, mut buffer: RawBufferRef) {
        if !self.validate_exclusive_access()
            || !buffer.validate_exclusive_access()
            || buffer.buffer_ref_count() != 1
        {
            panic!("must have exclusive access to both buffer chains when appending buffer - no handles may be active");
        }
        let mut chain_begin = super::chain_iter::chain_begin_ptr(self.ptr);
        let mut chain_end = super::chain_iter::chain_end_ptr(buffer.ptr);
        // it's assumed safe to modify the buffer headers in the chain since we have mutable access to self and buffer
        unsafe {
            (&mut *buffer_header_ptr_mut(chain_begin)).prev =
                Some(super::handle::make_handle(chain_end));
            (&mut *buffer_header_ptr_mut(chain_end)).next =
                Some(super::handle::make_handle(chain_begin));
            // reduce the ref count of `buffer`, then forget it without dropping.
            // this is because `buffer` is no longer the exclusive reference to its chain,
            // so calling drop would cause it to drop the chain.
            buffer
                .buffer_header()
                .ref_count
                .fetch_sub(1, Ordering::Relaxed);
            core::mem::forget(buffer);
            drop(chain_end);
        }
    }
    pub fn append_buffer(&mut self, mut buffer: RawBufferRef) {
        if !self.validate_exclusive_access()
            || !buffer.validate_exclusive_access()
            || buffer.buffer_ref_count() != 1
        {
            panic!("must have exclusive access to both buffer chains when appending buffer - no handles may be active");
        }
        let mut chain_begin = super::chain_iter::chain_begin_ptr(self.ptr);
        let mut chain_end = super::chain_iter::chain_end_ptr(buffer.ptr);
        // it's assumed safe to modify the buffer headers in the chain since we have mutable access to self and buffer
        unsafe {
            (&mut *buffer_header_ptr_mut(chain_begin)).prev =
                Some(super::handle::make_handle(chain_end));
            (&mut *buffer_header_ptr_mut(chain_end)).next =
                Some(super::handle::make_handle(chain_begin));
            // reduce the ref count of `buffer`, then forget it without dropping.
            // this is because `buffer` is no longer the exclusive reference to its chain,
            // so calling drop would cause it to drop the chain.
            buffer
                .buffer_header()
                .ref_count
                .fetch_sub(1, Ordering::Relaxed);
            core::mem::forget(buffer);
        }
    }
}

impl Drop for RawBufferRef {
    fn drop(&mut self) {
        unsafe {
            if !drop_buffer(self.ptr) {
                self.buffer_header()
                    .ref_lock
                    .store(false, Ordering::Release);
                self.buffer_header().ref_lock_waker.wake();
            }
        }
    }
}

pub(super) fn buffer_header_ptr_mut(ptr: NonNull<u8>) -> *mut BufferHeader {
    unsafe { ptr.as_ptr().sub(core::mem::size_of::<BufferHeader>()) as *mut BufferHeader }
}
pub(super) fn buffer_header_ptr(ptr: NonNull<u8>) -> *const BufferHeader {
    unsafe { ptr.as_ptr().sub(core::mem::size_of::<BufferHeader>()) as *const BufferHeader }
}
unsafe fn drop_buffer_chain(buf_ptr: NonNull<u8>, header: &mut BufferHeader) {
    // get the end and begin pointers for the chain *before* we take the next/prev out of ourself
    let chain_end_ptr = super::chain_iter::chain_end_ptr(buf_ptr);
    let chain_begin_ptr = super::chain_iter::chain_begin_ptr(buf_ptr);
    let next = header.next.take();
    let prev = header.prev.take();
    // some time after this point, when clearing the chain handles, drop_buffer may be called
    // recursively for the current buf_ptr, so make sure to avoid accessing any data from it.
    if next.is_some() {
        let mut chain_iter = super::chain_iter::ChainIterReversePtr::new(chain_end_ptr);
        iter_chain_clear_until_self(
            buf_ptr,
            chain_iter,
            |header: &mut BufferHeader| header.prev = None,
            |header: &mut BufferHeader| header.next = None,
        );
    }
    if prev.is_some() {
        let mut chain_iter = super::chain_iter::ChainIterForwardPtr::new(chain_begin_ptr);
        iter_chain_clear_until_self(
            buf_ptr,
            chain_iter,
            |header| header.next = None,
            |header| header.prev = None,
        );
    }
    // make sure that next/prev is not dropped before the chains are entirely cleared
    drop(next);
    drop(prev);
}
// runs the drop logic for a buffer reference (exclusive or non-exclusive) and returns whether the buffer was dropped
pub(super) unsafe fn drop_buffer(buf_ptr: NonNull<u8>) -> bool {
    let header = &*buffer_header_ptr(buf_ptr);
    // when the buffer is a chain, the handles create a reference counting cycle.
    // the expected_ref_count is the number of references we would expect the buffer to have
    // based on whether the prev/next pointers are set.
    // If we are dropping the only external reference to the buffer,
    // we iterate through the chain and drop the handles,
    // which will call drop_buffer for self and others in the chain
    let mut expected_ref_count = 1;
    if header.next.is_some() {
        expected_ref_count += 1;
    }
    if header.prev.is_some() {
        expected_ref_count += 1;
    }
    if header.ref_count.fetch_sub(1, Ordering::Relaxed) == expected_ref_count {
        let header = &mut *buffer_header_ptr_mut(buf_ptr);
        if expected_ref_count == 1 {
            let bufpool_ptr = header.bufpool;

            let bufpool_ref = header.bufpool.expect("no bufpool set in BufferRef drop");
            // free ourself
            let bufpool_header = header
                .bufpool
                .take()
                .expect("no bufpool set in BufferRef drop");
            bufpool_header
                .as_ref()
                .free_bufref(RawBufferRef::from_raw(buf_ptr));
            super::bufpool::decrement_refcount(bufpool_ref);
        } else {
            drop_buffer_chain(buf_ptr, header);
        }
        true
    } else {
        false
    }
}

unsafe fn iter_chain_clear_until_self<F: FnMut(&mut BufferHeader), G: FnMut(&mut BufferHeader)>(
    this: NonNull<u8>,
    mut chain_iter: impl Iterator<Item = NonNull<u8>>,
    mut clear_next_func: F,
    mut clear_prev_func: G,
) {
    let mut prev: Option<NonNull<u8>> = None;
    while let Some(mut curr) = chain_iter.next() {
        if let Some(mut prev) = prev.take() {
            clear_next_func((&mut *buffer_header_ptr_mut(prev)));
        }
        if curr == this {
            break;
        }
        clear_prev_func((&mut *buffer_header_ptr_mut(curr)));
        prev = Some(curr);
    }
    if let Some(mut prev) = prev.take() {
        clear_prev_func((&mut *buffer_header_ptr_mut(prev)));
    }
}
