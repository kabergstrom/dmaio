use super::bufpool::{BufferPool, BufferPoolHeader};
use super::chain_iter::{
    ChainIterForward, ChainIterForwardRaw, ChainIterReverse, ChainIterReverseRaw,
};
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
    pub(super) ptr: NonNull<u8>,
}
unsafe impl Send for RawBufferRef {}
unsafe impl Sync for RawBufferRef {}

/// This trait is unsafe to implement only because mut_segment() and shared_segment() must
/// not return references to overlapping memory
pub unsafe trait UserHeader: Send {
    type Mutable: Send;
    type Shared: Send + Sync;
    fn mut_segment(&mut self) -> &mut Self::Mutable;
    fn shared_segment(&self) -> &Self::Shared;
}
unsafe impl UserHeader for () {
    type Mutable = ();
    type Shared = ();
    fn mut_segment(&mut self) -> &mut Self::Mutable {
        self
    }
    fn shared_segment(&self) -> &Self::Shared {
        &self
    }
}

#[derive(Default, Debug)]
#[doc(hidden)]
/// Internal header segment that is stored packed before the data segment
pub(crate) struct BufferHeader {
    pub ref_count: AtomicUsize, // number of references to the buffer by RawBufferRefs and BufferHandles
    pub chain_owner: Option<NonNull<u8>>, // pointer to owning buffer in the chain
    pub next: Option<NonNull<u8>>, // pointer to the next buffer in the chain
    pub prev: Option<NonNull<u8>>, // pointer to the previous buffer in the chain
    pub bufpool: Option<NonNull<BufferPoolHeader>>, // pointer to the header of the pool which owns this buffer
    pub ref_lock_waker: futures_core::task::__internal::AtomicWaker,
    pub data_start_offset: u32, // offset from the start of the data segment
    pub data_size: u32, // size of data in the data segment, starting from data_ptr + data_start_offset
    pub header_size: u16, // size of the header segment (including user header and this header)
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
    pub(super) fn buffer_ref_count(&self) -> usize {
        self.ref_count.load(Ordering::Relaxed)
    }
}
impl<T> BufferRef<T> {
    pub fn raw_mut(&mut self) -> &mut RawBufferRef {
        &mut self.buffer_ref
    }
    pub fn raw(&self) -> &RawBufferRef {
        &self.buffer_ref
    }
    pub fn into_raw(self) -> RawBufferRef {
        self.buffer_ref
    }
    pub unsafe fn from_raw(raw: RawBufferRef) -> Self {
        Self {
            buffer_ref: raw,
            _marker: core::marker::PhantomData,
        }
    }
    pub fn try_reserve_front(&mut self, bytes_to_reserve: usize) -> bool {
        let raw = self.raw_mut();
        let data_ptr = raw.data_ptr();
        let header = raw.buffer_header_mut();
        if header.chain_owner.is_some() {
            todo!("try_reserve_front not implemented for buffer chains");
        }
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
        to_reserve == bytes_to_reserve
    }
    pub fn try_prepend_write(&mut self, buf: &[u8]) -> bool {
        let header = self.raw().buffer_header();
        if header.chain_owner.is_some() {
            todo!("try_prepend_write not implemented for buffer chains");
        }
        let bytes_remaining = header.data_start_offset();
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
        to_write == buf.len()
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
    pub fn chain_owner(&self) -> Option<BufferHandle<T>> {
        self.raw()
            .buffer_header()
            .chain_owner
            .map(|o| BufferHandle {
                handle: unsafe { super::handle::make_handle(o) },
                marker: core::marker::PhantomData,
            })
    }
    pub fn chain_iter_forward(&self) -> ChainIterForward<T> {
        ChainIterForward::new(self.make_handle())
    }
    pub fn chain_iter_reverse(&self) -> ChainIterReverse<T> {
        ChainIterReverse::new(self.make_handle())
    }
    pub fn chain_begin(&self) -> BufferHandle<T> {
        BufferHandle {
            handle: self.raw().chain_begin(),
            marker: core::marker::PhantomData,
        }
    }
    pub fn chain_end(&self) -> BufferHandle<T> {
        BufferHandle {
            handle: self.raw().chain_end(),
            marker: core::marker::PhantomData,
        }
    }
    pub fn make_handle(&self) -> BufferHandle<T> {
        BufferHandle {
            handle: self.buffer_ref.make_handle(),
            marker: core::marker::PhantomData,
        }
    }
    fn user_header(&self) -> &T {
        unsafe { &*(self.buffer_ref.header_ptr() as *const T) }
    }
    fn user_header_mut(&mut self) -> &mut T {
        unsafe { &mut *(self.buffer_ref.header_ptr() as *mut T) }
    }
}
impl<T: UserHeader> BufferRef<T> {
    pub fn mut_header<V>(&mut self) -> &mut V
    where
        <T as UserHeader>::Mutable: AsMut<V>,
    {
        self.user_header_mut().mut_segment().as_mut()
    }
    pub fn shared_header<V>(&self) -> &V
    where
        <T as UserHeader>::Shared: AsRef<V>,
    {
        self.user_header().shared_segment().as_ref()
    }
    pub fn parts(
        &mut self,
    ) -> (
        &<T as UserHeader>::Shared,
        &mut <T as UserHeader>::Mutable,
        &RawBufferRef,
    ) {
        // it's safe to grab two separate references here because the UserHeader unsafe trait requires the user to guarantee
        // shared and mut segments do not overlap in memory
        let shared_segment =
            unsafe { &mut *(self.buffer_ref.header_ptr() as *mut T) }.shared_segment();
        let mut_segment = unsafe { &mut *(self.buffer_ref.header_ptr() as *mut T) }.mut_segment();
        (shared_segment, mut_segment, &self.buffer_ref)
    }
}

impl RawBufferRef {
    pub fn into_raw(self) -> NonNull<u8> {
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
            chain_iter: Some(unsafe { super::chain_iter::chain_begin_ptr(self.ptr) }),
        }
    }
    pub fn chain_owner(&self) -> Option<RawBufferHandle> {
        self.buffer_header()
            .chain_owner
            .map(|o| unsafe { super::handle::make_handle(o) })
    }
    pub fn chain_iter_forward(&self) -> ChainIterForwardRaw {
        ChainIterForwardRaw::new(self.make_handle())
    }
    pub fn chain_iter_reverse(&self) -> ChainIterReverseRaw {
        ChainIterReverseRaw::new(self.make_handle())
    }
    pub fn chain_begin(&self) -> RawBufferHandle {
        unsafe { super::handle::make_handle(super::chain_iter::chain_begin_ptr(self.ptr)) }
    }
    pub fn chain_end(&self) -> RawBufferHandle {
        unsafe { super::handle::make_handle(super::chain_iter::chain_end_ptr(self.ptr)) }
    }
    // pub fn writer<'a>(&'a mut self) -> impl std::io::Write + 'a {
    //     super::write::BufferWriter::new(self)
    // }
    pub fn make_handle(&self) -> RawBufferHandle {
        unsafe { super::handle::make_handle(self.ptr) }
    }
    fn buffer_ref_count(&self) -> usize {
        self.buffer_header().ref_count.load(Ordering::Relaxed)
    }

    pub fn prepend_buffer(&mut self, mut buffer: RawBufferRef) {
        if buffer.buffer_ref_count() != 1 {
            panic!(
                "must have exclusive access to `buffer` when appending - no handles may be active"
            );
        }
        unsafe {
            let mut chain_begin = super::chain_iter::chain_begin_ptr(self.ptr);
            let mut chain_end = super::chain_iter::chain_end_ptr(buffer.ptr);
            // it's assumed safe to modify the buffer headers in the chain since we have mutable access to self and buffer
            (&mut *buffer_header_ptr_mut(chain_begin)).prev = Some(chain_end);
            (&mut *buffer_header_ptr_mut(chain_end)).next = Some(chain_begin);
            // reduce the ref count of `buffer`, then forget it without dropping.
            // this is because `buffer` is no longer the exclusive reference to its chain,
            // so calling drop would cause it to drop the chain.
            buffer
                .buffer_header()
                .ref_count
                .fetch_sub(1, Ordering::Relaxed);
            buffer
                .buffer_header()
                .ref_lock
                .store(false, Ordering::Relaxed);
            core::mem::forget(buffer);

            let mut chain_iter = super::chain_iter::ChainIterReversePtr::new(chain_begin);
            while let Some(buffer_ptr) = chain_iter.next() {
                let buffer_header = &mut *buffer_header_ptr_mut(buffer_ptr);
                buffer_header.chain_owner = Some(self.ptr);
            }
        }
    }
    pub fn append_buffer(&mut self, mut buffer: RawBufferRef) {
        if buffer.buffer_ref_count() != 1 {
            panic!(
                "must have exclusive access to `buffer` when appending - no handles may be active"
            );
        }
        unsafe {
            let mut chain_begin = super::chain_iter::chain_begin_ptr(buffer.ptr);
            let mut chain_end = super::chain_iter::chain_end_ptr(self.ptr);
            // it's assumed safe to modify the buffer headers in the chain since we have mutable access to self and buffer
            (&mut *buffer_header_ptr_mut(chain_begin)).prev = Some(chain_end);
            (&mut *buffer_header_ptr_mut(chain_end)).next = Some(chain_begin);
            // reduce the ref count of `buffer`, then forget it without dropping.
            // this is because `buffer` is no longer the exclusive reference to its chain,
            // so calling drop would cause it to drop the chain.
            buffer
                .buffer_header()
                .ref_count
                .fetch_sub(1, Ordering::Relaxed);
            buffer
                .buffer_header()
                .ref_lock
                .store(false, Ordering::Relaxed);
            core::mem::forget(buffer);

            let mut chain_iter = super::chain_iter::ChainIterForwardPtr::new(chain_end);
            while let Some(buffer_ptr) = chain_iter.next() {
                let buffer_header = &mut *buffer_header_ptr_mut(buffer_ptr);
                buffer_header.chain_owner = Some(self.ptr);
            }
        }
    }
}

impl Drop for RawBufferRef {
    fn drop(&mut self) {
        self.buffer_header()
            .ref_lock
            .store(false, Ordering::Release);
        self.buffer_header().ref_lock_waker.wake();
        unsafe {
            drop_buffer(self.ptr);
        }
    }
}

pub(super) fn buffer_header_ptr_mut(ptr: NonNull<u8>) -> *mut BufferHeader {
    unsafe { ptr.as_ptr().sub(core::mem::size_of::<BufferHeader>()) as *mut BufferHeader }
}
pub(super) fn buffer_header_ptr(ptr: NonNull<u8>) -> *const BufferHeader {
    unsafe { ptr.as_ptr().sub(core::mem::size_of::<BufferHeader>()) as *const BufferHeader }
}
pub(super) fn buffer_data_ptr(header: &BufferHeader, buffer_ptr: NonNull<u8>) -> NonNull<u8> {
    buffer_ptr
}
unsafe fn drop_buffer_chain(buf_ptr: NonNull<u8>, header: &mut BufferHeader) {
    if let Some(next) = header.next {
        let mut chain_iter = super::chain_iter::ChainIterForwardPtr::new(next);
        while let Some(buffer) = chain_iter.next() {
            free_buffer(buffer);
        }
    }
    if let Some(prev) = header.prev {
        let mut chain_iter = super::chain_iter::ChainIterReversePtr::new(prev);
        while let Some(buffer) = chain_iter.next() {
            free_buffer(buffer);
        }
    }
}
// runs the drop logic for a buffer reference (exclusive or non-exclusive) and returns whether the buffer was dropped
pub(super) unsafe fn drop_buffer(buf_ptr: NonNull<u8>) {
    let header = &*buffer_header_ptr(buf_ptr);
    if let Some(owner) = header.chain_owner {
        if owner != buf_ptr {
            // if we're dropping a buffer owned by another buffer,
            // drop as if it's a reference to the owning buffer
            drop_buffer(owner);
            return;
        }
    }
    if header.ref_count.fetch_sub(1, Ordering::Acquire) == 1 {
        let header = &mut *buffer_header_ptr_mut(buf_ptr);
        drop_buffer_chain(buf_ptr, header);
        free_buffer(buf_ptr);
    }
}
unsafe fn free_buffer(buf_ptr: NonNull<u8>) {
    let header = &mut *buffer_header_ptr_mut(buf_ptr);
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
}
