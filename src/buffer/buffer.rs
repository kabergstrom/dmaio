use super::bufpool::{BufferPool, BufferPoolHeader};
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
    pub data_capacity: u32,     // total capacity of the data segment in bytes
    pub data_start_offset: u32, // offset from the start of the data segment
    pub data_size: u32, // size of data in the data segment, starting from data_ptr + data_start_offset
    pub ref_count: AtomicUsize, // number of references to the buffer by RawBufferRefs and BufferHandles
    pub ref_lock: AtomicBool, // exclusive lock for BufferRef - this is true if there exists a BufferRef for the buffer
    pub header_size: u16,     // size of the header segment
    pub bufpool: Option<NonNull<BufferPoolHeader>>, // pointer to the header of the pool which owns this buffer
}
impl BufferHeader {
    pub fn data_capacity(&self) -> u32 {
        self.data_capacity
    }
    pub fn data_start_offset(&self) -> u32 {
        self.data_start_offset
    }
    fn data_start_offset_mut(&mut self) -> &mut u32 {
        &mut self.data_start_offset
    }
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
        let bytes_remaining =
            header.data_capacity() - header.data_size() - header.data_start_offset();
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
            pos: 0,
        }
    }
    pub fn make_handle(&self) -> RawBufferHandle {
        unsafe { super::handle::make_handle(self.ptr) }
    }
}

impl Drop for RawBufferRef {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.ptr;
            let mut header = self.buffer_header_mut();
            if header.ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
                drop_buffer(ptr, header);
            } else {
                header.ref_lock.store(false, Ordering::Release);
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
pub(super) unsafe fn drop_buffer(buf_ptr: NonNull<u8>, header: &mut BufferHeader) {
    let ptr = header.bufpool;

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
