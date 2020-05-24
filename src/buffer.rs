use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::mempool::{BufferPool, MemPool, MemPoolHeader};
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
pub struct BufferRef<T> {
    buffer_ref: RawBufferRef,
    _marker: core::marker::PhantomData<T>,
}
pub struct BufferHandle<T> {
    handle: RawBufferHandle,
    marker: core::marker::PhantomData<T>,
}
impl<T> Clone for BufferHandle<T> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            marker: core::marker::PhantomData,
        }
    }
}
impl<T> BufferHandle<T> {
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
                .sub(self.handle.buffer_header().header_size as usize) as *const T
        }
    }
}
pub struct RawBufferHandle {
    ptr: *mut u8,
}
unsafe impl Send for RawBufferHandle {}
unsafe impl Sync for RawBufferHandle {}
impl Clone for RawBufferHandle {
    fn clone(&self) -> Self {
        unsafe { make_handle(self.ptr) }
    }
}
impl RawBufferHandle {
    pub fn try_upgrade(mut self) -> core::result::Result<RawBufferRef, Self> {
        // figure out if nothing else has a real ref on the buffer, and if so, turn this into a BufferRef
        // otherwise return None
        match self.buffer_header_mut().ref_lock.compare_exchange(
            false,
            true,
            Ordering::Relaxed,
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
}
impl Drop for RawBufferHandle {
    fn drop(&mut self) {
        let ptr = self.ptr;
        let mut header = self.buffer_header_mut();
        if header.ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
            unsafe { drop_buffer(ptr, header) };
        }
    }
}
pub struct RawBufferRef {
    ptr: *mut u8,
}
unsafe impl Send for RawBufferRef {}
unsafe impl Sync for RawBufferRef {}

#[derive(Default, Debug)]
pub struct BufferHeader {
    pub header_size: u16,
    pub data_capacity: u32,
    pub data_start_offset: u32, // offset from the start of the data segment
    pub data_size: u32, // size of data in the data segment, starting from data_ptr + data_start_offset
    pub allocation_size: u32,
    pub ref_count: AtomicUsize,
    pub ref_lock: AtomicBool,
    pub mempool: Option<NonNull<MemPoolHeader>>,
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
    fn data_size_mut(&mut self) -> &mut u32 {
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
    pub fn user_header(&self) -> &T {
        unsafe { &*(self.buffer_ref.header_ptr() as *const T) }
    }
    pub fn user_header_mut(&mut self) -> &mut T {
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
                let copy_src = data_ptr.add(header.data_start_offset() as usize);
                let copy_dst = data_ptr.add(header.data_start_offset() as usize + to_reserve);
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
                    .add(self.raw().buffer_header().data_start_offset() as usize - to_write);
                core::ptr::copy(buf.as_ptr(), write_start, to_write);
            }
            *self.raw_mut().buffer_header_mut().data_start_offset_mut() -= to_write as u32;
        }
        to_write
    }
    pub fn reader<'a>(&'a self) -> BufferReader<'a> {
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
    pub unsafe fn into_raw(self) -> *mut u8 {
        let ret_val = self.ptr;
        core::mem::forget(self);
        ret_val
    }
    pub unsafe fn from_raw(ptr: *mut u8) -> Self {
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
    pub fn data_ptr(&self) -> *mut u8 {
        self.ptr
    }
    pub(crate) unsafe fn header_ptr(&self) -> *mut u8 {
        self.ptr.sub(self.buffer_header().header_size as usize)
    }
    pub fn reader<'a>(&'a self) -> BufferReader<'a> {
        BufferReader {
            buffer: self,
            pos: 0,
        }
    }
    pub fn make_handle(&self) -> RawBufferHandle {
        unsafe { make_handle(self.ptr) }
    }
}

unsafe fn make_handle(ptr: *mut u8) -> RawBufferHandle {
    assert!(
        (&*buffer_header_ptr(ptr))
            .ref_count
            .fetch_add(1, Ordering::Relaxed)
            > 0
    );
    RawBufferHandle { ptr: ptr }
}
fn buffer_header_ptr_mut(ptr: *mut u8) -> *mut BufferHeader {
    unsafe { ptr.sub(core::mem::size_of::<BufferHeader>()) as *mut BufferHeader }
}
fn buffer_header_ptr(ptr: *const u8) -> *const BufferHeader {
    unsafe { ptr.sub(core::mem::size_of::<BufferHeader>()) as *const BufferHeader }
}
impl Drop for RawBufferRef {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.ptr;
            let mut header = self.buffer_header_mut();
            if header.ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
                drop_buffer(ptr, header);
            } else {
                header.ref_lock.store(false, Ordering::Relaxed);
            }
        }
    }
}
unsafe fn drop_buffer(buf_ptr: *mut u8, header: &mut BufferHeader) {
    let ptr = header.mempool;

    let mempool_ref = header.mempool.expect("no mempool set in BufferRef drop");
    // free ourself
    let mempool_header = header
        .mempool
        .take()
        .expect("no mempool set in BufferRef drop");
    mempool_header
        .as_ref()
        .free_bufref(RawBufferRef::from_raw(buf_ptr));
    crate::mempool::decrement_refcount(mempool_ref);
}

impl<T> std::io::Write for BufferRef<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer_ref.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer_ref.flush()
    }
}
impl std::io::Write for RawBufferRef {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let data_ptr = self.data_ptr();
        let mut header = self.buffer_header_mut();
        let bytes_remaining =
            header.data_capacity() - header.data_size() - header.data_start_offset();
        let to_write = core::cmp::min(bytes_remaining as usize, buf.len());
        if to_write > 0 {
            unsafe {
                let write_start =
                    data_ptr.add((header.data_start_offset() + header.data_size()) as usize);
                core::ptr::copy(buf.as_ptr(), write_start, to_write);
            }
            *header.data_size_mut() += to_write as u32;
        }
        Ok(to_write)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BufferReader<'a> {
    buffer: &'a RawBufferRef,
    pos: u32,
}
impl<'a> std::io::Read for BufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let header = self.buffer.buffer_header();
        let bytes_remaining = header.data_size() - self.pos;
        if bytes_remaining <= 0 {
            return Ok(0);
        }
        let to_read = core::cmp::min(bytes_remaining as usize, buf.len());
        if to_read > 0 {
            unsafe {
                let src_ptr = self
                    .buffer
                    .data_ptr()
                    .add(header.data_start_offset() as usize);
                let dst_ptr = buf.as_mut_ptr();
                core::ptr::copy(src_ptr, dst_ptr, to_read);
            }
        }
        self.pos += to_read as u32;
        Ok(to_read as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    unsafe fn create_mempool(data_size: usize, num_buffers: usize) -> MemPool<()> {
        MemPool::new(
            num_buffers,
            Layout::from_size_align(data_size, 1).unwrap(),
            crate::mempool::virtual_alloc,
            |ptr, size| crate::mempool::virtual_free(ptr, size).unwrap(),
            (),
        )
        .unwrap()
    }
    #[test]
    fn buf_alloc_free() {
        unsafe {
            let mempool = create_mempool(1024, 1);
            let buf = mempool.alloc();
            assert!(buf.is_some());
            assert!(mempool.alloc().is_none());
            drop(buf);
            let buf = mempool.alloc();
            assert!(buf.is_some());
            drop(buf);
        }
    }

    #[test]
    fn buf_read_write() {
        unsafe {
            use std::io::{Read, Write};
            let mempool = create_mempool(1024, 1);
            let buf = mempool.alloc();
            assert!(buf.is_some());
            let mut buf = buf.unwrap();
            let ref_value = [1, 2, 3];
            assert_eq!(3, buf.write(&ref_value).unwrap());
            let mut value = [0u8; 3];
            let mut reader = buf.reader();
            assert_eq!(3, reader.read(&mut value).unwrap());
            assert_eq!(value, ref_value);
            assert_eq!(0, reader.read(&mut value).unwrap());
        }
    }

    #[test]
    fn buf_write_overflow_fail() {
        unsafe {
            use std::io::{Read, Write};
            let buf_data_size = 256;
            let mempool = create_mempool(buf_data_size, 1);
            let buf = mempool.alloc();
            assert!(buf.is_some());
            let mut buf = buf.unwrap();
            let ref_value = [3u8; 512]; // try to write more than the buffer can hold
            assert_eq!(buf_data_size, buf.write(&ref_value).unwrap());
            assert_eq!(0, buf.write(&ref_value).unwrap());
        }
    }

    #[test]
    fn buf_reserve_front() {
        unsafe {
            use std::io::Write;
            let buf_data_size = 256;
            let mempool = create_mempool(buf_data_size, 1);
            let buf = mempool.alloc();
            assert!(buf.is_some());
            let mut buf = buf.unwrap();
            assert_eq!(255, buf.reserve_front(255));
            let ref_value = [3u8; 512]; // try to write more than the buffer can hold
            assert_eq!(1, buf.write(&ref_value).unwrap());
            assert_eq!(0, buf.reserve_front(1));
        }
    }
}
