use crate::alloc::alloc::Layout;
use crate::mempool::MemPool;
use crate::Result;
use core::mem::MaybeUninit;
pub trait IntoBufferRef {
    fn buf_ref(&self) -> &BufferRef;
    unsafe fn into_raw(self) -> *mut u8;
    unsafe fn from_raw(ptr: *mut u8) -> Self
    where
        Self: Sized;
}

#[derive(Debug)]
pub struct BufferHeader {
    pub header_size: usize,
    pub data_size: usize,
    pub allocation_size: usize,
    pub free_obj: *mut (),
    pub free_func: unsafe fn(*mut (), *mut u8, usize),
}
impl BufferHeader {
    pub fn allocation_size(&self) -> usize {
        self.allocation_size
    }
}
impl BufferRef {
    pub unsafe fn into_raw(self) -> *mut u8 {
        let ret_val = self.ptr;
        core::mem::forget(self);
        ret_val
    }
    pub unsafe fn from_raw(ptr: *mut u8) -> Self {
        Self { ptr }
    }
    unsafe fn buffer_header(&self) -> &BufferHeader {
        let header_ptr = self.ptr.sub(core::mem::size_of::<BufferHeader>()) as *mut BufferHeader;
        &*header_ptr
    }
    pub unsafe fn data_ptr(&self) -> *mut u8 {
        self.ptr
    }
    pub unsafe fn data_size(&self) -> usize {
        self.buffer_header().data_size
    }
    pub unsafe fn header_ptr(&self) -> *mut u8 {
        self.ptr.sub(self.buffer_header().header_size)
    }
}
pub struct BufferRef {
    ptr: *mut u8,
}
impl Drop for BufferRef {
    fn drop(&mut self) {
        unsafe {
            let header = self.buffer_header();
            let free_func = self.buffer_header().free_func;
            let allocation_size = header.allocation_size();

            (free_func)(header.free_obj, self.ptr, allocation_size);
        }
    }
}

impl IntoBufferRef for BufferRef {
    fn buf_ref(&self) -> &BufferRef {
        self
    }
    unsafe fn into_raw(self) -> *mut u8 {
        self.into_raw()
    }
    unsafe fn from_raw(ptr: *mut u8) -> Self
    where
        Self: Sized,
    {
        Self { ptr }
    }
}

pub struct BufUserHeader<T> {
    user: T,
}
impl<T: Default> Default for BufUserHeader<T> {
    fn default() -> Self {
        Self { user: T::default() }
    }
}
pub trait UserBufferRef: IntoBufferRef {
    type Header: Sized + Default;
    type UserHeader: Sized + Default;
    fn header(&self) -> &Self::Header;
    fn header_mut(&mut self) -> &mut Self::Header;
    fn user_header(&self) -> &Self::UserHeader;
    fn user_header_mut(&mut self) -> &mut Self::UserHeader;
}

impl<T: UserBufferRef> UserBufferRef for UserBufferRefImpl<T> {
    type Header = BufUserHeader<<T as UserBufferRef>::Header>;
    type UserHeader = <T as UserBufferRef>::Header;
    fn header(&self) -> &Self::Header {
        unsafe { &*(self.buf_ref().header_ptr() as *mut Self::Header) }
    }
    fn header_mut(&mut self) -> &mut Self::Header {
        unsafe { &mut *(self.buf_ref().header_ptr() as *mut Self::Header) }
    }
    fn user_header(&self) -> &Self::UserHeader {
        &self.header().user
    }
    fn user_header_mut(&mut self) -> &mut Self::UserHeader {
        &mut self.header_mut().user
    }
}
pub struct UserBufferRefImpl<T> {
    buf: BufferRef,
    _marker: core::marker::PhantomData<T>,
}
impl<T: UserBufferRef> IntoBufferRef for UserBufferRefImpl<T> {
    fn buf_ref(&self) -> &BufferRef {
        self.buf.buf_ref()
    }
    unsafe fn into_raw(self) -> *mut u8 {
        self.buf.into_raw()
    }
    unsafe fn from_raw(ptr: *mut u8) -> Self
    where
        Self: Sized,
    {
        Self {
            buf: BufferRef::from_raw(ptr),
            _marker: core::marker::PhantomData,
        }
    }
}
pub struct UserBufferPool<T> {
    mempool: MemPool,
    _marker: core::marker::PhantomData<T>,
}
impl<T: UserBufferRef> UserBufferPool<T> {
    pub fn mempool(&self) -> &MemPool {
        &self.mempool
    }
    pub fn new(num_elements: usize, element_layout: Layout) -> Result<Self> {
        unsafe {
            let free_func = |ptr, size| {
                crate::mempool::virtual_free(ptr, size).unwrap();
            };
            let mempool = MemPool::new(
                num_elements,
                element_layout,
                Layout::new::<<T as UserBufferRef>::Header>(),
                crate::mempool::virtual_alloc,
                free_func,
            )?;
            Ok(Self {
                mempool,
                _marker: core::marker::PhantomData,
            })
        }
    }
    pub fn alloc(&self) -> Option<T> {
        unsafe {
            self.mempool
                .alloc()
                .map(|buf| <T as IntoBufferRef>::from_raw(buf.into_raw()))
        }
    }
}
