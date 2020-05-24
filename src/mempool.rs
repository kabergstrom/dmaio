use crate::alloc::{alloc::Layout, sync::Arc};
use crate::buffer::{BufferHeader, BufferRef, RawBufferRef};
use crate::ring::Ring;
use crate::{Error, Result};
use core::mem::{size_of, MaybeUninit};
use core::ptr::{null_mut, NonNull};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use parking_lot::Mutex;
use std::{future::Future, task::Waker};
use winapi::{
    shared::ntdef::PVOID,
    um::{
        errhandlingapi::GetLastError,
        memoryapi, sysinfoapi,
        winnt::{MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE},
    },
};

struct MemPoolVTable {}
pub struct MemPoolHeader {
    ref_count: AtomicUsize,
    alloc_ptr: *mut u8,       // pointer to the start of the entire allocation
    alloc_layout: Layout,     // layout of the entire allocation
    chunk_start_ptr: *mut u8, // pointer to the start of the chunk pool
    num_chunks: usize,        // number of chunks in the chunk pool
    pool_layout: Layout,      // layout of the chunk pool
    chunk_layout: Layout,     // layout of a chunk
    data_layout: Layout,      // layout of the data segment of a chunk
    header_layout: Layout,    // layout of the header segment of a chunk
    free_chunks: Ring<*mut u8>,
    free_func: unsafe fn(*mut u8, usize),
    pending_wakers: Mutex<Vec<Waker>>,
    num_free_buffers: AtomicUsize,
    buf_pool_ptr: MaybeUninit<*mut dyn BufferPool>,
}
impl MemPoolHeader {
    pub(crate) fn free_bufref(&self, buffer: RawBufferRef) {
        unsafe {
            (*self.buf_pool_ptr.as_ptr())
                .as_ref()
                .expect("buf pool vtable ptr null")
                .free(buffer);
        }
    }
    pub fn chunk_pool_size(&self) -> usize {
        self.pool_layout.size()
    }
    pub fn chunk_start_ptr(&self) -> *mut u8 {
        self.chunk_start_ptr
    }
    pub fn chunk_size(&self) -> usize {
        self.chunk_layout.size()
    }
    unsafe fn init_buffer_header(&self, buffer: &mut RawBufferRef) {
        core::ptr::write(
            buffer.buffer_header_mut(),
            BufferHeader {
                data_capacity: self.data_layout.size() as u32,
                allocation_size: self.chunk_layout.size() as u32,
                header_size: self.header_layout.size() as u16,
                data_size: 0,
                data_start_offset: 0,
                ref_count: AtomicUsize::new(1),
                ref_lock: AtomicBool::new(true),
                mempool: Some(NonNull::new_unchecked(
                    self as *const MemPoolHeader as *mut MemPoolHeader,
                )),
            },
        );
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) unsafe fn decrement_refcount(mut ptr: NonNull<MemPoolHeader>) {
    if ptr.as_ref().ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
        (*(*ptr.as_mut().buf_pool_ptr.as_ptr())).drop_self_in_place();
    }
}
pub(crate) struct MemPoolInner<T> {
    header: MemPoolHeader,
    initializer: T,
}
pub trait BufferInitializer {
    fn initialize_self_dyn(&mut self, mempool: &MemPoolHeader) -> Result<()>;
    unsafe fn initialize_buf_dyn(&self, mempool: &MemPoolHeader, buffer: &mut RawBufferRef);
}
impl<T: CustomMemPool> BufferInitializer for T {
    fn initialize_self_dyn(&mut self, mempool: &MemPoolHeader) -> Result<()> {
        <T as CustomMemPool>::initialize_self(self, mempool)
    }
    unsafe fn initialize_buf_dyn(&self, mempool: &MemPoolHeader, buffer: &mut RawBufferRef) {
        let header_ptr = buffer.header_ptr() as *mut <T as CustomMemPool>::Header;
        std::ptr::write(
            header_ptr,
            <T as CustomMemPool>::initialize_buf(self, mempool, &buffer),
        );
    }
}
pub trait CustomMemPool: BufferInitializer {
    type Header;
    fn initialize_self(&mut self, mempool: &MemPoolHeader) -> Result<()>;
    fn initialize_buf(&self, mempool: &MemPoolHeader, buffer: &RawBufferRef) -> Self::Header;
}
impl CustomMemPool for () {
    type Header = ();
    fn initialize_self(&mut self, mempool: &MemPoolHeader) -> Result<()> {
        Ok(())
    }
    fn initialize_buf(&self, mempool: &MemPoolHeader, buffer: &RawBufferRef) -> Self::Header {
        ()
    }
}
// this trait exists so that the alloc/free interface can be free from type parameters
pub trait BufferPool {
    fn alloc_initialized(&self, waker: Option<&Waker>) -> Option<RawBufferRef>;
    fn free(&self, buffer: RawBufferRef);
    unsafe fn drop_self_in_place(&mut self);
}
impl<T> Drop for MemPoolInner<T> {
    fn drop(&mut self) {
        unsafe {
            let free_func = self.header.free_func;
            let alloc_ptr = self.header.alloc_ptr;
            let alloc_size = self.header.alloc_layout.size();
            (free_func)(alloc_ptr, alloc_size);
        }
    }
}
impl<T: CustomMemPool> BufferPool for MemPoolInner<T> {
    fn alloc_initialized(&self, waker: Option<&Waker>) -> Option<RawBufferRef> {
        unsafe {
            if let Some(mut buffer_ref) = self
                .header
                .free_chunks
                .pop_single()
                .map(|ptr| RawBufferRef::from_raw(ptr))
            {
                self.header.num_free_buffers.fetch_sub(1, Ordering::Relaxed);
                self.header.init_buffer_header(&mut buffer_ref);
                debug_assert!(buffer_ref.data_ptr() > self.header.chunk_start_ptr());
                debug_assert!(
                    buffer_ref.data_ptr().add(
                        buffer_ref.buffer_header().data_start_offset() as usize
                            + buffer_ref.buffer_header().data_size() as usize
                    ) <= self.header.alloc_ptr.add(self.header.alloc_layout.size())
                );
                debug_assert!(
                    buffer_ref.header_ptr() >= self.header.chunk_start_ptr
                        && buffer_ref.header_ptr()
                            < self
                                .header
                                .chunk_start_ptr
                                .add(self.header.alloc_layout.size())
                );
                self.initializer
                    .initialize_buf_dyn(&self.header, &mut buffer_ref);
                Some(buffer_ref)
            } else {
                None
            }
        }
    }
    fn free(&self, buffer: RawBufferRef) {
        unsafe {
            core::ptr::drop_in_place(buffer.header_ptr() as *mut <T as CustomMemPool>::Header);
            if let Some(buffer) = self.header.free_chunks.push(buffer.into_raw()) {
                core::mem::forget(buffer);
                panic!("buffer free exceeded internal pool capacity");
            } else {
                if self.header.num_free_buffers.fetch_add(1, Ordering::Relaxed) == 0 {
                    let mut pending_wakers = self.header.pending_wakers.lock();
                    let len = pending_wakers.len();
                    for waker in pending_wakers.drain(0..len) {
                        waker.wake();
                    }
                }
            }
        }
    }
    unsafe fn drop_self_in_place(&mut self) {
        // drops self
        core::ptr::drop_in_place(self);
    }
}
pub struct MemPool<T>(*mut MemPoolInner<T>, core::marker::PhantomData<T>);
unsafe impl<T> Send for MemPool<T> {}
unsafe impl<T> Sync for MemPool<T> {}
impl<T> MemPool<T> {
    pub(crate) fn inner(&self) -> &MemPoolInner<T> {
        unsafe { &*self.0 }
    }
}
impl<T> Clone for MemPool<T> {
    fn clone(&self) -> Self {
        self.inner()
            .header
            .ref_count
            .fetch_add(1, Ordering::Relaxed);
        MemPool(self.0, self.1)
    }
}
impl<T: CustomMemPool + 'static + Send + Sync> MemPool<T> {
    pub fn initializer(&self) -> &T {
        &self.inner().initializer
    }
    pub unsafe fn new(
        num_elements: usize,
        element_layout: Layout,
        alloc_func: unsafe fn(usize) -> Result<*mut u8>,
        free_func: unsafe fn(*mut u8, usize),
        initializer: T,
    ) -> Result<Self> {
        let page_size = page_size();
        let buffer_header_layout = Layout::new::<BufferHeader>();
        // calculate the layout for a single chunk
        let (chunk_layout, element_offset, header_layout) = {
            let user_header = Layout::new::<<T as CustomMemPool>::Header>();
            let header_size = user_header.size() + buffer_header_layout.size();
            let header_layout = Layout::from_size_align(header_size, user_header.align())?;
            let (chunk_layout, element_offset) = header_layout.extend(element_layout)?;
            (chunk_layout.pad_to_align(), element_offset, header_layout)
        };
        debug_assert!(chunk_layout.size() < 1 << 32);
        debug_assert!(element_offset < core::u16::MAX as usize);

        // calculate the layout for `num_elements` chunks
        let (pool_layout, chunk_size) = layout_repeat(&chunk_layout, num_elements)?;
        let pool_layout = pool_layout.align_to(page_size)?;
        let mempool_header_layout = Layout::new::<MemPoolInner<T>>();
        let (alloc_layout, pool_start_offset) = mempool_header_layout.extend(pool_layout)?;
        // pad the end with a pointer, because the windows RIO buffer registration checks are off by one :)
        let (alloc_layout, _) = alloc_layout.extend(Layout::new::<usize>())?;
        let mempool_ptr = alloc_func(alloc_layout.size())?;
        let chunk_start_ptr = mempool_ptr.add(pool_start_offset);
        debug_assert!(chunk_start_ptr as usize % pool_layout.align() == 0);
        debug_assert!(mempool_ptr as usize % pool_layout.align() == 0);
        core::ptr::write(
            mempool_ptr as *mut MemPoolInner<T>,
            MemPoolInner {
                header: MemPoolHeader {
                    ref_count: AtomicUsize::new(1),
                    alloc_ptr: mempool_ptr,
                    num_chunks: num_elements,
                    chunk_start_ptr,
                    alloc_layout,
                    data_layout: element_layout,
                    header_layout,
                    pool_layout,
                    chunk_layout,
                    num_free_buffers: AtomicUsize::new(num_elements),
                    pending_wakers: Mutex::new(Vec::new()),
                    free_func,
                    free_chunks: Ring::new(num_elements + 1),
                    buf_pool_ptr: MaybeUninit::zeroed(),
                },
                initializer,
            },
        );
        let mut mempool = &mut *(mempool_ptr as *mut MemPoolInner<T>);
        *mempool.header.buf_pool_ptr.as_mut_ptr() = mempool_ptr as *mut MemPoolInner<T>;

        for i in 0..num_elements {
            debug_assert!(i * chunk_size < pool_layout.size());
            let chunk_ptr = chunk_start_ptr.add(i * chunk_size);
            let data_ptr = chunk_ptr.add(element_offset);
            let header_ptr = data_ptr.sub(buffer_header_layout.size());
            debug_assert!(header_ptr >= chunk_start_ptr);
            debug_assert!(
                header_ptr.add(buffer_header_layout.size())
                    < chunk_start_ptr.add(pool_layout.size())
            );
            debug_assert!(
                data_ptr.add(element_layout.size()) <= mempool_ptr.add(alloc_layout.size())
            );
            debug_assert!(
                data_ptr.add(element_layout.size()) <= chunk_start_ptr.add(pool_layout.size())
            );
            mempool.header.free_chunks.push(data_ptr);
        }
        mempool.initializer.initialize_self(&mempool.header)?;
        Ok(MemPool(
            mempool_ptr as *mut MemPoolInner<T>,
            core::marker::PhantomData,
        ))
    }
    // pub unsafe fn free_raw(&self, ptr: *mut u8) {
    //     self.header().free_chunks.push(ptr);
    // }
    // pub unsafe fn alloc_raw(&self) -> Option<(*mut u8, usize)> {
    //     if let Some(buf) = self.header().free_chunks.pop_single() {
    //         Some((buf, self.chunk_size()))
    //     } else {
    //         None
    //     }
    // }
    pub fn alloc(&self) -> impl Future<Output = BufferRef<<T as CustomMemPool>::Header>> + Send {
        AllocFuture {
            mempool: self.clone(),
        }
    }
    pub fn chunk_pool_size(&self) -> usize {
        self.inner().header.pool_layout.size()
    }
    pub fn chunk_start_ptr(&self) -> *mut u8 {
        self.inner().header.chunk_start_ptr
    }
    pub fn chunk_size(&self) -> usize {
        self.inner().header.chunk_layout.size()
    }
}
pub fn default<T: CustomMemPool + Send + Sync + 'static>(
    pool_size: usize,
    buffer_size: Layout,
    initializer: T,
) -> Result<MemPool<T>> {
    unsafe {
        let free_func = |ptr, size| {
            crate::mempool::virtual_free(ptr, size).unwrap();
        };
        MemPool::new(
            pool_size,
            buffer_size,
            virtual_alloc,
            free_func,
            initializer,
        )
    }
}
impl<T> Drop for MemPool<T> {
    fn drop(&mut self) {
        unsafe {
            decrement_refcount(NonNull::new_unchecked(&mut (*self.0).header));
        }
    }
}

struct AllocFuture<T> {
    mempool: MemPool<T>,
}
impl<T: CustomMemPool> Future for AllocFuture<T> {
    type Output = BufferRef<<T as CustomMemPool>::Header>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.mempool.inner().alloc_initialized(Some(cx.waker())) {
            Some(buffer) => std::task::Poll::Ready(unsafe {
                BufferRef::<<T as CustomMemPool>::Header>::from_raw(buffer)
            }),
            None => {
                println!("failed to alloc");
                std::task::Poll::Pending
            }
        }
    }
}

pub unsafe fn virtual_alloc(size: usize) -> Result<*mut u8> {
    let buffer = memoryapi::VirtualAlloc(
        null_mut(),
        size,
        MEM_RESERVE | MEM_COMMIT, //| MEM_LARGE_PAGES,
        PAGE_READWRITE,
    ) as *mut u8;
    if buffer == null_mut() {
        Err(Error::WinError("VirtualAlloc", GetLastError()))?
    } else {
        Ok(buffer)
    }
}
pub unsafe fn virtual_free(buf: *mut u8, size: usize) -> Result<()> {
    let ret_val = memoryapi::VirtualFree(buf as PVOID, size, MEM_RELEASE);
    if ret_val != 0 {
        Err(Error::WinError("VirtualFree", GetLastError()))?
    } else {
        Ok(())
    }
}
fn ensure_divisible(val: usize, divisor: usize) -> usize {
    let rem = val % divisor;
    if rem != 0 {
        val + divisor - rem
    } else {
        val
    }
}

pub unsafe fn page_size() -> usize {
    let mut info = sysinfoapi::SYSTEM_INFO::default();
    sysinfoapi::GetSystemInfo(&mut info as *mut _);
    info.dwPageSize as usize
}
pub unsafe fn allocation_granularity(size: usize) -> usize {
    let mut info = sysinfoapi::SYSTEM_INFO::default();
    sysinfoapi::GetSystemInfo(&mut info as *mut _);
    info.dwAllocationGranularity as usize
}
pub unsafe fn large_page_minimum(size: usize) -> usize {
    memoryapi::GetLargePageMinimum()
}
fn layout_padding_needed_for(layout: &Layout, align: usize) -> usize {
    let len = layout.size();

    // Rounded up value is:
    //   len_rounded_up = (len + align - 1) & !(align - 1);
    // and then we return the padding difference: `len_rounded_up - len`.
    //
    // We use modular arithmetic throughout:
    //
    // 1. align is guaranteed to be > 0, so align - 1 is always
    //    valid.
    //
    // 2. `len + align - 1` can overflow by at most `align - 1`,
    //    so the &-mask with `!(align - 1)` will ensure that in the
    //    case of overflow, `len_rounded_up` will itself be 0.
    //    Thus the returned padding, when added to `len`, yields 0,
    //    which trivially satisfies the alignment `align`.
    //
    // (Of course, attempts to allocate blocks of memory whose
    // size and padding overflow in the above manner should cause
    // the allocator to yield an error anyway.)

    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}
fn layout_repeat(layout: &Layout, n: usize) -> Result<(Layout, usize)> {
    // This cannot overflow. Quoting from the invariant of Layout:
    // > `size`, when rounded up to the nearest multiple of `align`,
    // > must not overflow (i.e., the rounded value must be less than
    // > `usize::MAX`)
    let padded_size = layout.size() + layout_padding_needed_for(layout, layout.align());
    let alloc_size = padded_size.checked_mul(n).unwrap();
    unsafe {
        // self.align is already known to be valid and alloc_size has been
        // padded already.
        Ok((
            Layout::from_size_align_unchecked(alloc_size, layout.align()),
            padded_size,
        ))
    }
}
