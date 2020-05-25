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
        winnt::{MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE},
    },
};

struct BufferPoolVTable {}
/// BufferPool's internal untyped header
pub struct BufferPoolHeader {
    /// count of references to the buffer pool (by allocated buffers and BufferPools)
    ref_count: AtomicUsize,
    /// pointer to the start of the entire allocation
    alloc_ptr: NonNull<u8>,
    /// layout of the entire allocation
    alloc_layout: Layout,
    /// pointer to the start of the buffer pool
    buffer_start_ptr: NonNull<u8>,
    /// number of buffers in the buffer pool
    num_buffers: usize,
    /// layout of the buffer pool
    pool_layout: Layout,
    /// layout of a buffer
    buffer_layout: Layout,
    /// layout of the data segment of a buffer
    buffer_data_layout: Layout,
    /// layout of the header segment of a buffer
    header_layout: Layout,
    /// ring buffer of free buffers
    free_buffers: Ring<NonNull<u8>>,
    /// function used to free the buffer's memory when the buffer pool is dropped
    free_func: unsafe fn(NonNull<u8>, usize),
    /// list of wakers to wake when buffers are freed; they are added to this vec if no buffers are available on alloc
    pending_wakers: Mutex<Vec<Waker>>,
    /// count of number of free buffers
    num_free_buffers: AtomicUsize,
    /// fat pointer to the buffer pool's typed section, used to initialize and drop buffers
    buf_pool_ptr: MaybeUninit<*mut dyn BufferPoolDyn>,
}
impl BufferPoolHeader {
    pub(crate) fn free_bufref(&self, buffer: RawBufferRef) {
        unsafe {
            (*self.buf_pool_ptr.as_ptr())
                .as_ref()
                .expect("buf pool vtable ptr null")
                .free(buffer);
        }
    }
    pub fn pool_size_bytes(&self) -> usize {
        self.pool_layout.size()
    }
    pub fn pool_start_ptr(&self) -> NonNull<u8> {
        self.buffer_start_ptr
    }
    pub fn buffer_size_bytes(&self) -> usize {
        self.buffer_layout.size()
    }
    unsafe fn init_buffer_header(&self, buffer: &mut RawBufferRef) {
        core::ptr::write(
            buffer.buffer_header_mut(),
            BufferHeader {
                data_capacity: self.buffer_data_layout.size() as u32,
                header_size: self.header_layout.size() as u16,
                data_size: 0,
                data_start_offset: 0,
                ref_count: AtomicUsize::new(1),
                ref_lock: AtomicBool::new(true),
                bufpool: Some(NonNull::new_unchecked(
                    self as *const BufferPoolHeader as *mut BufferPoolHeader,
                )),
            },
        );
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) unsafe fn decrement_refcount(mut ptr: NonNull<BufferPoolHeader>) {
    if ptr.as_ref().ref_count.fetch_sub(1, Ordering::Relaxed) == 1 {
        (*(*ptr.as_mut().buf_pool_ptr.as_ptr())).drop_self_in_place();
    }
}
pub(crate) struct BufferPoolInner<T> {
    header: BufferPoolHeader,
    initializer: T,
}
trait BufferInitializer {
    fn initialize_self_dyn(&mut self, bufpool: &BufferPoolHeader) -> Result<()>;
    unsafe fn initialize_buf_dyn(&self, bufpool: &BufferPoolHeader, buffer: &mut RawBufferRef);
}
impl<T: BufferHeaderInit> BufferInitializer for T {
    fn initialize_self_dyn(&mut self, bufpool: &BufferPoolHeader) -> Result<()> {
        <T as BufferHeaderInit>::initialize_self(self, bufpool)
    }
    unsafe fn initialize_buf_dyn(&self, bufpool: &BufferPoolHeader, buffer: &mut RawBufferRef) {
        let header_ptr = buffer.header_ptr() as *mut <T as BufferHeaderInit>::Header;
        std::ptr::write(
            header_ptr,
            <T as BufferHeaderInit>::initialize_header(self, bufpool, &buffer),
        );
    }
}
/// The `BufferHeaderInit` trait allows for users to implement initialization of custom buffer headers in a `BufferPool`.
pub trait BufferHeaderInit {
    type Header;
    fn initialize_self(&mut self, bufpool: &BufferPoolHeader) -> Result<()>;
    fn initialize_header(&self, bufpool: &BufferPoolHeader, buffer: &RawBufferRef) -> Self::Header;
}
impl BufferHeaderInit for () {
    type Header = ();
    fn initialize_self(&mut self, bufpool: &BufferPoolHeader) -> Result<()> {
        Ok(())
    }
    fn initialize_header(&self, bufpool: &BufferPoolHeader, buffer: &RawBufferRef) -> Self::Header {
        ()
    }
}
// this trait exists so that the alloc/free interface can be free from type parameters
pub(crate) trait BufferPoolDyn {
    fn alloc_initialized(&self, waker: Option<&Waker>) -> Option<RawBufferRef>;
    fn free(&self, buffer: RawBufferRef);
    unsafe fn drop_self_in_place(&mut self);
}
impl<T> Drop for BufferPoolInner<T> {
    fn drop(&mut self) {
        unsafe {
            let free_func = self.header.free_func;
            let alloc_ptr = self.header.alloc_ptr;
            let alloc_size = self.header.alloc_layout.size();
            (free_func)(alloc_ptr, alloc_size);
        }
    }
}
impl<T: BufferHeaderInit> BufferPoolDyn for BufferPoolInner<T> {
    fn alloc_initialized(&self, waker: Option<&Waker>) -> Option<RawBufferRef> {
        unsafe {
            if let Some(mut buffer_ref) = self
                .header
                .free_buffers
                .pop_single()
                .map(|ptr| RawBufferRef::from_raw(ptr))
            {
                self.header.num_free_buffers.fetch_sub(1, Ordering::Relaxed);
                self.header.init_buffer_header(&mut buffer_ref);
                debug_assert!(buffer_ref.data_ptr() > self.header.pool_start_ptr());
                debug_assert!(
                    buffer_ref.data_ptr().as_ptr().add(
                        buffer_ref.buffer_header().data_start_offset() as usize
                            + buffer_ref.buffer_header().data_size() as usize
                    ) <= self
                        .header
                        .alloc_ptr
                        .as_ptr()
                        .add(self.header.alloc_layout.size())
                );
                debug_assert!(
                    buffer_ref.header_ptr() >= self.header.buffer_start_ptr.as_ptr()
                        && buffer_ref.header_ptr()
                            < self
                                .header
                                .buffer_start_ptr
                                .as_ptr()
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
            core::ptr::drop_in_place(buffer.header_ptr() as *mut <T as BufferHeaderInit>::Header);
            if let Some(buffer) = self.header.free_buffers.push(buffer.into_raw()) {
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
/// A fixed-size pool of fixed-size buffers
pub struct BufferPool<T>(*mut BufferPoolInner<T>, core::marker::PhantomData<T>);
unsafe impl<T> Send for BufferPool<T> {}
unsafe impl<T> Sync for BufferPool<T> {}
impl<T> BufferPool<T> {
    pub(crate) fn inner(&self) -> &BufferPoolInner<T> {
        unsafe { &*self.0 }
    }
}
impl<T> Clone for BufferPool<T> {
    fn clone(&self) -> Self {
        self.inner()
            .header
            .ref_count
            .fetch_add(1, Ordering::Relaxed);
        BufferPool(self.0, self.1)
    }
}
impl<T: BufferHeaderInit + 'static + Send + Sync> BufferPool<T> {
    /// Gets a reference to the user-provided `BufferHeaderInit`
    pub fn initializer(&self) -> &T {
        &self.inner().initializer
    }
    /// Creates a new buffer pool
    pub unsafe fn new(
        num_buffers: usize,
        data_segment_layout: Layout,
        alloc_func: unsafe fn(usize) -> Result<*mut u8>,
        free_func: unsafe fn(NonNull<u8>, usize),
        initializer: T,
    ) -> Result<Self> {
        let page_size = page_size();
        let buffer_header_layout = Layout::new::<BufferHeader>();
        // calculate the layout for a single buffer
        let (buffer_layout, data_segment_offset, header_layout) = {
            let user_header = Layout::new::<<T as BufferHeaderInit>::Header>();
            let header_size = user_header.size() + buffer_header_layout.size();
            let header_layout = Layout::from_size_align(header_size, user_header.align())?;
            let (buffer_layout, data_segment_offset) = header_layout.extend(data_segment_layout)?;
            (
                buffer_layout.pad_to_align(),
                data_segment_offset,
                header_layout,
            )
        };
        debug_assert!(buffer_layout.size() < 1 << 32);
        debug_assert!(data_segment_offset < core::u16::MAX as usize);

        // calculate the layout for `num_buffers` buffers
        let (pool_layout, buffer_size) = layout_repeat(&buffer_layout, num_buffers)?;
        let pool_layout = pool_layout.align_to(page_size)?;
        let bufpool_header_layout = Layout::new::<BufferPoolInner<T>>();
        let (alloc_layout, pool_start_offset) = bufpool_header_layout.extend(pool_layout)?;
        // pad the end with a pointer, because the windows RIO buffer registration checks are off by one :)
        let (alloc_layout, _) = alloc_layout
            .extend(Layout::from_size_align(large_page_minimum(), large_page_minimum()).unwrap())?;
        let bufpool_ptr = alloc_func(alloc_layout.size())?;
        let buffer_start_ptr = bufpool_ptr.add(pool_start_offset);
        debug_assert!(buffer_start_ptr as usize % pool_layout.align() == 0);
        debug_assert!(bufpool_ptr as usize % pool_layout.align() == 0);
        core::ptr::write(
            bufpool_ptr as *mut BufferPoolInner<T>,
            BufferPoolInner {
                header: BufferPoolHeader {
                    ref_count: AtomicUsize::new(1),
                    alloc_ptr: NonNull::new_unchecked(bufpool_ptr),
                    num_buffers,
                    buffer_start_ptr: NonNull::new_unchecked(buffer_start_ptr),
                    alloc_layout,
                    buffer_data_layout: data_segment_layout,
                    header_layout,
                    pool_layout,
                    buffer_layout,
                    num_free_buffers: AtomicUsize::new(num_buffers),
                    pending_wakers: Mutex::new(Vec::new()),
                    free_func,
                    free_buffers: Ring::new(num_buffers + 1),
                    buf_pool_ptr: MaybeUninit::zeroed(),
                },
                initializer,
            },
        );
        let mut bufpool = &mut *(bufpool_ptr as *mut BufferPoolInner<T>);
        *bufpool.header.buf_pool_ptr.as_mut_ptr() = bufpool_ptr as *mut BufferPoolInner<T>;

        for i in 0..num_buffers {
            debug_assert!(i * buffer_size < pool_layout.size());
            let buffer_ptr = buffer_start_ptr.add(i * buffer_size);
            let data_ptr = buffer_ptr.add(data_segment_offset);
            let header_ptr = data_ptr.sub(buffer_header_layout.size());
            debug_assert!(header_ptr >= buffer_start_ptr);
            debug_assert!(
                header_ptr.add(buffer_header_layout.size())
                    < buffer_start_ptr.add(pool_layout.size())
            );
            debug_assert!(
                data_ptr.add(data_segment_layout.size()) <= bufpool_ptr.add(alloc_layout.size())
            );
            debug_assert!(
                data_ptr.add(data_segment_layout.size())
                    <= buffer_start_ptr.add(pool_layout.size())
            );
            bufpool
                .header
                .free_buffers
                .push(NonNull::new_unchecked(data_ptr));
        }
        bufpool.initializer.initialize_self(&bufpool.header)?;
        Ok(BufferPool(
            bufpool_ptr as *mut BufferPoolInner<T>,
            core::marker::PhantomData,
        ))
    }
    /// Allocates a buffer. The returned future will return `Poll::Pending` if there are no available buffers,
    /// and will be woken when buffers become available.
    pub fn alloc(&self) -> impl Future<Output = BufferRef<<T as BufferHeaderInit>::Header>> + Send {
        AllocFuture {
            bufpool: self.clone(),
        }
    }
    /// Returns the size of the allocation containing buffers
    pub fn pool_size_bytes(&self) -> usize {
        self.inner().header.pool_layout.size()
    }
    /// Returns a pointer to the start of the allocation containing buffers
    pub fn pool_start_ptr(&self) -> NonNull<u8> {
        self.inner().header.buffer_start_ptr
    }
    /// Returns the size of buffers allocated by this buffer pool in bytes
    pub fn buffer_size_bytes(&self) -> usize {
        self.inner().header.buffer_layout.size()
    }
}
pub fn default_pool<T: BufferHeaderInit + Send + Sync + 'static>(
    pool_size: usize,
    buffer_size: Layout,
    initializer: T,
) -> Result<BufferPool<T>> {
    unsafe {
        let free_func = |ptr: NonNull<u8>, size| {
            virtual_free(ptr.as_ptr(), size).unwrap();
        };
        BufferPool::new(
            pool_size,
            buffer_size,
            virtual_alloc,
            free_func,
            initializer,
        )
    }
}
impl<T> Drop for BufferPool<T> {
    fn drop(&mut self) {
        unsafe {
            decrement_refcount(NonNull::new_unchecked(&mut (*self.0).header));
        }
    }
}

struct AllocFuture<T> {
    bufpool: BufferPool<T>,
}
impl<T: BufferHeaderInit> Future for AllocFuture<T> {
    type Output = BufferRef<<T as BufferHeaderInit>::Header>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.bufpool.inner().alloc_initialized(Some(cx.waker())) {
            Some(buffer) => std::task::Poll::Ready(unsafe {
                BufferRef::<<T as BufferHeaderInit>::Header>::from_raw(buffer)
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
        MEM_RESERVE | MEM_COMMIT, // | MEM_LARGE_PAGES,
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
pub unsafe fn allocation_granularity() -> usize {
    let mut info = sysinfoapi::SYSTEM_INFO::default();
    sysinfoapi::GetSystemInfo(&mut info as *mut _);
    info.dwAllocationGranularity as usize
}
pub unsafe fn large_page_minimum() -> usize {
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
