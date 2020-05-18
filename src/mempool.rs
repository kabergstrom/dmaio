use crate::alloc::alloc::{Layout};
use crate::buffer::{BufferRef, BufferHeader};
use crate::ring::Ring;
use crate::{Error, Result};
use core::ptr::{null_mut};
use core::sync::atomic::{AtomicUsize, Ordering};
use winapi::{
    shared::ntdef::PVOID,
    um::{
        errhandlingapi::GetLastError,
        memoryapi, sysinfoapi,
        winnt::{MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE},
    },
};

struct MemPoolHeader {
    alloc_ptr: *mut u8,
    ref_count: AtomicUsize,
    alloc_layout: Layout,
    chunk_start_ptr: *mut u8,
    pool_layout: Layout,
    num_chunks: usize,
    chunk_layout: Layout,
    free_chunks: Ring<*mut u8>,
    free_func: unsafe fn(*mut u8, usize),
}
pub struct MemPool {
    ptr: *mut MemPoolHeader,
}
impl Clone for MemPool {
    fn clone(&self) -> Self {
        unsafe {
            (*self.ptr).ref_count.fetch_add(1, Ordering::Relaxed);
            Self { ptr: self.ptr }
        }
    }
}
impl Drop for MemPool {
    fn drop(&mut self) {
        unsafe {
            let mut header = &mut *self.ptr;
            if header.ref_count.fetch_sub(1, Ordering::Acquire) == 0 {
                let free_func = header.free_func;
                let alloc_ptr = header.alloc_ptr;
                let alloc_size = header.alloc_layout.size();
                core::ptr::drop_in_place(self.ptr);
                (header.free_func)(alloc_ptr, alloc_size);
            }
        }
    }
}
impl MemPool {
    unsafe fn header(&self) -> &MemPoolHeader {
        &*self.ptr
    }
    pub unsafe fn new(
        num_elements: usize,
        element_layout: Layout,
        header_layout: Layout,
        alloc_func: unsafe fn(usize) -> Result<*mut u8>,
        free_func: unsafe fn(*mut u8, usize),
    ) -> Result<Self> {
        let buffer_header_layout = Layout::new::<BufferHeader>();
        // calculate the layout for a single chunk
        let (chunk_layout, element_offset) = {
            let user_header = header_layout;
            let header_size = user_header.size() + buffer_header_layout.size();
            let header_layout = Layout::from_size_align(header_size, user_header.align())?;
            let (chunk_layout, element_offset) = header_layout.extend(element_layout)?;
            (chunk_layout.pad_to_align(), element_offset)
        };
        debug_assert!(chunk_layout.size() < 1 << 32);

        // calculate the layout for `num_elements` chunks
        let (pool_layout, chunk_size) = layout_repeat(&chunk_layout, num_elements)?;
        let mempool_header_layout = Layout::new::<MemPoolHeader>();
        let (alloc_layout, pool_start_offset) = mempool_header_layout.extend(pool_layout)?;
        let mempool_ptr = alloc_func(alloc_layout.size())?;
        let chunk_start_ptr = mempool_ptr.add(pool_start_offset);
        debug_assert!(chunk_start_ptr as usize % pool_layout.align() == 0);
        core::ptr::write(
            mempool_ptr as *mut MemPoolHeader,
            MemPoolHeader {
                alloc_ptr: mempool_ptr,
                ref_count: AtomicUsize::new(1),
                num_chunks: num_elements,
                chunk_start_ptr: chunk_start_ptr,
                alloc_layout,
                pool_layout,
                chunk_layout,
                free_func,
                free_chunks: Ring::new(num_elements),
            },
        );
        let mempool_header = &mut *(mempool_ptr as *mut MemPoolHeader);

        for i in 0..num_elements {
            debug_assert!(i * chunk_size < pool_layout.size());
            let chunk_ptr = chunk_start_ptr.add(i * chunk_size);
            let data_ptr = chunk_ptr.add(element_offset);
            let header_ptr = data_ptr.sub(buffer_header_layout.size());
            debug_assert!(
                header_ptr >= chunk_start_ptr
                    && header_ptr.add(buffer_header_layout.size())
                        < mempool_ptr.add(alloc_layout.size())
            );
            let buffer_header = &mut *(header_ptr as *mut BufferHeader);
            core::ptr::write(
                buffer_header,
                BufferHeader {
                    data_size: element_layout.size(),
                    allocation_size: chunk_layout.size(),
                    header_size: element_offset,
                    free_func: |obj, ptr, _| {
                        (&*(obj as *mut MemPoolHeader)).free_chunks.push(ptr);
                    },
                    free_obj: mempool_ptr as *mut (),
                },
            );
            mempool_header.free_chunks.push(data_ptr);
        }
        Ok(MemPool {
            ptr: mempool_header as *mut MemPoolHeader,
        })
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
    pub unsafe fn alloc(&self) -> Option<BufferRef> {
        if let Some(buf) = self.header().free_chunks.pop_single() {
            Some(BufferRef::from_raw(buf))
        } else {
            None
        }
    }
    pub fn chunk_pool_size(&self) -> usize {
        unsafe { self.header().pool_layout.size() }
    }
    pub fn chunk_start_ptr(&self) -> *mut u8 {
        unsafe { self.header().chunk_start_ptr }
    }
    pub fn chunk_size(&self) -> usize {
        unsafe { self.header().chunk_layout.size() }
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
