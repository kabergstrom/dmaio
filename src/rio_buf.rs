#![allow(non_snake_case)]
use crate::alloc::alloc::Layout;
use crate::buffer::{BufferRef, RawBufferRef};
use crate::mempool::{BufferInitializer, CustomMemPool, MemPool, MemPoolHeader};
use crate::rio::{read_sockaddr, rio_vtable, write_sockaddr, RIOOpType};
use crate::Result;
use core::mem::MaybeUninit;
use winapi::{
    ctypes::c_int,
    shared::mswsockdef::RIO_INVALID_BUFFERID,
    shared::{
        minwindef::{DWORD, MAKEWORD, ULONG},
        ws2def,
    },
};

use winapi::shared::mswsockdef::{RIO_BUF, RIO_BUFFERID};

pub struct RIOPacketBuf {
    pub(crate) data_rio_offset: u32,
    pub(crate) rio_buffer_id: RIO_BUFFERID,
    pub(crate) local_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) remote_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) active_op: RIOOpType,
    pub(crate) op_socket: Option<crate::rio::RIOSocket>,
}
unsafe impl Send for RIOPacketBuf {}
unsafe impl Sync for RIOPacketBuf {}
impl Default for RIOPacketBuf {
    fn default() -> Self {
        Self {
            data_rio_offset: Default::default(),
            rio_buffer_id: RIO_INVALID_BUFFERID,
            local_addr: Default::default(),
            remote_addr: Default::default(),
            active_op: Default::default(),
            op_socket: None,
        }
    }
}
impl AsMut<RIOPacketBuf> for RIOPacketBuf {
    fn as_mut(&mut self) -> &mut RIOPacketBuf {
        self
    }
}

impl RIOPacketBuf {
    pub(crate) unsafe fn make_rio_buf(
        &self,
        buf_ref: &RawBufferRef,
        ptr: *const u8,
        size: u32,
    ) -> RIO_BUF {
        let data_ptr = buf_ref.data_ptr();
        let diff = if ptr > data_ptr {
            -(ptr.sub(data_ptr as usize) as isize)
        } else {
            data_ptr.sub(ptr as usize) as isize
        };
        let rio_offset = self.data_rio_offset;
        let mut rio_buf = RIO_BUF::default();
        rio_buf.Offset = rio_offset - diff as u32;
        rio_buf.Length = size as u32;
        rio_buf.BufferId = self.rio_buffer_id;
        rio_buf
    }
    pub(crate) unsafe fn data_rio_buf(&self, buf_ref: &RawBufferRef, size: u32) -> RIO_BUF {
        let header = buf_ref.buffer_header();
        self.make_rio_buf(
            buf_ref,
            buf_ref.data_ptr().add(header.data_start_offset() as usize) as *const u8,
            size,
        )
    }
    pub(crate) unsafe fn local_addr_rio_buf(&self, buf_ref: &RawBufferRef) -> RIO_BUF {
        self.make_rio_buf(
            buf_ref,
            &self.local_addr as *const _ as *const u8,
            core::mem::size_of::<ws2def::SOCKADDR_STORAGE>() as u32,
        )
    }
    pub(crate) unsafe fn remote_addr_rio_buf(&self, buf_ref: &RawBufferRef) -> RIO_BUF {
        self.make_rio_buf(
            buf_ref,
            &self.remote_addr as *const _ as *const u8,
            core::mem::size_of::<ws2def::SOCKADDR_STORAGE>() as u32,
        )
    }
    pub fn local_addr(&self, buf_ref: &RawBufferRef) -> Option<std::net::SocketAddr> {
        unsafe { read_sockaddr(&self.local_addr) }
    }
    pub fn set_local_addr(&mut self, addr: Option<std::net::SocketAddr>) {
        unsafe {
            write_sockaddr(&mut self.local_addr, addr);
        }
    }
    pub fn remote_addr(&self) -> Option<std::net::SocketAddr> {
        unsafe { read_sockaddr(&self.remote_addr) }
    }
    pub fn set_remote_addr(&mut self, addr: Option<std::net::SocketAddr>) {
        unsafe {
            write_sockaddr(&mut self.remote_addr, addr);
        }
    }
}

pub struct RIOPacketPool {
    buffer_id: RIO_BUFFERID,
}
unsafe impl Send for RIOPacketPool {}
unsafe impl Sync for RIOPacketPool {}
impl Default for RIOPacketPool {
    fn default() -> Self {
        Self {
            buffer_id: RIO_INVALID_BUFFERID,
        }
    }
}
impl CustomMemPool for RIOPacketPool {
    type Header = RIOPacketBuf;

    fn initialize_self(&mut self, mempool: &crate::mempool::MemPoolHeader) -> Result<()> {
        self.buffer_id = unsafe {
            rio_vtable().RegisterBuffer(
                mempool.chunk_start_ptr() as *mut i8,
                mempool.chunk_pool_size() as u32,
            )?
        };
        Ok(())
    }
    fn initialize_buf(&self, mempool: &MemPoolHeader, buffer: &RawBufferRef) -> Self::Header {
        let rio_buffer_id = self.buffer_id;
        let data_rio_offset = buffer
            .data_ptr()
            .wrapping_sub(mempool.chunk_start_ptr() as usize)
            as ULONG;
        let mut header = Self::Header::default();
        header.data_rio_offset = data_rio_offset;
        header.rio_buffer_id = rio_buffer_id;
        header
    }
}

// pub struct RIOPacketPool<T> {
//     pool: UserBufferPool<RIOPacketBuf<T>>,
//     buffer_id: RIO_BUFFERID,
// }
// impl<T: Default> RIOPacketPool<T> {
//     pub fn new(num_elements: usize, element_layout: Layout) -> Result<Self> {
//         let pool = UserBufferPool::new(num_elements, element_layout)?;
//         let buffer_id = unsafe {
//             rio_vtable().RegisterBuffer(
//                 pool.mempool().chunk_start_ptr() as *mut i8,
//                 pool.mempool().chunk_pool_size() as u32,
//             )?
//         };
//         Ok(Self { pool, buffer_id })
//     }
//     pub fn alloc(&self) -> Option<RIOPacketBuf<T>> {
//         self.pool.alloc().map(|mut buf| unsafe {
//             let buf_offset =
//                 buf.buf_ref()
//                     .data_ptr()
//                     .sub(self.pool.mempool().chunk_start_ptr() as usize) as u32;
//             let packet_header = buf.header_mut();
//             packet_header.rio_buffer_id = self.buffer_id;
//             packet_header.data_rio_offset = buf_offset;
//             buf
//         })
//     }
// }

// struct RIOBufHeader<T> {
//     // initialized on alloc.
//     // contains the offset from the start of the registered buffer to the data section of this sub-buffer
//     rio_buf: RIO_BUF,
//     user: MaybeUninit<T>,
// }
// pub struct RIOBuf<T> {
//     buf: BufferRef,
//     _marker: core::marker::PhantomData<T>,
// }
// impl<T> RIOBuf<T> {
//     pub unsafe fn into_raw(self) -> *mut u8 {
//         self.buf.into_raw()
//     }
//     pub unsafe fn from_raw(ptr: *mut u8) -> Self {
//         Self {
//             buf: BufferRef::from_raw(ptr),
//             _marker: core::marker::PhantomData,
//         }
//     }
//     pub fn user_data(&self) -> &MaybeUninit<T> {
//         unsafe { &self.rio_buf_header().user }
//     }
//     pub fn user_data_mut(&mut self) -> &mut MaybeUninit<T> {
//         unsafe { &mut self.rio_buf_header_mut().user }
//     }
//     unsafe fn rio_buf_header(&self) -> &RIOBufHeader<T> {
//         &*(self.buf.header_ptr() as *mut RIOBufHeader<T>)
//     }
//     unsafe fn rio_buf_header_mut(&mut self) -> &mut RIOBufHeader<T> {
//         &mut *(self.buf.header_ptr() as *mut RIOBufHeader<T>)
//     }
//     pub fn rio_buf(&self) -> &RIO_BUF {
//         unsafe { &self.rio_buf_header().rio_buf }
//     }
//     pub fn buffer(&self) -> &BufferRef {
//         &self.buf
//     }
//     unsafe fn make_rio_buf(&self, ptr: *const (), size: usize) -> RIO_BUF {
//         let data_ptr = self.buffer().data_ptr();
//         let mut rio_buf = self.rio_buf().clone();
//         rio_buf.Offset -= data_ptr.sub(ptr as usize) as u32;
//         rio_buf.Length = size as u32;
//         rio_buf
//     }
// }
// pub struct RIOPool<T> {
//     mempool: MemPool,
//     buffer_id: RIO_BUFFERID,
//     _marker: core::marker::PhantomData<T>,
// }
// impl<T> RIOPool<T> {
//     pub fn new(num_elements: usize, element_layout: Layout) -> Result<Self> {
//         unsafe {
//             let free_func = |ptr, size| {
//                 crate::mempool::virtual_free(ptr, size).unwrap();
//             };
//             let mempool = MemPool::new(
//                 num_elements,
//                 element_layout,
//                 Layout::new::<RIOBufHeader<T>>(),
//                 crate::mempool::virtual_alloc,
//                 free_func,
//             )?;
//             let buffer_id = rio_vtable().RegisterBuffer(
//                 mempool.chunk_start_ptr() as *mut i8,
//                 mempool.chunk_pool_size() as u32,
//             )?;
//             Ok(Self {
//                 mempool,
//                 buffer_id,
//                 _marker: core::marker::PhantomData,
//             })
//         }
//     }
//     pub fn alloc(&self) -> Option<RIOBuf<T>> {
//         unsafe {
//             self.mempool.alloc().map(|buf| {
//                 let header_ptr = buf.header_ptr() as *mut RIOBufHeader<T>;
//                 let header_mut = &mut *header_ptr;
//                 core::ptr::write(
//                     &mut header_mut.rio_buf,
//                     RIO_BUF {
//                         BufferId: self.buffer_id,
//                         Offset: buf
//                             .data_ptr()
//                             .wrapping_sub(self.mempool.chunk_start_ptr() as usize)
//                             as ULONG,
//                         Length: buf.data_size() as u32,
//                     },
//                 );
//                 RIOBuf {
//                     buf,
//                     _marker: core::marker::PhantomData,
//                 }
//             })
//         }
//     }
// }
