#![allow(non_snake_case)]
use crate::alloc::alloc::Layout;
use crate::buffer::{BufferRef, IntoBufferRef, UserBufferPool, UserBufferRef, UserBufferRefImpl};
use crate::mempool::MemPool;
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

impl<T: Default> IntoBufferRef for RIOPacketBuf<T> {
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
            buf: UserBufferRefImpl::<Self>::from_raw(ptr),
            _marker: core::marker::PhantomData,
        }
    }
}
impl<T: Default> UserBufferRef for RIOPacketBuf<T> {
    type Header = RIOPacketBufHeader<T>;
    type UserHeader = T;
    fn header(&self) -> &Self::Header {
        self.buf.user_header()
    }
    fn header_mut(&mut self) -> &mut Self::Header {
        self.buf.user_header_mut()
    }
    fn user_header(&self) -> &Self::UserHeader {
        &self.header().user
    }
    fn user_header_mut(&mut self) -> &mut Self::UserHeader {
        &mut self.header_mut().user
    }
}

pub struct RIOPacketBufHeader<T> {
    pub(crate) data_rio_offset: u32,
    pub(crate) rio_buffer_id: RIO_BUFFERID,
    pub(crate) local_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) remote_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) active_op: RIOOpType,
    pub(crate) user: T,
}
impl<T: Default> Default for RIOPacketBufHeader<T> {
    fn default() -> Self {
        Self {
            data_rio_offset: Default::default(),
            rio_buffer_id: RIO_INVALID_BUFFERID,
            local_addr: Default::default(),
            remote_addr: Default::default(),
            active_op: Default::default(),
            user: Default::default(),
        }
    }
}

pub struct RIOPacketBuf<T> {
    buf: UserBufferRefImpl<Self>,
    _marker: core::marker::PhantomData<T>,
}
impl<T: Default> RIOPacketBuf<T> {
    pub(crate) unsafe fn make_rio_buf(&self, ptr: *const u8, size: usize) -> RIO_BUF {
        let data_ptr = self.buf_ref().data_ptr();
        debug_assert!(data_ptr >= ptr as *mut u8);
        let diff = data_ptr.sub(ptr as usize);
        let rio_offset = self.header().data_rio_offset;
        let mut rio_buf = RIO_BUF::default();
        rio_buf.Offset = rio_offset - diff as u32;
        rio_buf.Length = size as u32;
        rio_buf.BufferId = self.header().rio_buffer_id;
        rio_buf
    }
    pub(crate) unsafe fn data_rio_buf(&self) -> RIO_BUF {
        self.make_rio_buf(
            self.buf_ref().data_ptr() as *const u8,
            self.buf_ref().data_size(),
        )
    }
    pub(crate) unsafe fn local_addr_rio_buf(&self) -> RIO_BUF {
        self.make_rio_buf(
            &self.header().local_addr as *const _ as *const u8,
            core::mem::size_of::<ws2def::SOCKADDR_STORAGE>(),
        )
    }
    pub(crate) unsafe fn remote_addr_rio_buf(&self) -> RIO_BUF {
        self.make_rio_buf(
            &self.header().remote_addr as *const _ as *const u8,
            core::mem::size_of::<ws2def::SOCKADDR_STORAGE>(),
        )
    }
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        unsafe { read_sockaddr(&self.header().local_addr) }
    }
    pub fn set_local_addr(&mut self, addr: Option<std::net::SocketAddr>) {
        unsafe {
            write_sockaddr(&mut self.header_mut().local_addr, addr);
        }
    }
    pub fn remote_addr(&self) -> Option<std::net::SocketAddr> {
        unsafe { read_sockaddr(&self.header().remote_addr) }
    }
    pub fn set_remote_addr(&mut self, addr: Option<std::net::SocketAddr>) {
        unsafe {
            write_sockaddr(&mut self.header_mut().remote_addr, addr);
        }
    }
}

pub struct RIOPacketPool<T> {
    pool: UserBufferPool<RIOPacketBuf<T>>,
    buffer_id: RIO_BUFFERID,
}
impl<T: Default> RIOPacketPool<T> {
    pub fn new(num_elements: usize, element_layout: Layout) -> Result<Self> {
        let pool = UserBufferPool::new(num_elements, element_layout)?;
        let buffer_id = unsafe {
            rio_vtable().RegisterBuffer(
                pool.mempool().chunk_start_ptr() as *mut i8,
                pool.mempool().chunk_pool_size() as u32,
            )?
        };
        Ok(Self { pool, buffer_id })
    }
    pub fn alloc(&self) -> Option<RIOPacketBuf<T>> {
        self.pool.alloc().map(|mut buf| unsafe {
            let buf_offset =
                buf.buf_ref()
                    .data_ptr()
                    .sub(self.pool.mempool().chunk_start_ptr() as usize) as u32;
            let packet_header = buf.header_mut();
            packet_header.rio_buffer_id = self.buffer_id;
            packet_header.data_rio_offset = buf_offset;
            buf
        })
    }
}

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
