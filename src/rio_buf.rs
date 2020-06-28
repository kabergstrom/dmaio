#![allow(non_snake_case)]
use crate::alloc::alloc::Layout;
use crate::buffer::{BufferHeaderInit, BufferPool, BufferPoolHeader, BufferRef, RawBufferRef};
use crate::rio::{read_sockaddr, rio_vtable, write_sockaddr, OwnedRawSocket, RIOSocket};
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

pub(crate) enum NetOp {
    None,
    Send(RIOSocket),
    Receive(RIOSocket),
    AcceptEx(OwnedRawSocket, RIOSocket),
    ConnectEx(RIOSocket),
}
impl Default for NetOp {
    fn default() -> Self {
        NetOp::None
    }
}

pub struct NetHeader {
    pub(crate) data_rio_offset: u32,
    pub(crate) rio_buffer_id: RIO_BUFFERID,
    pub(crate) local_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) remote_addr: ws2def::SOCKADDR_STORAGE,
    pub(crate) active_op: NetOp,
}
unsafe impl Send for NetHeader {}
unsafe impl Sync for NetHeader {}
impl Default for NetHeader {
    fn default() -> Self {
        Self {
            data_rio_offset: Default::default(),
            rio_buffer_id: RIO_INVALID_BUFFERID,
            local_addr: Default::default(),
            remote_addr: Default::default(),
            active_op: Default::default(),
        }
    }
}
impl AsMut<NetHeader> for NetHeader {
    fn as_mut(&mut self) -> &mut NetHeader {
        self
    }
}

impl NetHeader {
    pub(crate) fn consume_op(&mut self) -> NetOp {
        core::mem::replace(&mut self.active_op, NetOp::None)
    }
    pub(crate) unsafe fn make_rio_buf(
        &self,
        buf_ref: &RawBufferRef,
        ptr: *const u8,
        size: u32,
    ) -> RIO_BUF {
        let data_ptr = buf_ref.data_ptr().as_ptr();
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
            buf_ref
                .data_ptr()
                .as_ptr()
                .add(header.data_start_offset() as usize) as *const u8,
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
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
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

pub struct NetHeaderInit {
    buffer_id: RIO_BUFFERID,
}
unsafe impl Send for NetHeaderInit {}
unsafe impl Sync for NetHeaderInit {}
impl Default for NetHeaderInit {
    fn default() -> Self {
        Self {
            buffer_id: RIO_INVALID_BUFFERID,
        }
    }
}
impl BufferHeaderInit for NetHeaderInit {
    type Header = NetHeader;

    fn initialize_self(&mut self, mempool: &BufferPoolHeader) -> Result<()> {
        self.buffer_id = unsafe {
            rio_vtable().RegisterBuffer(
                mempool.pool_start_ptr().as_ptr() as *mut i8,
                mempool.pool_size_bytes() as u32,
            )?
        };
        Ok(())
    }
    fn initialize_header(&self, mempool: &BufferPoolHeader, buffer: &RawBufferRef) -> Self::Header {
        let rio_buffer_id = self.buffer_id;
        let data_rio_offset = buffer
            .data_ptr()
            .as_ptr()
            .wrapping_sub(mempool.pool_start_ptr().as_ptr() as usize)
            as ULONG;
        let mut header = Self::Header::default();
        header.data_rio_offset = data_rio_offset;
        header.rio_buffer_id = rio_buffer_id;
        header
    }
}
