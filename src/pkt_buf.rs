#![allow(non_snake_case)]
use crate::alloc::alloc::Layout;
use crate::buffer::{BufferRef, RawBufferRef};
use crate::mempool::MemPool;
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::null_mut;

// impl<T: Default> IntoBufferRef for PacketChainBuf<T> {
//     fn buf_ref(&self) -> &RawBufferRef {
//         self.buf.buf_ref()
//     }
//     fn buf_ref_mut(&mut self) -> &mut RawBufferRef {
//         self.buf.buf_ref_mut()
//     }
//     unsafe fn into_raw(self) -> *mut u8 {
//         self.buf.into_raw()
//     }
//     unsafe fn from_raw(ptr: *mut u8) -> Self
//     where
//         Self: Sized,
//     {
//         Self {
//             buf: BufferRef::<Self>::from_raw(ptr),
//             _marker: core::marker::PhantomData,
//         }
//     }
// }
// impl<T: Default> UserBufferRef for PacketChainBuf<T> {
//     type Header = PacketChainBufHeader<T>;
//     type UserHeader = T;
//     fn header(&self) -> &Self::Header {
//         self.buf.user_header()
//     }
//     fn header_mut(&mut self) -> &mut Self::Header {
//         self.buf.user_header_mut()
//     }
//     fn user_header(&self) -> &Self::UserHeader {
//         &self.header().user
//     }
//     fn user_header_mut(&mut self) -> &mut Self::UserHeader {
//         &mut self.header_mut().user
//     }
// }

// pub struct PacketChainBufHeader<T> {
//     pub(crate) prev: *mut u8, // BufferRef ptr, not owned
//     pub(crate) next: *mut u8, // BufferRef ptr, owned
//     pub(crate) user: T,
// }
// impl<T: Default> Default for PacketChainBufHeader<T> {
//     fn default() -> Self {
//         Self {
//             prev: null_mut(),
//             next: null_mut(),
//             user: Default::default(),
//         }
//     }
// }

// pub trait IntoPacketChainBuf<T: Default>: IntoBufferRef {
//     fn pkt_chain_ref(&self) -> &PacketChainBuf<T>;
//     fn pkt_chain_ref_mut(&mut self) -> &mut PacketChainBuf<T>;
//     // how do we make sure reading/writing a packet chain buffer is as easy as a regular buffer?
//     // do we move reader() from IntoBufferRef into some other trait perhaps?
//     fn append_packet(&mut self, other: Self)
//     where
//         Self: Sized,
//     {
//         debug_assert!(self.pkt_chain_ref().header().next == null_mut());
//         unsafe {
//             let other_ptr = <Self as IntoBufferRef>::into_raw(other);
//             self.pkt_chain_ref_mut().header_mut().next = other_ptr;
//         }
//     }
// }

// pub struct PacketChainBuf<T> {
//     buf: BufferRef<Self>,
//     _marker: core::marker::PhantomData<T>,
// }
