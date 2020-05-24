#![warn(clippy::all)]
#![allow(dead_code)]
#![allow(unused)]
// #![cfg_attr(not(feature = "std"), no_std)]
// #[cfg(not(feature = "std"))]
// extern crate alloc;
// #[cfg(feature = "std")]
use std as alloc;

mod buffer;
mod cache_padded;
mod iocp;
mod mempool;
mod net_api;
mod pkt_buf;
mod ring;
mod rio;
mod rio_buf;
use crate::iocp::{IOCPQueue, IOCPQueueBuilder};
use crate::net_api::{
    IOPacketHeader, IOPacketPool, NetContext, NetContextBuilder, UdpSocket, UdpSocketBuilder,
};
use crate::rio_buf::RIOPacketBuf;

use alloc::alloc::Layout;
use alloc::sync::Arc;
use core::mem::size_of;
use core::ptr::null_mut;
use winapi::{
    ctypes::c_int,
    shared::{
        basetsd::ULONG_PTR,
        minwindef::{DWORD, ULONG},
        ntdef::{PCHAR, PVOID},
        ws2def::{self},
    },
    um::{
        errhandlingapi::GetLastError,
        handleapi::INVALID_HANDLE_VALUE,
        ioapiset,
        minwinbase::OVERLAPPED_ENTRY,
        winbase::INFINITE,
        winnt::HANDLE,
        winsock2::{self},
    },
};

#[derive(Clone, Debug)]
pub enum Error {
    CreateCompletionPortFailed,
    WSAErr(&'static str, c_int),
    WinError(&'static str, DWORD),
    LayoutErr(core::alloc::LayoutErr),
    SlotsExhausted,
    InvalidCompletionObject,
    FuturePanic,
}
impl From<core::alloc::LayoutErr> for Error {
    fn from(err: core::alloc::LayoutErr) -> Self {
        Error::LayoutErr(err)
    }
}
type Result<T> = core::result::Result<T, Error>;

unsafe fn create_completion_port(max_thread_users: u32) -> Result<HANDLE> {
    let handle =
        ioapiset::CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, max_thread_users);
    if handle != INVALID_HANDLE_VALUE {
        Ok(handle)
    } else {
        Err(Error::CreateCompletionPortFailed)
    }
}
const PACKET_SIZE: u32 = 16000;
const SEND_OPS_IN_FLIGHT: u32 = 24;
const RECV_OPS_IN_FLIGHT: u32 = 24;
const SOCKET_BUF_SIZE: u32 = SEND_OPS_IN_FLIGHT * PACKET_SIZE * 1;
const PACKETS_TO_SEND: usize = 100_000;

async fn recv_loop(
    packet_pool: mempool::MemPool<IOPacketPool>,
    net_context: NetContext<IOPacketHeader>,
    server_addr: std::net::SocketAddr,
) -> Result<()> {
    println!("start recv loop");
    let server_socket = UdpSocketBuilder::new().build(&net_context)?;
    server_socket.bind(server_addr)?;
    let mut packets_received = 0;
    loop {
        let recv_packet = packet_pool.alloc().await;
        let recv_packet2 = packet_pool.alloc().await;
        let recv1 = server_socket.receive(recv_packet);
        let recv2 = server_socket.receive(recv_packet2);
        let (recv_packet, result) = recv1.await;
        result?;
        let (recv_packet2, result) = recv2.await;
        result?;
        packets_received += 1;
        if packets_received % 10000 == 0 {
            println!("received {}", packets_received);
        }
    }
}
async fn send_loop(
    packet_pool: mempool::MemPool<IOPacketPool>,
    net_context: NetContext<IOPacketHeader>,
    server_addr: std::net::SocketAddr,
    client_addr: std::net::SocketAddr,
) -> Result<()> {
    println!("start send loop");
    let client_socket = UdpSocketBuilder::new().build(&net_context)?;
    client_socket.bind(client_addr)?;
    let mut packets_sent = 0;
    loop {
        let mut send_packet = packet_pool.alloc().await;
        use std::io::Write;
        send_packet.write(&[5u8; PACKET_SIZE as usize]); // write data to packet
        send_packet
            .header_mut::<RIOPacketBuf>()
            .set_remote_addr(Some(server_addr)); // set remote address of the send op
        let (send_packet, result) = client_socket.send(send_packet).await;
        result?;
        packets_sent += 1;
        if packets_sent % 10000 == 0 {
            println!("sent {}", packets_sent);
        }
    }

    Ok(())
}
fn main() -> Result<()> {
    unsafe {
        rio::wsa_init()?;
        rio::init_rio()?;
    }
    let packet_pool = mempool::default(
        1024,
        Layout::from_size_align(PACKET_SIZE as usize, 64).unwrap(),
        IOPacketPool::default(),
    )?;
    let mut iocp_builder = IOCPQueueBuilder::new(1)?;
    let net_context = NetContextBuilder::<IOPacketHeader>::new(&mut iocp_builder).finish()?;

    let mut iocp = iocp_builder.build()?;
    iocp.poll(Some(0));
    let spawn_handle = iocp.handle();
    let ret_val = spawn_handle.spawn(async {
        println!("hi");
        5
    });
    let server_addr = "127.0.0.1:3512".parse().unwrap();
    let client_addr = "127.0.0.1:3513".parse().unwrap();
    spawn_handle.spawn(async move { println!("done {:?}", ret_val.await) });
    let recv_packet_pool = packet_pool.clone();
    let recv_context = net_context.clone();
    spawn_handle.spawn(async move {
        recv_loop(recv_packet_pool, recv_context, server_addr)
            .await
            .unwrap();
        println!("recv loop died");
    });
    let send_packet_pool = packet_pool.clone();
    let send_net_context = net_context.clone();
    spawn_handle.spawn(async move {
        send_loop(send_packet_pool, send_net_context, server_addr, client_addr)
            .await
            .unwrap();
        println!("send loop died");
    });
    loop {
        if iocp.poll(Some(10)).unwrap() {
            println!("POKE");
            net_context.poke();
        }
    }

    Ok(())
}
// fn main() -> Result<()> {
//     unsafe {
//         let iocp = create_completion_port(1)?;
//         rio::wsa_init()?;
//         rio::init_rio()?;
//         let send_buf = [1u8; PACKET_SIZE as usize];
//         let mut rio_completions: Vec<rio::RIOCompletion<net_api::IOPacketHeader>> =
//             Vec::with_capacity(10000);
//         let mut rio_cq = rio::RIOQueue::new(
//             Some(iocp),
//             net_api::RIO_QUEUE_COMPLETION_KEY,
//             SEND_OPS_IN_FLIGHT * 2 + RECV_OPS_IN_FLIGHT * 2,
//             rio_completions.capacity(),
//         )?;
//         let udp_server_socket = rio::RIOSocket::new_udp(
//             &rio_cq,
//             SEND_OPS_IN_FLIGHT as u32,
//             &rio_cq,
//             RECV_OPS_IN_FLIGHT as u32,
//         )?;
//         udp_server_socket.set_recv_buffer_size(SOCKET_BUF_SIZE as usize);
//         let server_addr: std::net::SocketAddr = "127.0.0.1:3512".parse().unwrap();
//         rio::bind_socket(&udp_server_socket, server_addr)?;

//         // let mut client_sockets = Vec::new();
//         // for i in 0..3 {
//         let i = 0;
//         let udp_client_socket = rio::RIOSocket::new_udp(
//             &rio_cq,
//             SEND_OPS_IN_FLIGHT as u32,
//             &rio_cq,
//             RECV_OPS_IN_FLIGHT as u32,
//         )?;
//         udp_client_socket.set_send_buffer_size(SOCKET_BUF_SIZE as usize);
//         let client_addr: std::net::SocketAddr = format!("127.0.0.1:{}", i + 3513).parse().unwrap();
//         rio::bind_socket(&udp_client_socket, client_addr)?;
//         //     client_sockets.push((udp_client_socket, client_addr));
//         // }

//         let addr_size = size_of::<ws2def::SOCKADDR_IN>();
//         let free_func = |ptr, size| {
//             crate::mempool::virtual_free(ptr, size).unwrap();
//         };
//         let packet_pool = mempool::MemPool::<net_api::IOPacketHeaderInitializer>::new(
//             (RECV_OPS_IN_FLIGHT + SEND_OPS_IN_FLIGHT) as usize,
//             Layout::from_size_align_unchecked(PACKET_SIZE as usize, 64),
//             mempool::virtual_alloc,
//             free_func,
//             net_api::IOPacketHeaderInitializer::default(),
//         )?;
//         let num_receives = RECV_OPS_IN_FLIGHT;
//         for i in 0..num_receives {
//             let mut packet = packet_pool.alloc().expect("Failed to alloc packet");
//             udp_server_socket.receive_ex(packet)?;
//         }
//         udp_server_socket.commit_receive_ex()?;
//         let num_sends = SEND_OPS_IN_FLIGHT;
//         for i in 0..num_sends {
//             let mut packet = packet_pool.alloc().expect("Failed to alloc packet");
//             let net_header: &mut RIOPacketBuf = packet.header_mut();
//             net_header.set_remote_addr(Some(server_addr));
//             // let capacity = packet.raw().buffer_header().data_capacity();
//             // packet.raw_mut().buffer_header_mut().set_data_size(capacity);
//             use std::io::Write;
//             packet.write(&send_buf);
//             udp_client_socket.send_ex(packet)?;
//         }
//         udp_client_socket.commit_send_ex()?;
//         ioapiset::PostQueuedCompletionStatus(iocp, 3, 9, null_mut());

//         let mut completion_entries: [OVERLAPPED_ENTRY; 1024] = [Default::default(); 1024];
//         let mut packets_counted = 0;
//         let mut receives_counted = 0;
//         let mut sends_counted = 0;
//         println!("start!");
//         let mut sends_in_flight = num_sends;
//         let mut receives_in_flight = num_receives;
//         // let mut compare_buf = Vec::new();
//         'outer: loop {
//             let mut num_received_entries = 0;
//             if ioapiset::GetQueuedCompletionStatusEx(
//                 iocp,
//                 completion_entries.as_mut_ptr(),
//                 completion_entries.len() as ULONG,
//                 &mut num_received_entries,
//                 INFINITE,
//                 0,
//             ) != 1
//             {
//                 Err(Error::WinError(
//                     "GetQueuedCompletionStatusEx",
//                     GetLastError(),
//                 ))?
//             } else {
//                 for i in 0..num_received_entries as usize {
//                     let key = completion_entries[i].lpCompletionKey;
//                     if key == net_api::RIO_QUEUE_COMPLETION_KEY {
//                         let rio_queue =
//                             rio::RIOQueue::from_overlapped(&*completion_entries[i].lpOverlapped);
//                         let num_completions =
//                             rio_queue.dequeue_completions(&mut rio_completions)?;
//                         for completion in rio_completions.drain(0..num_completions) {
//                             match completion.op_type {
//                                 rio::RIOOpType::Send => {
//                                     completion.result.expect("Send failed");
//                                     sends_in_flight -= 1;
//                                     sends_counted += 1;
//                                 }
//                                 rio::RIOOpType::Receive => {
//                                     if let Err(err) = completion.result {
//                                         let rio_buf = AsRef::<RIOPacketBuf>::as_ref(
//                                             completion.packet_buf.user_header(),
//                                         )
//                                         .data_rio_buf(
//                                             completion.packet_buf.raw(),
//                                             completion
//                                                 .packet_buf
//                                                 .raw()
//                                                 .buffer_header()
//                                                 .data_capacity(),
//                                         );
//                                         println!(
//                                                 "fail with size {}\n rio buf {} {} end {} \ncalculated offset {}",
//                                                 completion.packet_buf.raw().buffer_header().data_capacity(),
//                                                 rio_buf.Offset,
//                                                 rio_buf.Length,
//                                                 rio_buf.Offset + rio_buf.Length,
//                                                 completion.packet_buf.raw().data_ptr()
//                                     .wrapping_sub(packet_pool.chunk_start_ptr() as usize) as usize
//                                             );
//                                     } else {
//                                         use std::io::Read;
//                                         // compare_buf.clear();
//                                         debug_assert!(
//                                             completion.packet_buf.raw().buffer_header().data_size()
//                                                 as usize
//                                                 == send_buf.len()
//                                         );
//                                         let data_size =
//                                             completion.packet_buf.raw().buffer_header().data_size()
//                                                 as usize;
//                                         // if compare_buf.capacity() < data_size {
//                                         //     compare_buf.reserve(data_size - compare_buf.capacity());
//                                         // }
//                                         // compare_buf.set_len(data_size);

//                                         // completion
//                                         //     .packet_buf
//                                         //     .reader()
//                                         //     .read(&mut compare_buf)
//                                         //     .unwrap();
//                                         // debug_assert!(
//                                         //     compare_buf.len() == send_buf.len(),
//                                         //     "{} != {}",
//                                         //     compare_buf.len(),
//                                         //     send_buf.len()
//                                         // );
//                                         // for i in 0..compare_buf.len() {
//                                         //     debug_assert!(compare_buf[i] == send_buf[i], "at {}", i);
//                                         // }
//                                         receives_counted += 1;
//                                     }
//                                     receives_in_flight -= 1;
//                                 }
//                                 _ => panic!("wat"),
//                             }
//                         }
//                         if receives_counted <= PACKETS_TO_SEND {
//                             let num_sends = SEND_OPS_IN_FLIGHT - sends_in_flight;
//                             for i in 0..num_sends {
//                                 let mut packet =
//                                     packet_pool.alloc().expect("Failed to alloc packet");
//                                 let net_header: &mut RIOPacketBuf = packet.header_mut();
//                                 net_header.set_remote_addr(Some(server_addr));
//                                 // let capacity = packet.raw().buffer_header().data_capacity();
//                                 // packet.raw_mut().buffer_header_mut().set_data_size(capacity);
//                                 use std::io::Write;
//                                 packet.write(&send_buf);
//                                 udp_client_socket.send_ex(packet)?;
//                             }
//                             if num_sends > 0 {
//                                 udp_client_socket.commit_send_ex()?;
//                             }
//                             sends_in_flight += num_sends;
//                         }
//                         let num_receives = RECV_OPS_IN_FLIGHT - receives_in_flight;
//                         for i in 0..num_receives {
//                             let packet = packet_pool.alloc().expect("Failed to alloc packet");
//                             udp_server_socket.receive_ex(packet)?;
//                         }
//                         if num_receives > 0 {
//                             udp_server_socket.commit_receive_ex()?;
//                         }
//                         receives_in_flight += num_receives;
//                         packets_counted += num_completions;
//                         if sends_in_flight == 0 && receives_counted >= PACKETS_TO_SEND {
//                             println!(
//                                 "done! counted {} completions, received {} packets, sent {} packets",
//                                 packets_counted, receives_counted, sends_counted
//                             );
//                             break 'outer;
//                         }
//                         if num_completions > 0 {
//                             rio_queue.poke();
//                         }
//                     }
//                 }
//             }
//         }
//         drop(packet_pool);
//     }
//     Ok(())
// }
