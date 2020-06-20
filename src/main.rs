#![warn(clippy::all)]
#![allow(dead_code)]
#![allow(unused)]
// #![cfg_attr(not(feature = "std"), no_std)]
// #[cfg(not(feature = "std"))]
// extern crate alloc;
// #[cfg(feature = "std")]
use std as alloc;

pub mod buffer;
mod cache_padded;
mod iocp;
mod net_api;
mod ring;
mod rio;
mod rio_buf;
use crate::iocp::{IOCPQueue, IOCPQueueBuilder};
use crate::net_api::{
    IOPacketHeader, IOPacketPool, NetContext, NetContextBuilder, UdpSocket, UdpSocketBuilder,
};
use crate::rio_buf::RIOPacketBuf;
use futures_core::stream::Stream;

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
const PACKET_SIZE: u32 = 4096;
const SEND_OPS_IN_FLIGHT: u32 = 64;
const RECV_OPS_IN_FLIGHT: u32 = 64;
const SOCKET_BUF_SIZE: u32 = SEND_OPS_IN_FLIGHT * PACKET_SIZE * 16;
const PACKETS_TO_SEND: usize = 100_000;

async fn recv_loop(
    net_context: NetContext<IOPacketHeader>,
    server_addr: std::net::SocketAddr,
) -> Result<()> {
    println!("start recv loop");
    let server_socket = UdpSocketBuilder::new(server_addr)
        .concurrent_sends(SEND_OPS_IN_FLIGHT)
        .concurrent_receives(RECV_OPS_IN_FLIGHT)
        .build(&net_context)?;
    let mut packets_received = 0;
    use net_api::AsyncBufferReadExt;
    let mut packets = Vec::new();
    let mut recv_stream = server_socket.receive();
    let mut sends = Vec::new();
    loop {
        let num_new_packets = recv_stream.read(&mut packets).await?;
        let len = packets.len();
        for mut recv_packet in packets.drain(0..len) {
            let mut header = recv_packet.mut_header::<RIOPacketBuf>();
            header.set_local_addr(None);
            // use std::io::Read;
            // let mut buf = Vec::new();

            // recv_packet.reader().read_to_end(&mut buf).unwrap();
            sends.push(server_socket.send(recv_packet)?);
            // result?;
        }
        let len = sends.len();
        for send in sends.drain(0..len) {
            send.await?;
        }
        packets_received += num_new_packets;
        if packets_received % 100000 == 0 {
            println!("received {}", packets_received);
        }
    }
}
async fn send_loop(
    net_context: NetContext<IOPacketHeader>,
    server_addr: std::net::SocketAddr,
    client_addr: std::net::SocketAddr,
) -> Result<()> {
    println!("start send loop");
    let client_socket = UdpSocketBuilder::new(client_addr)
        .concurrent_sends(SEND_OPS_IN_FLIGHT)
        .concurrent_receives(RECV_OPS_IN_FLIGHT)
        .build(&net_context)?;
    let mut packets_sent = 0;
    let mut recv_stream = client_socket.receive();
    let mut packets = Vec::new();
    use net_api::AsyncBufferReadExt;
    let mut sends = Vec::new();
    loop {
        for i in 0..SEND_OPS_IN_FLIGHT {
            let mut send_packet = net_context.alloc_buffer().await;
            use std::io::Write;
            send_packet.write(&[5u8; PACKET_SIZE as usize]); // write data to packet
            send_packet
                .mut_header::<RIOPacketBuf>()
                .set_remote_addr(Some(server_addr)); // set remote address of the send op
            sends.push(client_socket.send(send_packet)?);
        }
        let len = sends.len();
        for send in sends.drain(0..len) {
            send.await?;
        }
        let num_new_packets = recv_stream.read(&mut packets).await?;
        packets.clear();
        packets_sent += len;
        if packets_sent % 100000 == 0 {
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
    let num_threads = 8;
    let mut iocp_queues = Vec::new();
    let num_receivers = 1;
    let packet_pool = buffer::default_pool(
        RECV_OPS_IN_FLIGHT as usize
            + SEND_OPS_IN_FLIGHT as usize * num_threads * num_receivers * 2 as usize,
        Layout::from_size_align(PACKET_SIZE as usize, 4096).unwrap(),
        IOPacketPool::default(),
    )?;
    for j in 0..num_receivers {
        let packet_pool = packet_pool.clone();
        let server_addr = format!("127.0.0.1:351{}", j).parse().unwrap();
        let mut iocp_builder = IOCPQueueBuilder::new(1)?;
        let net_context =
            NetContextBuilder::new(&mut iocp_builder, packet_pool.clone()).finish()?;
        let mut iocp = iocp_builder.build()?;
        let spawn_handle = iocp.handle();
        spawn_handle.spawn(async move {
            recv_loop(net_context, server_addr).await.unwrap();
            println!("recv loop died");
        });
        iocp_queues.push(iocp);
    }

    for i in 0..(num_threads - num_receivers) {
        let packet_pool = packet_pool.clone();
        let mut iocp_builder = IOCPQueueBuilder::new(1)?;
        let send_net_context =
            NetContextBuilder::new(&mut iocp_builder, packet_pool.clone()).finish()?;
        let mut iocp = iocp_builder.build()?;
        let spawn_handle = iocp.handle();
        for j in 0..num_receivers {
            let send_net_context = send_net_context.clone();
            let client_addr = format!("127.0.0.1:36{}{}", i, j).parse().unwrap();
            let server_addr = format!("127.0.0.1:351{}", j).parse().unwrap();
            spawn_handle.spawn(async move {
                send_loop(send_net_context, server_addr, client_addr)
                    .await
                    .unwrap();
                println!("send loop died");
            });
        }
        iocp_queues.push(iocp);
    }

    let mut threads = Vec::new();
    for mut iocp in iocp_queues {
        threads.push(std::thread::spawn(move || -> Result<()> {
            loop {
                if iocp.poll(Some(10)).unwrap() {
                    // net_context.poke().unwrap();
                }
            }
            Ok(())
        }));
    }
    for thread in threads {
        thread.join().unwrap();
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

impl<T: ?Sized> StreamExt for T where T: Stream {}
trait StreamExt: Stream {
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Unpin,
    {
        Next::new(self)
    }
}
/// Future for the [`next`](super::StreamExt::next) method.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Next<'a, St: ?Sized> {
    stream: &'a mut St,
}

impl<St: ?Sized + Unpin> Unpin for Next<'_, St> {}

impl<'a, St: ?Sized + Stream + Unpin> Next<'a, St> {
    fn new(stream: &'a mut St) -> Self {
        Next { stream }
    }
}

impl<St: ?Sized + futures_core::stream::FusedStream + Unpin> futures_core::future::FusedFuture
    for Next<'_, St>
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

impl<St: ?Sized + Stream + Unpin> std::future::Future for Next<'_, St> {
    type Output = Option<St::Item>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.stream).poll_next(cx)
    }
}
