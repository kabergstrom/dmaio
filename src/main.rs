#![allow(dead_code)]
#![allow(unused)]
// #![cfg_attr(not(feature = "std"), no_std)]
// #[cfg(not(feature = "std"))]
// extern crate alloc;
// #[cfg(feature = "std")]
use std as alloc;

mod buffer;
mod cache_padded;
mod mempool;
mod ring;
mod rio;
mod rio_buf;

use alloc::alloc::Layout;
use alloc::sync::Arc;
use core::mem::size_of;
use core::ptr::null_mut;
use winapi::{
    ctypes::c_int,
    shared::{
        minwindef::{DWORD, ULONG},
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
fn main() -> Result<()> {
    unsafe {
        let iocp = create_completion_port(1)?;
        rio::wsa_init()?;
        rio::init_rio()?;
        let mut rio_completions: Vec<rio::RIOCompletion<()>> = Vec::with_capacity(10000);
        let send_ops_in_flight = 64;
        let recv_ops_in_flight = 64;
        let mut rio_cq = rio::create_rio_cq(
            Some(iocp),
            send_ops_in_flight as usize * 2 + recv_ops_in_flight as usize * 2,
            rio_completions.capacity(),
        )?;
        let mut rio_cq2 = rio::create_rio_cq(
            Some(iocp),
            send_ops_in_flight as usize * 2 + recv_ops_in_flight as usize * 2,
            rio_completions.capacity(),
        )?;
        let udp_server_socket = rio::create_rio_socket(
            winsock2::SOCK_DGRAM,
            ws2def::IPPROTO_UDP as c_int,
            0,
            &rio_cq,
            &rio_cq,
            recv_ops_in_flight,
            send_ops_in_flight,
        )?;
        let server_addr: std::net::SocketAddr = "127.0.0.1:3512".parse().unwrap();
        rio::bind_socket(&udp_server_socket, server_addr)?;

        let udp_client_socket = rio::create_rio_socket(
            winsock2::SOCK_DGRAM,
            ws2def::IPPROTO_UDP as c_int,
            0,
            &rio_cq,
            &rio_cq,
            recv_ops_in_flight,
            send_ops_in_flight,
        )?;
        let client_addr: std::net::SocketAddr = "127.0.0.1:3513".parse().unwrap();
        rio::bind_socket(&udp_client_socket, client_addr)?;

        let addr_size = size_of::<ws2def::SOCKADDR_IN>();
        let packet_pool = rio_buf::RIOPacketPool::<()>::new(
            (recv_ops_in_flight * 3 + send_ops_in_flight * 3) as usize,
            Layout::from_size_align_unchecked(8000, 64),
        )?;
        let num_receives = recv_ops_in_flight;
        for i in 0..num_receives {
            let packet = packet_pool.alloc().expect("Failed to alloc packet");
            udp_server_socket.receive_ex(packet)?;
        }
        udp_server_socket.commit_receive_ex()?;
        let num_sends = send_ops_in_flight;
        for i in 0..num_sends {
            let mut packet = packet_pool.alloc().expect("Failed to alloc packet");
            packet.set_remote_addr(Some(server_addr));
            udp_client_socket.send_ex(packet)?;
        }
        udp_client_socket.commit_send_ex()?;
        ioapiset::PostQueuedCompletionStatus(iocp, 3, 9, null_mut());

        let mut completion_entries: [OVERLAPPED_ENTRY; 1024] = [Default::default(); 1024];
        let rio_queue_mut = Arc::make_mut(&mut rio_cq);
        let mut packets_counted = 0;
        let mut receives_counted = 0;
        println!("start!");
        let mut sends_in_flight = num_sends;
        let mut receives_in_flight = num_receives;
        loop {
            let mut num_received_entries = 0;
            if ioapiset::GetQueuedCompletionStatusEx(
                iocp,
                completion_entries.as_mut_ptr(),
                completion_entries.len() as ULONG,
                &mut num_received_entries,
                INFINITE,
                0,
            ) != 1
            {
                Err(Error::WinError(
                    "GetQueuedCompletionStatusEx",
                    GetLastError(),
                ))?
            } else {
                let mut received_rio = false;
                for i in 0..num_received_entries as usize {
                    if completion_entries[i].lpCompletionKey == 0 {
                        received_rio = true;
                    }
                    // println!(
                    //     "Got an entry! key {} bytes sent {}",
                    //     completion_entries[i].lpCompletionKey,
                    //     completion_entries[i].dwNumberOfBytesTransferred
                    // );
                }
                if received_rio {
                    let num_completions =
                        rio_queue_mut.dequeue_completions(&mut rio_completions)?;

                    for completion in rio_completions.drain(0..num_completions) {
                        completion.result.unwrap();
                        match completion.op_type {
                            rio::RIOOpType::Send => {
                                sends_in_flight -= 1;
                            }
                            rio::RIOOpType::Receive => {
                                receives_in_flight -= 1;
                                receives_counted += 1;
                            }
                            _ => panic!("wat"),
                        }
                    }
                    let num_sends = send_ops_in_flight - sends_in_flight;
                    for i in 0..num_sends {
                        let mut packet = packet_pool.alloc().expect("Failed to alloc packet");
                        packet.set_remote_addr(Some(server_addr));
                        // core::ptr::write_bytes(packet.data_ptr(), 7, packet.data_capacity());
                        udp_client_socket.send_ex(packet)?;
                    }
                    if num_sends > 0 {
                        udp_client_socket.commit_send_ex()?;
                    }
                    let num_receives = recv_ops_in_flight - receives_in_flight;
                    for i in 0..num_receives {
                        let packet = packet_pool.alloc().expect("Failed to alloc packet");
                        udp_server_socket.receive_ex(packet)?;
                    }
                    if num_receives > 0 {
                        udp_server_socket.commit_receive_ex()?;
                    }
                    sends_in_flight += num_sends;
                    receives_in_flight += num_receives;
                    packets_counted += num_completions;
                    if receives_counted > 500000 {
                        break;
                    }
                }
            }
        }
        println!(
            "done! counted {} completions, received {} packets",
            packets_counted, receives_counted
        );
    }
    Ok(())
}
