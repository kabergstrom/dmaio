#![allow(non_snake_case)]
use crate::alloc::sync::Arc;
use crate::buffer::BufferRef;
use crate::buffer::{IntoBufferRef, UserBufferRef};
use crate::rio_buf::RIOPacketBuf;
use crate::{Error, Result};
use core::mem::{size_of, MaybeUninit};
use core::ptr::null_mut;
use winapi::{
    ctypes::c_int,
    shared::{
        guiddef::GUID,
        minwindef::{DWORD, MAKEWORD, ULONG},
        ntdef::{PCHAR, PVOID},
        ws2def::{self, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER},
    },
    um::{
        minwinbase::OVERLAPPED,
        mswsock::{
            self, RIO_EXTENSION_FUNCTION_TABLE, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION,
        },
        winnt::HANDLE,
        winsock2::{self, SOCKET},
    },
};

use winapi::shared::mswsockdef::{
    PRIORESULT, PRIO_BUF, RIORESULT, RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ,
    RIO_INVALID_BUFFERID, RIO_INVALID_CQ, RIO_INVALID_RQ, RIO_MSG_COMMIT_ONLY, RIO_MSG_DEFER,
    RIO_RQ,
};
use winapi::shared::winerror::ERROR_SUCCESS;
use winapi::um::mswsock::PRIO_NOTIFICATION_COMPLETION;

#[derive(Clone, Copy, PartialEq)]
pub enum RIOOpType {
    None,
    Send,
    Receive,
}
impl Default for RIOOpType {
    fn default() -> Self {
        RIOOpType::None
    }
}

static mut RIO_VTABLE: MaybeUninit<RIOVTable> = MaybeUninit::uninit();
pub struct RIOVTable(RIO_EXTENSION_FUNCTION_TABLE);
impl RIOVTable {
    pub(crate) unsafe fn Receive(
        &self,
        SocketQueue: RIO_RQ,
        pData: PRIO_BUF,
        DataBufferCount: ULONG,
        Flags: DWORD,
        RequestContext: PVOID,
    ) -> Result<()> {
        if (self.0.RIOReceive.unwrap())(SocketQueue, pData, DataBufferCount, Flags, RequestContext)
            != 1
        {
            Err(wsa_err("RIOReceive"))
        } else {
            Ok(())
        }
    }
    pub(crate) unsafe fn ReceiveEx(
        &self,
        SocketQueue: RIO_RQ,
        pData: PRIO_BUF,
        DataBufferCount: ULONG,
        pLocalAddress: PRIO_BUF,
        pRemoteAddress: PRIO_BUF,
        pControlContext: PRIO_BUF,
        pFlags: PRIO_BUF,
        Flags: DWORD,
        RequestContext: PVOID,
    ) -> Result<()> {
        if (self.0.RIOReceiveEx.unwrap())(
            SocketQueue,
            pData,
            DataBufferCount,
            pLocalAddress,
            pRemoteAddress,
            pControlContext,
            pFlags,
            Flags,
            RequestContext,
        ) != 1
        {
            Err(wsa_err("RIOReceiveEx"))
        } else {
            Ok(())
        }
    }

    pub(crate) unsafe fn Send(
        &self,
        SocketQueue: RIO_RQ,
        pData: PRIO_BUF,
        DataBufferCount: ULONG,
        Flags: DWORD,
        RequestContext: PVOID,
    ) -> Result<()> {
        if (self.0.RIOSend.unwrap())(SocketQueue, pData, DataBufferCount, Flags, RequestContext)
            != 1
        {
            Err(wsa_err("RIOSend"))
        } else {
            Ok(())
        }
    }

    pub(crate) unsafe fn SendEx(
        &self,
        SocketQueue: RIO_RQ,
        pData: PRIO_BUF,
        DataBufferCount: ULONG,
        pLocalAddress: PRIO_BUF,
        pRemoteAddress: PRIO_BUF,
        pControlContext: PRIO_BUF,
        pFlags: PRIO_BUF,
        Flags: DWORD,
        RequestContext: PVOID,
    ) -> Result<()> {
        if (self.0.RIOSendEx.unwrap())(
            SocketQueue,
            pData,
            DataBufferCount,
            pLocalAddress,
            pRemoteAddress,
            pControlContext,
            pFlags,
            Flags,
            RequestContext,
        ) != 1
        {
            Err(wsa_err("RIOSendEx"))
        } else {
            Ok(())
        }
    }
    pub(crate) unsafe fn CloseCompletionQueue(&self, CQ: RIO_CQ) {
        (self.0.RIOCloseCompletionQueue.unwrap())(CQ)
    }
    pub(crate) unsafe fn CreateCompletionQueue(
        &self,
        QueueSize: DWORD,
        NotificationCompletion: PRIO_NOTIFICATION_COMPLETION,
    ) -> Result<RIO_CQ> {
        let cq = (self.0.RIOCreateCompletionQueue.unwrap())(QueueSize, NotificationCompletion);
        if cq == RIO_INVALID_CQ {
            Err(wsa_err("RIOCreateCompletionQueue"))
        } else {
            Ok(cq)
        }
    }
    pub(crate) unsafe fn CreateRequestQueue(
        &self,
        Socket: SOCKET,
        MaxOutstandingReceive: ULONG,
        MaxReceiveDataBuffers: ULONG,
        MaxOutstandingSend: ULONG,
        MaxSendDataBuffers: ULONG,
        ReceiveCQ: RIO_CQ,
        SendCQ: RIO_CQ,
        SocketContext: PVOID,
    ) -> Result<RIO_RQ> {
        let rq = (self.0.RIOCreateRequestQueue.unwrap())(
            Socket,
            MaxOutstandingReceive,
            MaxReceiveDataBuffers,
            MaxOutstandingSend,
            MaxSendDataBuffers,
            ReceiveCQ,
            SendCQ,
            SocketContext,
        );
        if rq == RIO_INVALID_RQ {
            Err(wsa_err("RIOCreateRequestQueue"))
        } else {
            Ok(rq)
        }
    }
    pub(crate) unsafe fn DequeueCompletion(
        &self,
        CQ: RIO_CQ,
        Array: PRIORESULT,
        ArraySize: ULONG,
    ) -> Result<ULONG> {
        let retval = (self.0.RIODequeueCompletion.unwrap())(CQ, Array, ArraySize);
        if retval == RIO_CORRUPT_CQ {
            Err(wsa_err("RIODequeueCompletion"))
        } else {
            Ok(retval)
        }
    }
    pub(crate) unsafe fn DeregisterBuffer(&self, BufferID: RIO_BUFFERID) {
        (self.0.RIODeregisterBuffer.unwrap())(BufferID)
    }
    pub(crate) unsafe fn RegisterBuffer(
        &self,
        DataBuffer: PCHAR,
        DataLength: DWORD,
    ) -> Result<RIO_BUFFERID> {
        let retval = (self.0.RIORegisterBuffer.unwrap())(DataBuffer, DataLength);
        if RIO_INVALID_BUFFERID == retval {
            Err(wsa_err("RIORegisterBuffer"))
        } else {
            Ok(retval)
        }
    }
    pub(crate) unsafe fn Notify(&self, CQ: RIO_CQ) -> Result<()> {
        let retval = (self.0.RIONotify.unwrap())(CQ);
        if winsock2::WSAEALREADY == retval {
            Ok(())
        } else if ERROR_SUCCESS != retval as u32 {
            Err(Error::WSAErr("RIONotify", retval))
        } else {
            Ok(())
        }
    }
    pub(crate) unsafe fn ResizeCompletionQueue(&self, CQ: RIO_CQ, QueueSize: DWORD) -> Result<()> {
        let retval = (self.0.RIOResizeCompletionQueue.unwrap())(CQ, QueueSize);
        if retval != 1 {
            Err(wsa_err("RIOResizeCompletionQueue"))
        } else {
            Ok(())
        }
    }
    pub(crate) unsafe fn ResizeRequestQueue(
        &self,
        RQ: RIO_RQ,
        MaxOutstandingReceive: DWORD,
        MaxOutstandingSend: DWORD,
    ) -> Result<()> {
        let retval =
            (self.0.RIOResizeRequestQueue.unwrap())(RQ, MaxOutstandingReceive, MaxOutstandingSend);
        if retval != 1 {
            Err(wsa_err("RIOResizeCompletionQueue"))
        } else {
            Ok(())
        }
    }
}

pub unsafe fn rio_vtable<'a>() -> &'a RIOVTable {
    &*RIO_VTABLE.as_ptr()
}
pub unsafe fn init_rio() -> Result<()> {
    let socket = create_socket(
        winsock2::SOCK_DGRAM,
        ws2def::IPPROTO_UDP as c_int,
        winsock2::WSA_FLAG_REGISTERED_IO,
    )?;

    let mut output_size = 0;
    let mut rio_guid = mswsock::WSAID_MULTIPLE_RIO;
    let rio_guid_size = size_of::<GUID>() as u32;
    let mut vtable = RIO_EXTENSION_FUNCTION_TABLE::default();
    let vtable_size = size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32;
    let result = winsock2::WSAIoctl(
        socket,
        SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
        &mut rio_guid as *mut GUID as *mut _,
        rio_guid_size,
        &mut vtable as *mut RIO_EXTENSION_FUNCTION_TABLE as *mut _,
        vtable_size,
        &mut output_size,
        null_mut(),
        None,
    );
    if winsock2::closesocket(socket) != 0 {
        return Err(wsa_err("rio: closesocket"));
    }
    if result != 0 {
        Err(wsa_err("rio: WSAIoctl"))
    } else {
        let vtable = RIOVTable(vtable);
        core::ptr::write(RIO_VTABLE.as_mut_ptr(), vtable);
        Ok(())
    }
}

#[derive(Clone)]
pub struct RIOSocket {
    pub socket: SOCKET,
    pub rq: RIO_RQ,
    pub send_cq: RIO_CQ,
    pub receive_cq: RIO_CQ,
}
impl RIOSocket {
    pub fn receive_ex<T: Default>(&self, mut packet: RIOPacketBuf<T>) -> Result<()> {
        unsafe {
            let mut data_buf = packet.data_rio_buf();
            let mut local_addr_buf = packet.local_addr_rio_buf();
            let mut remote_addr_buf = packet.remote_addr_rio_buf();
            let local_addr_param = if packet.local_addr().is_some() {
                &mut local_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            let remote_addr_param = if packet.remote_addr().is_some() {
                &mut remote_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            packet.header_mut().active_op = RIOOpType::Receive;
            rio_vtable().ReceiveEx(
                self.rq,
                &mut data_buf as PRIO_BUF,
                1,
                local_addr_param,
                remote_addr_param,
                null_mut(),
                null_mut(),
                RIO_MSG_DEFER,
                packet.into_raw() as PVOID,
            )?;
            Ok(())
        }
    }
    pub fn commit_receive_ex(&self) -> Result<()> {
        unsafe {
            rio_vtable().ReceiveEx(
                self.rq,
                null_mut(),
                0,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                RIO_MSG_COMMIT_ONLY,
                null_mut(),
            )?;
            rio_vtable().Notify(self.send_cq)?;
        }
        Ok(())
    }
    pub fn commit_send_ex(&self) -> Result<()> {
        unsafe {
            rio_vtable().SendEx(
                self.rq,
                null_mut(),
                0,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                RIO_MSG_COMMIT_ONLY,
                null_mut(),
            )?;
            rio_vtable().Notify(self.send_cq)?;
        }
        Ok(())
    }
    pub fn send_ex<T: Default>(&self, mut packet: RIOPacketBuf<T>) -> Result<()> {
        unsafe {
            let mut data_buf = packet.data_rio_buf();
            let mut local_addr_buf = packet.local_addr_rio_buf();
            let mut remote_addr_buf = packet.remote_addr_rio_buf();
            let local_addr_param = if packet.local_addr().is_some() {
                &mut local_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            let remote_addr_param = if packet.remote_addr().is_some() {
                &mut remote_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            packet.header_mut().active_op = RIOOpType::Send;
            rio_vtable().SendEx(
                self.rq,
                &mut data_buf as PRIO_BUF,
                1,
                local_addr_param,
                remote_addr_param,
                null_mut(),
                null_mut(),
                RIO_MSG_DEFER,
                packet.into_raw() as PVOID,
            )?;
            Ok(())
        }
    }
}
pub unsafe fn create_rio_socket(
    _type: c_int,
    protocol: c_int,
    flags: DWORD,
    receive_cq: &RIOQueue,
    send_cq: &RIOQueue,
    max_outstanding_receive: ULONG,
    max_outstanding_send: ULONG,
) -> Result<RIOSocket> {
    let socket = create_socket(
        _type,
        protocol,
        flags | winsock2::WSA_FLAG_REGISTERED_IO | winsock2::WSA_FLAG_OVERLAPPED,
    )?;

    // let mut buffer_size = 2048 * 1024;
    // let buffer_sizeof = size_of::<i32>() as i32;
    // debug_assert!(
    //     winsock2::setsockopt(
    //         socket,
    //         ws2def::SOL_SOCKET,
    //         ws2def::SO_SNDBUF,
    //         &mut buffer_size as *mut _ as PCHAR,
    //         buffer_sizeof,
    //     ) == 0
    // );
    // debug_assert!(
    //     winsock2::setsockopt(
    //         socket,
    //         ws2def::SOL_SOCKET,
    //         ws2def::SO_RCVBUF,
    //         &mut buffer_size as *mut _ as PCHAR,
    //         buffer_sizeof,
    //     ) == 0
    // );
    match rio_vtable().CreateRequestQueue(
        socket,
        max_outstanding_receive,
        1,
        max_outstanding_send,
        1,
        receive_cq.cq,
        send_cq.cq,
        socket as *mut _,
    ) {
        Ok(rq) => Ok(RIOSocket {
            socket,
            rq,
            send_cq: send_cq.cq,
            receive_cq: receive_cq.cq,
        }),
        Err(err) => {
            winsock2::closesocket(socket);
            Err(err)
        }
    }
}

unsafe fn wsa_err(context: &'static str) -> Error {
    Error::WSAErr(context, winsock2::WSAGetLastError())
}
pub unsafe fn create_socket(_type: c_int, protocol: c_int, flags: DWORD) -> Result<SOCKET> {
    let socket = winsock2::WSASocketA(ws2def::AF_INET, _type, protocol, null_mut(), 0, flags);
    if socket == winsock2::INVALID_SOCKET {
        Err(wsa_err("WSASocketA"))
    } else {
        Ok(socket)
    }
}

pub unsafe fn wsa_init() -> Result<()> {
    let mut wsadata = MaybeUninit::<winsock2::WSADATA>::uninit();
    let version = MAKEWORD(2, 2);
    let err_code = winsock2::WSAStartup(version, wsadata.as_mut_ptr());
    if err_code != 0 {
        Err(Error::WSAErr("WSAStartup", err_code))
    } else {
        Ok(())
    }
}

pub struct RIOCompletion<T> {
    pub packet_buf: RIOPacketBuf<T>,
    pub result: Result<()>,
    pub op_type: RIOOpType,
}

#[derive(Clone)]
pub struct RIOQueue {
    // this OVERLAPPED struct is used when the RIO_CQ is bound with an IO Completion Port,
    // and a pointer to it will be passed in the IO completion packet by RIONotify
    iocp_overlapped: MaybeUninit<OVERLAPPED>,
    pub cq: RIO_CQ,
    results_buffer: Vec<RIORESULT>,
}
impl RIOQueue {
    pub unsafe fn dequeue_completions<T: Default>(
        &mut self,
        results_buf: &mut Vec<RIOCompletion<T>>,
    ) -> Result<usize> {
        debug_assert!(results_buf.is_empty());
        debug_assert!(results_buf.len() <= self.results_buffer.capacity());
        let num_completions = rio_vtable().DequeueCompletion(
            self.cq,
            self.results_buffer.as_mut_ptr(),
            self.results_buffer.capacity() as u32,
        )? as usize;
        self.results_buffer.set_len(num_completions);
        for i in 0..num_completions {
            let rio_result = self.results_buffer.get_unchecked(i);
            let mut packet_buf = RIOPacketBuf::<T>::from_raw(rio_result.RequestContext as *mut u8);
            let result = if rio_result.Status == 0 {
                Ok(())
            } else {
                Err(Error::WSAErr("RIORESULT", rio_result.Status))
            };
            let header_mut = packet_buf.header_mut();
            let op_type = header_mut.active_op;
            header_mut.active_op = RIOOpType::None;
            let completion = RIOCompletion {
                packet_buf,
                result,
                op_type,
            };
            core::ptr::write(results_buf.as_mut_ptr().add(i), completion);
        }
        results_buf.set_len(num_completions);
        Ok(num_completions)
    }
}

pub unsafe fn create_rio_cq(
    iocp_notify: Option<HANDLE>,
    queue_size: usize,
    results_buffer_size: usize,
) -> Result<Arc<RIOQueue>> {
    let mut rio_queue_alloc = Arc::new(RIOQueue {
        iocp_overlapped: MaybeUninit::zeroed(),
        cq: RIO_INVALID_CQ,
        results_buffer: Vec::with_capacity(results_buffer_size),
    });
    let mut iocp_registration = RIO_NOTIFICATION_COMPLETION::default();
    let rio_queue_alloc_mut = Arc::make_mut(&mut rio_queue_alloc);
    let notification = if let Some(iocp) = iocp_notify {
        iocp_registration.Type = RIO_IOCP_COMPLETION;
        iocp_registration.u.Iocp_mut().IocpHandle = iocp;
        iocp_registration.u.Iocp_mut().Overlapped =
            rio_queue_alloc_mut.iocp_overlapped.as_mut_ptr() as *mut _;
        &mut iocp_registration as *mut _
    } else {
        null_mut()
    };
    rio_queue_alloc_mut.cq = rio_vtable().CreateCompletionQueue(queue_size as u32, notification)?;
    Ok(rio_queue_alloc)
}

pub unsafe fn bind_socket(socket: &RIOSocket, socket_addr: std::net::SocketAddr) -> Result<()> {
    let mut addr = ws2def::SOCKADDR_STORAGE::default();
    write_sockaddr(&mut addr, Some(socket_addr));

    if winsock2::bind(
        socket.socket,
        &addr as *const _ as *const _,
        size_of::<ws2def::SOCKADDR_STORAGE>() as c_int,
    ) != 0
    {
        Err(wsa_err("bind"))?
    } else {
        Ok(())
    }
}

pub(crate) unsafe fn write_sockaddr(
    ptr: &mut ws2def::SOCKADDR_STORAGE,
    socket_addr: Option<std::net::SocketAddr>,
) {
    if let Some(socket_addr) = socket_addr {
        match socket_addr {
            std::net::SocketAddr::V4(addr) => {
                let mut sockaddr = ws2def::SOCKADDR_IN::default();
                sockaddr.sin_port = addr.port().swap_bytes();
                sockaddr.sin_family = ws2def::AF_INET as u16;
                let src_bytes = addr.ip().octets();
                let mut dst_bytes = sockaddr.sin_addr.S_un.S_un_b_mut();
                dst_bytes.s_b1 = src_bytes[0];
                dst_bytes.s_b2 = src_bytes[1];
                dst_bytes.s_b3 = src_bytes[2];
                dst_bytes.s_b4 = src_bytes[3];
                core::ptr::write(
                    ptr as *mut ws2def::SOCKADDR_STORAGE as *mut ws2def::SOCKADDR_IN,
                    sockaddr,
                );
            }
            std::net::SocketAddr::V6(_) => unimplemented!(),
        }
    } else {
        *ptr = ws2def::SOCKADDR_STORAGE::default();
    }
}

pub(crate) unsafe fn read_sockaddr(
    addr: &ws2def::SOCKADDR_STORAGE,
) -> Option<std::net::SocketAddr> {
    match addr.ss_family as i32 {
        ws2def::AF_INET => {
            let inet_addr = &*(addr as *const _ as *const ws2def::SOCKADDR_IN);
            let addr_bytes = inet_addr.sin_addr.S_un.S_un_b();
            Some(std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::new(
                    addr_bytes.s_b1,
                    addr_bytes.s_b2,
                    addr_bytes.s_b3,
                    addr_bytes.s_b4,
                ),
                inet_addr.sin_port,
            )))
        }
        0 => None,
        _ => unimplemented!(),
    }
}
