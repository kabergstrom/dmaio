#![allow(non_snake_case)]
use crate::alloc::sync::Arc;
use crate::buffer::{BufferRef, RawBufferRef, UserHeader};
use crate::rio_buf::RIOPacketBuf;
use crate::{Error, Result};
use core::mem::{size_of, MaybeUninit};
use core::ptr::{null_mut, NonNull};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::task::Waker;
use winapi::{
    ctypes::c_int,
    shared::{
        guiddef::GUID,
        minwindef::{DWORD, MAKEWORD, ULONG},
        ntdef::{PCHAR, PVOID},
        ws2def::{self, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER},
    },
    um::{
        minwinbase::{OVERLAPPED_u, OVERLAPPED},
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
    RIO_MSG_DONT_NOTIFY, RIO_RQ,
};
use winapi::shared::winerror::ERROR_SUCCESS;
use winapi::um::mswsock::PRIO_NOTIFICATION_COMPLETION;

#[derive(Debug, Clone, Copy, PartialEq)]
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

struct RIOSocketState {
    pub send_slots_open: u32,
    pub pending_send_wakers: SmallVec<[Waker; 1]>,
    pub recv_slots_open: u32,
    pub pending_recv_wakers: SmallVec<[Waker; 1]>,
}

struct RIOSocketInner {
    pub socket: SOCKET,
    pub rq: RIO_RQ,
    pub send_cq: RIOQueue,
    pub receive_cq: RIOQueue,
    pub total_send_slots: u32,
    pub total_recv_slots: u32,
    // the state mutex is used to protect usage of the RIO_RQ for SendEx and ReceiveEx
    pub state: Mutex<RIOSocketState>,
}
unsafe impl Send for RIOSocketInner {}
unsafe impl Sync for RIOSocketInner {}
impl Drop for RIOSocketInner {
    fn drop(&mut self) {
        self.send_cq.release_queue_slots(self.total_send_slots);
        self.receive_cq.release_queue_slots(self.total_recv_slots);
        unsafe { winsock2::closesocket(self.socket) };
    }
}
#[derive(Clone)]
pub struct RIOSocket(Arc<RIOSocketInner>);
impl RIOSocket {
    pub fn new_tcp(
        send_queue: &RIOQueue,
        concurrent_sends: u32,
        receive_queue: &RIOQueue,
        concurrent_receives: u32,
    ) -> Result<Self> {
        Self::new_internal(
            winsock2::SOCK_STREAM,
            ws2def::IPPROTO_TCP as c_int,
            0,
            send_queue,
            concurrent_sends,
            receive_queue,
            concurrent_receives,
        )
    }
    pub fn new_udp(
        send_queue: &RIOQueue,
        concurrent_sends: u32,
        receive_queue: &RIOQueue,
        concurrent_receives: u32,
    ) -> Result<Self> {
        Self::new_internal(
            winsock2::SOCK_DGRAM,
            ws2def::IPPROTO_UDP as c_int,
            0,
            send_queue,
            concurrent_sends,
            receive_queue,
            concurrent_receives,
        )
    }
    fn new_internal(
        _type: c_int,
        protocol: c_int,
        flags: DWORD,
        send_queue: &RIOQueue,
        concurrent_sends: u32,
        receive_queue: &RIOQueue,
        concurrent_receives: u32,
    ) -> Result<Self> {
        send_queue.reserve_queue_slots(concurrent_sends)?;
        match receive_queue.reserve_queue_slots(concurrent_receives) {
            Err(err) => {
                send_queue.release_queue_slots(concurrent_sends);
                Err(err)?
            }
            Ok(()) => {}
        }

        // creating a rio socket has to lock the CQ
        match unsafe {
            create_rio_socket(
                _type,
                protocol,
                flags,
                receive_queue.cq(),
                send_queue.cq(),
                concurrent_receives,
                concurrent_sends,
            )
        } {
            Err(err) => {
                send_queue.release_queue_slots(concurrent_sends);
                receive_queue.release_queue_slots(concurrent_receives);
                Err(err)?
            }
            Ok((socket, rq)) => Ok(RIOSocket(Arc::new(RIOSocketInner {
                socket,
                rq,
                send_cq: send_queue.clone(),
                receive_cq: receive_queue.clone(),
                total_recv_slots: concurrent_receives,
                total_send_slots: concurrent_sends,
                state: Mutex::new(RIOSocketState {
                    send_slots_open: concurrent_sends,
                    recv_slots_open: concurrent_receives,
                    pending_recv_wakers: SmallVec::new(),
                    pending_send_wakers: SmallVec::new(),
                }),
            }))),
        }
    }

    pub fn receive_ex<T: UserHeader>(
        &self,
        mut packet: BufferRef<T>,
        waker: Option<&Waker>,
    ) -> (Result<()>, Option<BufferRef<T>>)
    where
        <T as UserHeader>::Mutable: AsMut<RIOPacketBuf>,
    {
        let mut state_guard = self.0.state.lock();
        if state_guard.recv_slots_open == 0 {
            if let Some(waker) = waker {
                state_guard.pending_recv_wakers.push(waker.clone());
            }
            return (Err(Error::SlotsExhausted), Some(packet));
        }
        // println!("start op {:?}", RIOOpType::Receive);
        let (_, mut header, raw) = packet.parts();
        let mut header = header.as_mut();
        unsafe {
            let mut data_buf = header.data_rio_buf(raw, raw.buffer_header().data_capacity());
            let mut local_addr_buf = header.local_addr_rio_buf(raw);
            let mut remote_addr_buf = header.remote_addr_rio_buf(raw);
            let local_addr_param = &mut local_addr_buf as PRIO_BUF;
            let remote_addr_param = &mut remote_addr_buf as PRIO_BUF;
            header.active_op = RIOOpType::Receive;
            header.op_socket = Some(self.clone());
            let packet_ptr = packet.into_raw().into_raw().as_ptr();
            if let Err(err) = rio_vtable().ReceiveEx(
                self.0.rq,
                &mut data_buf as PRIO_BUF,
                1,
                local_addr_param,
                remote_addr_param,
                null_mut(),
                null_mut(),
                RIO_MSG_DEFER,
                packet_ptr as PVOID,
            ) {
                return (
                    Err(err),
                    Some(BufferRef::from_raw(RawBufferRef::from_raw(
                        NonNull::new_unchecked(packet_ptr),
                    ))),
                );
            }
            state_guard.recv_slots_open -= 1;
            (Ok(()), None)
        }
    }
    pub fn commit_receive_ex(&self) -> Result<()> {
        unsafe {
            rio_vtable().ReceiveEx(
                self.0.rq,
                null_mut(),
                0,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                RIO_MSG_COMMIT_ONLY,
                null_mut(),
            )?;
            Self::notify(self.0.receive_cq.cq());
        }
        Ok(())
    }
    fn notify(cq: RIO_CQ) {
        unsafe {
            rio_vtable().Notify(cq);
        }
    }
    pub fn commit_send_ex(&self) -> Result<()> {
        unsafe {
            rio_vtable().SendEx(
                self.0.rq,
                null_mut(),
                0,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                RIO_MSG_COMMIT_ONLY,
                null_mut(),
            )?;
            Self::notify(self.0.send_cq.cq());
        }
        Ok(())
    }
    fn complete_send(&self) {
        let mut state_guard = self.0.state.lock();
        state_guard.send_slots_open += 1;
        if let Some(waker) = state_guard.pending_send_wakers.pop() {
            waker.wake();
        }
    }
    fn complete_receive(&self) {
        let mut state_guard = self.0.state.lock();
        state_guard.recv_slots_open += 1;
        if let Some(waker) = state_guard.pending_recv_wakers.pop() {
            waker.wake();
        }
    }
    pub fn send_ex<T: UserHeader>(
        &self,
        mut packet: BufferRef<T>,
        waker: Option<&Waker>,
    ) -> Result<()>
    where
        <T as UserHeader>::Mutable: AsMut<RIOPacketBuf>,
    {
        let mut state_guard = self.0.state.lock();
        if state_guard.send_slots_open == 0 {
            if let Some(waker) = waker {
                state_guard.pending_send_wakers.push(waker.clone());
            }
            println!("send exhausted");
            return Err(Error::SlotsExhausted);
        }
        let (_, mut_segment, raw) = packet.parts();
        let mut header = mut_segment.as_mut();
        unsafe {
            let mut data_buf = header.data_rio_buf(raw, raw.buffer_header().data_size());
            let mut local_addr_buf = header.local_addr_rio_buf(raw);
            let mut remote_addr_buf = header.remote_addr_rio_buf(raw);
            let local_addr_param = if header.local_addr(raw).is_some() {
                &mut local_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            let remote_addr_param = if header.remote_addr().is_some() {
                &mut remote_addr_buf as PRIO_BUF
            } else {
                null_mut()
            };
            header.op_socket = Some(self.clone());
            debug_assert!(data_buf.Length > 0);
            debug_assert!(data_buf.BufferId != RIO_INVALID_BUFFERID);
            header.active_op = RIOOpType::Send;
            // println!("start op {:?} {:?}", RIOOpType::Send, self.0.socket);
            rio_vtable().SendEx(
                self.0.rq,
                &mut data_buf as PRIO_BUF,
                1,
                local_addr_param,
                remote_addr_param,
                null_mut(),
                null_mut(),
                RIO_MSG_DEFER,
                packet.into_raw().into_raw().as_ptr() as PVOID,
            )?;

            state_guard.send_slots_open -= 1;
            Ok(())
        }
    }
    pub fn set_recv_buffer_size(&self, buf_size: usize) -> Result<()> {
        let mut buffer_size: i32 = buf_size as i32;
        let buffer_sizeof = size_of::<i32>() as i32;
        let retval = unsafe {
            winsock2::setsockopt(
                self.0.socket,
                ws2def::SOL_SOCKET,
                ws2def::SO_RCVBUF,
                &mut buffer_size as *mut _ as PCHAR,
                buffer_sizeof,
            )
        };
        if retval != 0 {
            Err(Error::WSAErr("setsockopt", retval))
        } else {
            Ok(())
        }
    }
    pub fn set_send_buffer_size(&self, buf_size: usize) -> Result<()> {
        let mut buffer_size: i32 = buf_size as i32;
        let buffer_sizeof = size_of::<i32>() as i32;
        let retval = unsafe {
            winsock2::setsockopt(
                self.0.socket,
                ws2def::SOL_SOCKET,
                ws2def::SO_SNDBUF,
                &mut buffer_size as *mut _ as PCHAR,
                buffer_sizeof,
            )
        };
        if retval != 0 {
            Err(Error::WSAErr("setsockopt", retval))
        } else {
            Ok(())
        }
    }
    pub fn bind(&self, addr: std::net::SocketAddr) -> Result<()> {
        unsafe { bind_socket(self, addr) }
    }
    pub fn total_recv_slots(&self) -> u32 {
        self.0.total_recv_slots
    }
    pub fn total_send_slots(&self) -> u32 {
        self.0.total_recv_slots
    }
}
unsafe fn create_rio_socket(
    _type: c_int,
    protocol: c_int,
    flags: DWORD,
    receive_cq: RIO_CQ,
    send_cq: RIO_CQ,
    max_outstanding_receive: ULONG,
    max_outstanding_send: ULONG,
) -> Result<(SOCKET, RIO_RQ)> {
    let socket = create_socket(
        _type,
        protocol,
        flags | winsock2::WSA_FLAG_REGISTERED_IO | winsock2::WSA_FLAG_OVERLAPPED,
    )?;
    match rio_vtable().CreateRequestQueue(
        socket,
        max_outstanding_receive,
        1,
        max_outstanding_send,
        1,
        receive_cq,
        send_cq,
        socket as *mut _,
    ) {
        Ok(rq) => Ok((socket, rq)),
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
    pub(crate) packet_buf: BufferRef<T>,
    pub(crate) result: Result<()>,
    pub(crate) op_type: RIOOpType,
}
struct RIOQueueState {
    results_buffer: Vec<RIORESULT>,
    socket_slots_open: u32,
    socket_slots_total: u32,
}
pub(crate) struct RIOQueueInner {
    // this OVERLAPPED struct is used when the RIO_CQ is bound with an IO Completion Port,
    // and a pointer to it will be passed in the IO completion packet by RIONotify
    iocp_overlapped: MaybeUninit<OVERLAPPED>,
    cq: RIO_CQ,
    state: Mutex<RIOQueueState>,
}
unsafe impl Send for RIOQueueInner {}
unsafe impl Sync for RIOQueueInner {}
#[derive(Clone)]
pub struct RIOQueue(Arc<RIOQueueInner>);
impl RIOQueue {
    pub unsafe fn from_overlapped(overlapped: &OVERLAPPED) -> Self {
        let overlapped_value = Arc::from_raw(*overlapped.u.Pointer() as *const RIOQueueInner);
        let retval = RIOQueue(Arc::clone(&overlapped_value));
        core::mem::forget(overlapped_value);
        retval
    }
    pub fn new(
        iocp_notify: Option<HANDLE>,
        completion_key: usize,
        queue_size: u32,
        results_buffer_size: usize,
    ) -> Result<Self> {
        Ok(RIOQueue(unsafe {
            create_rio_cq(iocp_notify, completion_key, queue_size, results_buffer_size)
        }?))
    }
    fn cq(&self) -> RIO_CQ {
        self.0.cq
    }
    pub fn poke(&self) -> Result<()> {
        unsafe { rio_vtable().Notify(self.0.cq) }
    }
    fn reserve_queue_slots(&self, num_slots: u32) -> Result<()> {
        let mut state = self.0.state.lock();
        let mut size = state.socket_slots_total;
        let mut new_open_slots = 0;
        while new_open_slots + state.socket_slots_open < num_slots {
            size = size * 2;
            new_open_slots = size - state.socket_slots_total;
        }
        if new_open_slots > 0 {
            unsafe { rio_vtable().ResizeCompletionQueue(self.cq(), size)? };
            state.socket_slots_total = size;
            state.socket_slots_open += new_open_slots;
        }
        state.socket_slots_open -= num_slots;
        Ok(())
    }
    fn release_queue_slots(&self, num_slots: u32) {
        let mut state = self.0.state.lock();
        state.socket_slots_open += num_slots;
    }
    pub fn dequeue_completions<T: UserHeader>(
        &self,
        results_buf: &mut Vec<RIOCompletion<T>>,
    ) -> Result<usize>
    where
        <T as UserHeader>::Mutable: AsMut<RIOPacketBuf>,
    {
        let mut state = self.0.state.lock();
        let mut results_buffer = &mut state.results_buffer;
        debug_assert!(results_buf.is_empty());
        debug_assert!(results_buf.len() <= results_buffer.capacity());
        unsafe {
            let num_completions = rio_vtable().DequeueCompletion(
                self.0.cq,
                results_buffer.as_mut_ptr(),
                results_buffer.capacity() as u32,
            )? as usize;
            results_buffer.set_len(num_completions);
            for i in 0..num_completions {
                let rio_result = results_buffer.get_unchecked(i);
                let mut packet_buf = BufferRef::<T>::from_raw(RawBufferRef::from_raw(
                    NonNull::new_unchecked(rio_result.RequestContext as *mut u8),
                ));
                let result = if rio_result.Status == 0 {
                    Ok(())
                } else {
                    Err(Error::WSAErr("RIORESULT", rio_result.Status))
                };
                let mut header_mut = packet_buf.mut_header::<RIOPacketBuf>();
                let op_type = header_mut.active_op;
                header_mut.active_op = RIOOpType::None;
                match op_type {
                    RIOOpType::Send => {
                        let socket = header_mut
                            .op_socket
                            .take()
                            .expect("socket was not set when completing a RIO send");
                        socket.complete_send();
                    }
                    RIOOpType::Receive => {
                        let socket = header_mut
                            .op_socket
                            .take()
                            .expect("socket was not set when completing a RIO receive");
                        socket.complete_receive();
                        if result.is_ok() {
                            packet_buf
                                .raw_mut()
                                .buffer_header_mut()
                                .set_data_size(rio_result.BytesTransferred);
                        }
                    }
                    _ => {}
                }
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
}
impl Drop for RIOQueueInner {
    fn drop(&mut self) {
        unsafe { rio_vtable().CloseCompletionQueue(self.cq) };
    }
}

unsafe fn create_rio_cq(
    iocp_notify: Option<HANDLE>,
    completion_key: usize,
    queue_size: u32,
    results_buffer_size: usize,
) -> Result<Arc<RIOQueueInner>> {
    let mut rio_queue_alloc = Arc::new(RIOQueueInner {
        iocp_overlapped: MaybeUninit::zeroed(),
        cq: RIO_INVALID_CQ,
        state: Mutex::new(RIOQueueState {
            results_buffer: Vec::with_capacity(results_buffer_size),
            socket_slots_open: queue_size,
            socket_slots_total: queue_size,
        }),
    });
    let mut iocp_registration = RIO_NOTIFICATION_COMPLETION::default();
    let notification = if let Some(iocp) = iocp_notify {
        iocp_registration.Type = RIO_IOCP_COMPLETION;
        iocp_registration.u.Iocp_mut().IocpHandle = iocp;
        iocp_registration.u.Iocp_mut().CompletionKey = completion_key as PVOID;
        iocp_registration.u.Iocp_mut().Overlapped =
            rio_queue_alloc.iocp_overlapped.as_ptr() as *mut _;
        &mut iocp_registration as *mut _
    } else {
        null_mut()
    };
    // this is sound since we have the only ref to the Arc
    *(&rio_queue_alloc.cq as *const RIO_CQ as *mut RIO_CQ) =
        rio_vtable().CreateCompletionQueue(queue_size as u32, notification)?;
    let mut overlapped = OVERLAPPED::default();
    *overlapped.u.Pointer_mut() = Arc::into_raw(Arc::clone(&rio_queue_alloc)) as PVOID;
    core::ptr::write(
        rio_queue_alloc.iocp_overlapped.as_ptr() as *mut OVERLAPPED,
        overlapped,
    );
    Ok(rio_queue_alloc)
}

unsafe fn bind_socket(socket: &RIOSocket, socket_addr: std::net::SocketAddr) -> Result<()> {
    let mut addr = ws2def::SOCKADDR_STORAGE::default();
    write_sockaddr(&mut addr, Some(socket_addr));

    if winsock2::bind(
        socket.0.socket,
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
                inet_addr.sin_port.swap_bytes(),
            )))
        }
        0 => None,
        _ => unimplemented!(),
    }
}

pub enum RIOOpCompletion<T> {
    Send(BufferRef<T>),
    Receive {
        buffer: BufferRef<T>,
        local_addr: Option<std::net::SocketAddr>,
        remote_addr: Option<std::net::SocketAddr>,
    },
}
