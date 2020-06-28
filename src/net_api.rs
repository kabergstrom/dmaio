use crate::buffer::{
    BufferHandle, BufferHeaderInit, BufferPool, BufferPoolHeader, BufferRef, UserHeader,
};
use crate::iocp::{IOCPHandler, IOCPHeader, IOCPQueueBuilder, IOCPQueueHandle};
use crate::rio::{RIOCompletion, RIOQueue, RIOSocket};
use crate::rio_buf::{NetHeader, NetHeaderInit, NetOp};
use crate::{Error, Result};
use core::ptr::null_mut;
use core::sync::atomic::{AtomicPtr, Ordering};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::future::Future;
use std::net::SocketAddr;
use std::task::{Poll, Waker};

pub struct NetContext<T> {
    iocp_handle: IOCPQueueHandle,
    rio_queue: RIOQueue,
    buffer_pool: BufferPool<T>,
}
impl<T> NetContext<T> {
    pub fn poke(&self) -> Result<()> {
        self.rio_queue.poke()
    }
    pub fn iocp_handle(&self) -> &IOCPQueueHandle {
        &self.iocp_handle
    }
    pub fn alloc_buffer(&self) -> impl Future<Output = BufferRef<T>> {
        self.buffer_pool.alloc()
    }
    pub fn try_alloc(&self, waker: Option<&Waker>) -> Option<BufferRef<T>> {
        self.buffer_pool.try_alloc(waker)
    }
}
impl<T> Clone for NetContext<T> {
    fn clone(&self) -> Self {
        Self {
            iocp_handle: self.iocp_handle.clone(),
            rio_queue: self.rio_queue.clone(),
            buffer_pool: self.buffer_pool.clone(),
        }
    }
}

pub struct IOMutHeader {
    net: NetHeader,
    iocp: IOCPHeader,
}
impl AsMut<NetHeader> for IOMutHeader {
    fn as_mut(&mut self) -> &mut NetHeader {
        &mut self.net
    }
}
impl AsMut<IOCPHeader> for IOMutHeader {
    fn as_mut(&mut self) -> &mut IOCPHeader {
        &mut self.iocp
    }
}
pub struct IOPacketHeader {
    mut_header: IOMutHeader,
    waker: WakerContext,
}
unsafe impl UserHeader for IOPacketHeader {
    type Mutable = IOMutHeader;
    type Shared = WakerContext;
    fn mut_segment(&mut self) -> &mut Self::Mutable {
        &mut self.mut_header
    }
    fn shared_segment(&self) -> &Self::Shared {
        &self.waker
    }
}

#[derive(Default)]
pub struct IOPacketPool {
    rio_init: NetHeaderInit,
    waker_init: WakerContextInit,
}
impl BufferHeaderInit for IOPacketPool {
    type Header = IOPacketHeader;
    fn initialize_self(&mut self, bufpool: &BufferPoolHeader) -> Result<()> {
        self.rio_init.initialize_self(bufpool)?;
        self.waker_init.initialize_self(bufpool)?;
        Ok(())
    }
    fn initialize_header(
        &self,
        bufpool: &BufferPoolHeader,
        buffer: &crate::buffer::RawBufferRef,
    ) -> Self::Header {
        Self::Header {
            mut_header: IOMutHeader {
                net: self.rio_init.initialize_header(bufpool, buffer),
                iocp: IOCPHeader::default(),
            },
            waker: self.waker_init.initialize_header(bufpool, buffer),
        }
    }
}

pub struct TcpSocketBuilder<T> {
    concurrent_receives: u32,
    concurrent_sends: u32,
    marker: core::marker::PhantomData<T>,
}
impl<T: UserHeader> TcpSocketBuilder<T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader> + AsMut<IOCPHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    pub fn new() -> Self {
        Self {
            concurrent_receives: 32,
            concurrent_sends: 32,
            marker: core::marker::PhantomData,
        }
    }
    pub fn concurrent_sends(mut self, value: u32) -> Self {
        self.concurrent_sends = value;
        self
    }
    pub fn concurrent_receives(mut self, value: u32) -> Self {
        self.concurrent_receives = value;
        self
    }
    pub fn build(self, context: &NetContext<T>) -> Result<TcpSocket<T>> {
        TcpSocket::new(context, self.concurrent_sends, self.concurrent_receives)
    }
}
pub struct TcpSocket<T> {
    rio_socket: RIOSocket,
    net_context: NetContext<T>,
}
impl<T: UserHeader> TcpSocket<T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader> + AsMut<IOCPHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    pub fn bind(&self, socket_addr: std::net::SocketAddr) -> Result<()> {
        self.rio_socket.bind(socket_addr)?;
        Ok(())
    }
    pub fn listen(&self, backlog: i32) -> Result<()> {
        self.rio_socket.listen(backlog)?;
        Ok(())
    }
    #[doc(hidden)]
    fn new(
        context: &NetContext<T>,
        concurrent_sends: u32,
        concurrent_receives: u32,
    ) -> Result<Self> {
        Ok(Self {
            rio_socket: RIOSocket::new_tcp(
                &context.rio_queue,
                concurrent_sends,
                concurrent_receives,
            )?,
            net_context: context.clone(),
        })
    }
    pub async fn send(&self, packet_buf: BufferRef<T>) -> Result<impl Future<Output = Result<()>>> {
        let buf_handle = packet_buf.make_handle();
        drop(packet_buf);
        for handle in buf_handle.chain_begin().chain_iter_forward() {
            let buf_ref = handle.upgrade().await;
            buf_ref.shared_header().start().await;
            self.rio_socket.send_ex(buf_ref, None)?; // TODO wait on slot exhaustion instead of propagating error
        }
        self.rio_socket.commit_send_ex()?;
        Ok(wait_for_op_complete(buf_handle))
    }
    pub fn receive(&self) -> impl AsyncBufferRead<T> {
        ReceiveFuture {
            net_context: self.net_context.clone(),
            pending_receives: Vec::new(),
            socket: self.rio_socket.clone(),
        }
    }

    pub async fn connect(&self, mut connect_op: BufferRef<T>, addr: SocketAddr) -> Result<()> {
        let net_header = connect_op.mut_header::<NetHeader>();
        net_header.set_remote_addr(Some(addr));
        let op_handle = connect_op.make_handle();
        op_handle.shared_header().start().await;
        self.rio_socket.connect_ex(connect_op)?;
        op_handle.shared_header().wait().await?;
        let mut op_ref = op_handle.upgrade().await;
        let net_header = op_ref.mut_header::<NetHeader>();
        match net_header.consume_op() {
            NetOp::ConnectEx(_) => Ok(()),
            _ => panic!("unexpected net op type in accept"),
        }
    }

    pub async fn accept(
        &self,
        accept_op: BufferRef<T>,
        concurrent_sends: u32,
        concurrent_receives: u32,
    ) -> Result<TcpSocket<T>> {
        let op_handle = accept_op.make_handle();
        op_handle.shared_header().start().await;
        self.rio_socket.accept_ex(accept_op)?;
        op_handle.shared_header().wait().await?;
        let mut op_ref = op_handle.upgrade().await;
        let net_header = op_ref.mut_header::<NetHeader>();
        match net_header.consume_op() {
            NetOp::AcceptEx(raw_socket, listener_socket) => {
                let raw_socket = raw_socket.take();
                let new_socket = RIOSocket::from_socket(
                    raw_socket,
                    listener_socket.ty(),
                    listener_socket.protocol(),
                    &self.net_context.rio_queue,
                    concurrent_sends,
                    concurrent_receives,
                )?;
                let new_socket = TcpSocket {
                    net_context: self.net_context.clone(),
                    rio_socket: new_socket,
                };
                Ok(new_socket)
            }
            _ => panic!("unexpected net op type in accept"),
        }
    }
}

pub struct UdpSocketBuilder<T> {
    socket_addr: std::net::SocketAddr,
    concurrent_receives: u32,
    concurrent_sends: u32,
    marker: core::marker::PhantomData<T>,
}
impl<T: UserHeader> UdpSocketBuilder<T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    pub fn new(socket_addr: std::net::SocketAddr) -> Self {
        Self {
            socket_addr,
            concurrent_receives: 32,
            concurrent_sends: 32,
            marker: core::marker::PhantomData,
        }
    }
    pub fn concurrent_sends(mut self, value: u32) -> Self {
        self.concurrent_sends = value;
        self
    }
    pub fn concurrent_receives(mut self, value: u32) -> Self {
        self.concurrent_receives = value;
        self
    }
    pub fn build(self, context: &NetContext<T>) -> Result<UdpSocket<T>> {
        UdpSocket::bind(
            context,
            self.socket_addr,
            self.concurrent_sends,
            self.concurrent_receives,
        )
    }
}

pub struct UdpSocket<T> {
    rio_socket: RIOSocket,
    net_context: NetContext<T>,
}
impl<T> Clone for UdpSocket<T> {
    fn clone(&self) -> Self {
        Self {
            rio_socket: self.rio_socket.clone(),
            net_context: self.net_context.clone(),
        }
    }
}
impl<T: UserHeader> UdpSocket<T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    pub fn bind(
        context: &NetContext<T>,
        socket_addr: std::net::SocketAddr,
        concurrent_sends: u32,
        concurrent_receives: u32,
    ) -> Result<Self> {
        let socket = Self::new(context, concurrent_sends, concurrent_receives)?;
        socket.bind_socket(socket_addr)?;
        Ok(socket)
    }
    #[doc(hidden)]
    fn new(
        context: &NetContext<T>,
        concurrent_sends: u32,
        concurrent_receives: u32,
    ) -> Result<Self> {
        Ok(Self {
            rio_socket: RIOSocket::new_udp(
                &context.rio_queue,
                concurrent_sends,
                concurrent_receives,
            )?,
            net_context: context.clone(),
        })
    }
    pub async fn send(&self, packet_buf: BufferRef<T>) -> Result<impl Future<Output = Result<()>>> {
        let buf_handle = packet_buf.make_handle();
        drop(packet_buf);
        for handle in buf_handle.chain_begin().chain_iter_forward() {
            let buf_ref = handle.upgrade().await;
            buf_ref.shared_header().start().await;
            self.rio_socket.send_ex(buf_ref, None)?; // TODO wait on slot exhaustion instead of propagating error
        }
        self.rio_socket.commit_send_ex()?;
        Ok(wait_for_op_complete(buf_handle))
    }
    pub fn receive(&self) -> impl AsyncBufferRead<T> {
        ReceiveFuture {
            net_context: self.net_context.clone(),
            pending_receives: Vec::new(),
            socket: self.rio_socket.clone(),
        }
    }
    #[doc(hidden)]
    fn bind_socket(&self, socket_addr: std::net::SocketAddr) -> Result<()> {
        self.rio_socket.bind(socket_addr)
    }
}

pub const RIO_QUEUE_COMPLETION_KEY: usize = 0xBEEF;
pub struct NetContextBuilder<'a, T> {
    iocp_builder: &'a mut IOCPQueueBuilder,
    buffer_pool: BufferPool<T>,
    queue_size: u32,
    results_buffer_size: usize,
    marker: core::marker::PhantomData<T>,
}

struct RIOQueueHandlerInner<T> {
    results_buf: Vec<RIOCompletion<T>>,
}
struct RIOQueueHandler<T>(Mutex<RIOQueueHandlerInner<T>>);
impl<T: UserHeader> IOCPHandler for RIOQueueHandler<T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    fn handle_completion(&self, entry: &winapi::um::minwinbase::OVERLAPPED_ENTRY) -> Result<()> {
        let mut inner = self.0.lock();
        let mut iter = 0;

        let results_buf = &mut inner.results_buf;

        let rio_queue = unsafe {
            let iocp_payload = &*IOCPHeader::get_payload_ptr(entry.lpOverlapped);
            RIOQueue::from_overlapped_ptr(
                iocp_payload.ptr.expect("rio queue pointer null").as_ptr(),
            )
        };
        loop {
            let num_completions = rio_queue.dequeue_completions(results_buf)?;
            for mut completion in results_buf.drain(0..num_completions) {
                let buffer = completion.packet_buf.make_handle();
                let result = completion.result.clone();
                // println!(
                //     "completed {:?} for {:?}",
                //     core::mem::discriminant(&completion.packet_buf.mut_header().active_op),
                //     completion.packet_buf.raw().data_ptr()
                // );
                drop(completion);
                let mut waker_header = buffer.shared_header::<WakerContext>();
                waker_header.complete_op(result);
            }
            // println!("{}", num_completions);
            iter += 1;
            if iter >= 20 || num_completions == 0 {
                rio_queue.poke(); // this queues another IOCP packet for the rio queue to ensure we get all completions
                                  // doesn't really make sense to me, but CPU usage doesn't seem to be high despite this
                break;
            }
        }
        Ok(())
    }
}

impl<'a, T: UserHeader + 'static> NetContextBuilder<'a, T>
where
    <T as UserHeader>::Mutable: AsMut<NetHeader>,
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    pub fn new(iocp_builder: &'a mut IOCPQueueBuilder, buffer_pool: BufferPool<T>) -> Self {
        Self {
            iocp_builder,
            queue_size: 1024,
            buffer_pool,
            results_buffer_size: 1024,
            marker: core::marker::PhantomData,
        }
    }
    // builds and registers the context with the
    pub fn finish(self) -> Result<NetContext<T>> {
        let queue = RIOQueue::new(
            Some(self.iocp_builder.handle()),
            RIO_QUEUE_COMPLETION_KEY,
            self.queue_size,
            self.results_buffer_size,
        )?;
        self.iocp_builder.register_handler(
            RIO_QUEUE_COMPLETION_KEY,
            RIOQueueHandler::<T>(Mutex::new(RIOQueueHandlerInner {
                results_buf: Vec::with_capacity(self.results_buffer_size),
            })),
        );
        self.iocp_builder.register_handler(
            crate::rio::IOCP_SOCKET_KEY,
            crate::rio::NetIOCPHandler::<T>(core::marker::PhantomData),
        );
        Ok(NetContext {
            iocp_handle: self.iocp_builder.handle(),
            rio_queue: queue,
            buffer_pool: self.buffer_pool,
        })
    }
}

#[derive(Default)]
struct WakerContextInit;
impl BufferHeaderInit for WakerContextInit {
    type Header = WakerContext;
    fn initialize_self(&mut self, bufpool: &BufferPoolHeader) -> Result<()> {
        Ok(())
    }
    fn initialize_header(
        &self,
        bufpool: &BufferPoolHeader,
        buffer: &crate::buffer::RawBufferRef,
    ) -> Self::Header {
        WakerContext {
            pending_waker: futures_core::task::__internal::AtomicWaker::new(),
            op_result: AtomicPtr::new(null_mut()),
        }
    }
}
pub struct WakerContext {
    pending_waker: futures_core::task::__internal::AtomicWaker,
    op_result: AtomicPtr<Error>, // when the op is uninitialized, this ptr will be 0x0
                                 // when the op is pending, this ptr will be 0x1
                                 // when the op is ready with no error, this ptr will be 0x2
                                 // when the op is ready with error, this ptr will be a Box<Error>
}
impl AsRef<WakerContext> for WakerContext {
    fn as_ref(&self) -> &WakerContext {
        self
    }
}

impl WakerContext {
    pub fn start<'a>(&'a self) -> impl Future<Output = ()> + 'a {
        WakerStart(self)
    }
    pub fn start_op(&self, waker: Option<&Waker>) {
        if let Some(waker) = self.pending_waker.take() {
            panic!("active waker when starting op");
        }
        self.op_result.store(0x1 as *mut Error, Ordering::Release);
        if let Some(waker) = waker {
            self.pending_waker.register(waker);
        }
    }
    pub fn complete_op(&self, op_result: Result<()>) {
        let ptr_value = match op_result {
            Ok(()) => 0x2 as *mut Error,
            Err(err) => Box::into_raw(Box::new(err)),
        };
        self.op_result.store(ptr_value, Ordering::Release);
        let waker = self.pending_waker.take();
        waker.unwrap().wake();
    }

    pub fn poll(&self, waker: &Waker) -> Poll<Result<()>> {
        let result_ptr = self.op_result.load(Ordering::Acquire);
        let result = if result_ptr == 0x0 as *mut Error {
            Poll::Ready(Err(Error::OpUninitialized))
        } else if result_ptr == 0x1 as *mut Error {
            self.pending_waker.register(waker);
            return Poll::Pending;
        } else if result_ptr == 0x2 as *mut Error {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(unsafe { *Box::from_raw(result_ptr) }))
        };
        self.pending_waker.take();
        self.op_result.store(0x0 as *mut Error, Ordering::Release);
        result
    }
    pub fn wait<'a>(&'a self) -> impl Future<Output = Result<()>> + 'a {
        WakerWait(self)
    }
}
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct WakerWait<'a>(&'a WakerContext);
impl<'a> Future for WakerWait<'a> {
    type Output = Result<()>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let waker_ctx = self.0;
        waker_ctx.poll(cx.waker())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct WakerStart<'a>(&'a WakerContext);
impl<'a> Future for WakerStart<'a> {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let waker_ctx = self.0;
        waker_ctx.start_op(Some(cx.waker()));
        Poll::Ready(())
    }
}

pub trait AsyncBufferRead<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
        buffers: &mut Vec<BufferRef<T>>,
    ) -> Poll<Result<usize>>;
}

struct ReceiveFuture<T> {
    pending_receives: Vec<BufferHandle<T>>,
    socket: RIOSocket,
    net_context: NetContext<T>,
}
impl<T> Unpin for ReceiveFuture<T> {}

impl<T: UserHeader> AsyncBufferRead<T> for ReceiveFuture<T>
where
    <T as UserHeader>::Shared: AsRef<WakerContext>,
    <T as UserHeader>::Mutable: AsMut<NetHeader>,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
        buffers: &mut Vec<BufferRef<T>>,
    ) -> Poll<Result<usize>> {
        let mut buffers_received = 0;
        for (idx, buffer) in self.pending_receives.iter().enumerate() {
            let waker_ctx = buffer.shared_header();
            match waker_ctx.poll(cx.waker()) {
                Poll::Pending => {}
                Poll::Ready(result) => {
                    if let Err(err) = result {
                        return Poll::Ready(Err(err));
                    }
                    match buffer.clone().try_upgrade(Some(cx.waker())) {
                        Ok(buf_ref) => {
                            buffers.push(buf_ref);
                            buffers_received += 1;
                        }
                        Err(_) => {}
                    }
                }
            }
            break;
        }

        self.pending_receives.drain(0..buffers_received);
        let mut has_started_receive = false;
        while self.pending_receives.len() < self.socket.total_recv_slots() as usize {
            if let Some(recv_buffer) = self.net_context.try_alloc(Some(cx.waker())) {
                recv_buffer
                    .shared_header::<WakerContext>()
                    .start_op(Some(cx.waker()));
                let buffer_handle = recv_buffer.make_handle();
                let (result, _) = self.socket.receive_ex(recv_buffer, Some(cx.waker()));
                match result {
                    Err(Error::SlotsExhausted) => break,
                    Err(err) => return Poll::Ready(Err(err)),
                    Ok(()) => {
                        has_started_receive = true;
                        self.as_mut().pending_receives.push(buffer_handle);
                    }
                }
            } else {
                break;
            }
        }
        if has_started_receive {
            self.socket.commit_receive_ex()?;
        }

        if buffers_received > 0 {
            Poll::Ready(Ok(buffers_received))
        } else {
            Poll::Pending
        }
    }
}

impl<T, V: AsyncBufferRead<T>> AsyncBufferReadExt<T> for V {}
pub trait AsyncBufferReadExt<T>: AsyncBufferRead<T> {
    fn read<'a>(&'a mut self, buffers: &'a mut Vec<BufferRef<T>>) -> Read<'a, Self, T> {
        Read {
            reader: self,
            buf: buffers,
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Read<'a, R: ?Sized, T> {
    reader: &'a mut R,
    buf: &'a mut Vec<BufferRef<T>>,
}

impl<R: AsyncBufferRead<T>, T> Future for Read<'_, R, T>
where
    R: Unpin + ?Sized,
{
    type Output = Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<usize>> {
        let me = &mut *self;
        std::pin::Pin::new(&mut *me.reader).poll_read(cx, me.buf)
    }
}

struct AcceptFuture<T> {
    buffer_handle: BufferHandle<T>,
    socket: RIOSocket,
    net_context: NetContext<T>,
}
impl<T> Unpin for AcceptFuture<T> {}

// impl<T: UserHeader> Future for AcceptFuture<T>
// where
//     <T as UserHeader>::Shared: AsRef<WakerContext>,
// {
//     type Output = Result<TcpSocket<T>>;
//     fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
//         let waker_header = self.buffer_handle.shared_header();
//         // match waker_header.poll(cx.waker()) {
//         //     Poll::Pending => return Poll::Pending,
//         //     Poll::Ready(result) =>  {

//         //     match result {
//         //         Ok(_) => {

//         //         }
//         //         Err(err)=> self.buffer_handle.
//         //     }
//         // }
//         // }
//     }
// }

async fn wait_for_op_complete<T: UserHeader>(buffer_handle: BufferHandle<T>) -> Result<()>
where
    <T as UserHeader>::Shared: AsRef<WakerContext>,
{
    for handle in buffer_handle.chain_begin().chain_iter_forward() {
        handle.shared_header().wait().await?;
    }
    Ok(())
}
