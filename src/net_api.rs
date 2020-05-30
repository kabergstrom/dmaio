use crate::buffer::{BufferHandle, BufferHeaderInit, BufferPool, BufferPoolHeader, BufferRef};
use crate::iocp::{IOCPHandler, IOCPQueueBuilder};
use crate::rio::{RIOCompletion, RIOQueue, RIOSocket};
use crate::rio_buf::{RIOPacketBuf, RIOPacketPool};
use crate::{Error, Result};
use futures_core::Stream;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::future::Future;
use std::task::{Poll, Waker};

pub struct NetContext<T> {
    rio_queue: RIOQueue,
    buffer_pool: BufferPool<T>,
}
impl<T> NetContext<T> {
    pub fn poke(&self) -> Result<()> {
        self.rio_queue.poke()
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
            rio_queue: self.rio_queue.clone(),
            buffer_pool: self.buffer_pool.clone(),
        }
    }
}

pub struct IOPacketHeader {
    rio: RIOPacketBuf,
    waker: WakerContext,
}
impl AsMut<WakerContext> for IOPacketHeader {
    fn as_mut(&mut self) -> &mut WakerContext {
        &mut self.waker
    }
}
impl AsRef<WakerContext> for IOPacketHeader {
    fn as_ref(&self) -> &WakerContext {
        &self.waker
    }
}
impl AsMut<RIOPacketBuf> for IOPacketHeader {
    fn as_mut(&mut self) -> &mut RIOPacketBuf {
        &mut self.rio
    }
}
impl AsRef<RIOPacketBuf> for IOPacketHeader {
    fn as_ref(&self) -> &RIOPacketBuf {
        &self.rio
    }
}

#[derive(Default)]
pub struct IOPacketPool {
    rio_init: RIOPacketPool,
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
            rio: self.rio_init.initialize_header(bufpool, buffer),
            waker: self.waker_init.initialize_header(bufpool, buffer),
        }
    }
}

pub struct UdpSocketBuilder<T> {
    concurrent_receives: u32,
    concurrent_sends: u32,
    marker: core::marker::PhantomData<T>,
}
impl<T: AsMut<RIOPacketBuf> + AsRef<WakerContext>> UdpSocketBuilder<T> {
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
    pub fn build(self, context: &NetContext<T>) -> Result<UdpSocket<T>> {
        UdpSocket::new(context, self.concurrent_sends, self.concurrent_receives)
    }
}
pub struct UdpSocket<T> {
    rio_socket: RIOSocket,
    net_context: NetContext<T>,
}
impl<T: AsMut<RIOPacketBuf> + AsRef<WakerContext>> UdpSocket<T> {
    fn new(
        context: &NetContext<T>,
        concurrent_sends: u32,
        concurrent_receives: u32,
    ) -> Result<Self> {
        Ok(Self {
            rio_socket: RIOSocket::new_udp(
                &context.rio_queue,
                concurrent_sends,
                &context.rio_queue,
                concurrent_receives,
            )?,
            net_context: context.clone(),
        })
    }
    pub fn send(&self, packet_buf: BufferRef<T>) -> Result<impl Future<Output = Result<()>>> {
        packet_buf.header::<WakerContext>().start_op(None);
        let buf_handle = packet_buf.make_handle();
        self.rio_socket.send_ex(packet_buf, None)?; // TODO wait on slot exhaustion instead of propagating error
        self.rio_socket.commit_send_ex()?;
        Ok(BufferFuture { buf_handle })
    }
    pub fn receive(&self) -> impl AsyncBufferRead<T> {
        ReceiveFuture {
            net_context: self.net_context.clone(),
            pending_receives: Vec::new(),
            socket: self.rio_socket.clone(),
        }
    }
    // pub fn receive(
    //     &self,
    //     packet_buf: BufferRef<T>,
    // ) -> impl Future<Output = (BufferRef<T>, Result<()>)> {
    //     packet_buf.header::<WakerContext>().start_op();
    //     let buf_handle = packet_buf.make_handle();
    //     // TODO make the whole receive/send flow a future to handle exhaustion of send/receive slots
    //     self.rio_socket.receive_ex(packet_buf, None).unwrap(); // TODO push the result into the buffer ref, and immediately return
    //     self.rio_socket.commit_receive_ex().unwrap();
    //     BufferFuture { buf_handle }
    // }
    pub fn bind(&self, socket_addr: std::net::SocketAddr) -> Result<()> {
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
impl<T: AsMut<RIOPacketBuf> + AsMut<WakerContext>> IOCPHandler for RIOQueueHandler<T> {
    fn handle_completion(&self, entry: &winapi::um::minwinbase::OVERLAPPED_ENTRY) -> Result<()> {
        let mut inner = self.0.lock();
        let mut iter = 0;

        loop {
            unsafe {
                let results_buf = &mut inner.results_buf;
                let rio_queue = RIOQueue::from_overlapped(&*entry.lpOverlapped);
                let num_completions = rio_queue.dequeue_completions(results_buf)?;
                for mut completion in results_buf.drain(0..num_completions) {
                    let mut waker_header = completion.packet_buf.header_mut::<WakerContext>();
                    waker_header.complete_op(completion.result.clone());
                    // println!("finish op {:?}", completion.op_type);
                    drop(completion);
                }
                // println!("{}", num_completions);
                iter += 1;
                if iter >= 20 || num_completions == 0 {
                    if num_completions == 0 {
                        rio_queue.poke();
                    }
                    break;
                }
            }
        }
        // if num_completions > 0 {
        //     rio_queue.poke()?;
        // }
        Ok(())
    }
}

impl<'a, T: AsMut<RIOPacketBuf> + AsMut<WakerContext> + 'static> NetContextBuilder<'a, T> {
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
            Some(self.iocp_builder.iocp_handle()),
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
        Ok(NetContext {
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
        WakerContext(Mutex::new(WakerContextInner {
            pending_wakers: SmallVec::new(),
            op_result: Poll::Pending,
            has_op: false,
        }))
    }
}
struct WakerContextInner {
    pending_wakers: SmallVec<[Waker; 1]>,
    op_result: Poll<Result<()>>,
    has_op: bool,
}
pub struct WakerContext(Mutex<WakerContextInner>);

use std::sync::atomic::Ordering;
static LOL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
impl WakerContext {
    pub fn start_op(&self, waker: Option<&Waker>) {
        let mut inner = self.0.lock();
        inner.op_result = Poll::Pending;
        inner.has_op = true;
        let mut wakers = &mut inner.pending_wakers;
        let len = wakers.len();
        for waker in wakers.drain(0..len) {
            println!("dropping a waker");
        }
        if let Some(waker) = waker {
            wakers.push(waker.clone());
            // println!(
            //     "registering {:?} {}",
            //     waker,
            //     LOL.fetch_add(1, Ordering::Relaxed)
            // );
        }
    }
    pub fn complete_op(&self, op_result: Result<()>) {
        let mut inner = self.0.lock();
        inner.op_result = Poll::Ready(op_result);
        inner.has_op = false;
        let mut wakers = &mut inner.pending_wakers;
        let len = wakers.len();
        for waker in wakers.drain(0..len) {
            // println!("waking {:?} {}", waker, LOL.fetch_sub(1, Ordering::Relaxed));
            waker.wake()
        }
        // assert!(len > 0);
    }
    pub fn poll(&self, waker: &Waker) -> Poll<Result<()>> {
        let mut inner = self.0.lock();
        if inner.op_result.is_ready() {
            inner.op_result.clone()
        } else {
            assert!(inner.has_op);
            let mut wakers = &mut inner.pending_wakers;
            wakers.push(waker.clone());
            // println!(
            //     "registering {:?} {}",
            //     waker,
            //     LOL.fetch_add(1, Ordering::Relaxed)
            // );
            Poll::Pending
        }
    }
}
struct BufferFuture<T> {
    buf_handle: BufferHandle<T>,
}
impl<T: AsRef<WakerContext>> Future for BufferFuture<T> {
    type Output = Result<()>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // it's safe to get a ref to the waker context through a non-exclusive pointer despite any existing exclusive access,
        // because it is internally protected by a mutex
        let waker_ctx =
            unsafe { AsRef::<WakerContext>::as_ref(&*self.buf_handle.user_header_ptr()) };
        match waker_ctx.poll(cx.waker()) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match self.buf_handle.clone().try_upgrade(Some(cx.waker())) {
                Ok(buf_ref) => Poll::Ready(result),
                Err(_) => Poll::Pending,
            },
        }
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

impl<T: AsRef<WakerContext> + AsMut<RIOPacketBuf>> AsyncBufferRead<T> for ReceiveFuture<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
        buffers: &mut Vec<BufferRef<T>>,
    ) -> Poll<Result<usize>> {
        let mut buffers_received = 0;
        for (idx, buffer) in self.pending_receives.iter().enumerate() {
            let waker_ctx = unsafe { AsRef::<WakerContext>::as_ref(&*buffer.user_header_ptr()) };
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
                    .header::<WakerContext>()
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
