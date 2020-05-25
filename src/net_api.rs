use crate::buffer::{BufferHandle, BufferHeaderInit, BufferPoolHeader, BufferRef};
use crate::iocp::{IOCPHandler, IOCPQueueBuilder};
use crate::rio::{RIOCompletion, RIOQueue, RIOSocket};
use crate::rio_buf::{RIOPacketBuf, RIOPacketPool};
use crate::Result;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::future::Future;
use std::task::{Poll, Waker};

pub struct NetContext<T> {
    rio_queue: RIOQueue,
    marker: core::marker::PhantomData<T>,
}
impl<T> NetContext<T> {
    pub fn poke(&self) -> Result<()> {
        self.rio_queue.poke()
    }
}
impl<T> Clone for NetContext<T> {
    fn clone(&self) -> Self {
        Self {
            rio_queue: self.rio_queue.clone(),
            marker: core::marker::PhantomData,
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
            concurrent_receives: 16,
            concurrent_sends: 16,
            marker: core::marker::PhantomData,
        }
    }
    pub fn build(self, context: &NetContext<T>) -> Result<UdpSocket<T>> {
        UdpSocket::new(context, self.concurrent_sends, self.concurrent_receives)
    }
}
pub struct UdpSocket<T> {
    rio_socket: RIOSocket,
    marker: core::marker::PhantomData<T>,
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
            marker: core::marker::PhantomData,
        })
    }
    pub fn send(
        &self,
        packet_buf: BufferRef<T>,
    ) -> impl Future<Output = (BufferRef<T>, Result<()>)> {
        packet_buf.header::<WakerContext>().start_op();
        let buf_handle = packet_buf.make_handle();
        self.rio_socket.send_ex(packet_buf).unwrap(); // TODO push the result into the buffer ref, and immediately return
        self.rio_socket.commit_send_ex().unwrap();
        BufferFuture { buf_handle }
    }
    pub fn receive(
        &self,
        packet_buf: BufferRef<T>,
    ) -> impl Future<Output = (BufferRef<T>, Result<()>)> {
        packet_buf.header::<WakerContext>().start_op();
        let buf_handle = packet_buf.make_handle();
        self.rio_socket.receive_ex(packet_buf).unwrap(); // TODO push the result into the buffer ref, and immediately return
        self.rio_socket.commit_receive_ex().unwrap();
        BufferFuture { buf_handle }
    }
    pub fn bind(&self, socket_addr: std::net::SocketAddr) -> Result<()> {
        self.rio_socket.bind(socket_addr)
    }
}

pub const RIO_QUEUE_COMPLETION_KEY: usize = 0xBEEF;
pub struct NetContextBuilder<'a, T> {
    iocp_builder: &'a mut IOCPQueueBuilder,
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
        }
        Ok(())
    }
}

impl<'a, T: AsMut<RIOPacketBuf> + AsMut<WakerContext> + 'static> NetContextBuilder<'a, T> {
    pub fn new(iocp_builder: &'a mut IOCPQueueBuilder) -> Self {
        Self {
            iocp_builder,
            queue_size: 1024,
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
            marker: core::marker::PhantomData,
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

impl WakerContext {
    pub fn start_op(&self) {
        let mut inner = self.0.lock();
        inner.op_result = Poll::Pending;
        inner.has_op = true;
        let mut wakers = &mut inner.pending_wakers;
        let len = wakers.len();
        for waker in wakers.drain(0..len) {
            println!("dropping a waker");
        }
    }
    pub fn complete_op(&self, op_result: Result<()>) {
        let mut inner = self.0.lock();
        inner.op_result = Poll::Ready(op_result);
        inner.has_op = false;
        let mut wakers = &mut inner.pending_wakers;
        let len = wakers.len();
        for waker in wakers.drain(0..len) {
            waker.wake()
        }
    }
    pub fn poll(&self, waker: &Waker) -> Poll<Result<()>> {
        let mut inner = self.0.lock();
        if inner.op_result.is_ready() {
            inner.op_result.clone()
        } else {
            assert!(inner.has_op);
            let mut wakers = &mut inner.pending_wakers;
            wakers.push(waker.clone());
            Poll::Pending
        }
    }
}
struct BufferFuture<T> {
    buf_handle: BufferHandle<T>,
}
impl<T: AsRef<WakerContext>> Future for BufferFuture<T> {
    type Output = (BufferRef<T>, Result<()>);
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // it's safe to get a ref to the waker context through a non-exclusive pointer despite any existing exclusive access,
        // because it is internally protected by a mutex
        let waker_ctx =
            unsafe { AsRef::<WakerContext>::as_ref(&*self.buf_handle.user_header_ptr()) };
        match waker_ctx.poll(cx.waker()) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match self.buf_handle.clone().try_upgrade() {
                Ok(buf_ref) => Poll::Ready((buf_ref, result)),
                Err(_) => Poll::Pending,
            },
        }
    }
}
