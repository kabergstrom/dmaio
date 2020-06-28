use crate::alloc::alloc::Layout;
use crate::alloc::sync::Arc;
use crate::{Error, Result};
use core::future::Future;
use core::mem::size_of;
use core::ptr::{null_mut, NonNull};
use winapi::{
    ctypes::c_int,
    shared::{
        minwindef::{DWORD, ULONG},
        winerror::WAIT_TIMEOUT,
        ws2def::{self},
    },
    um::{
        errhandlingapi::GetLastError,
        handleapi::{CloseHandle, INVALID_HANDLE_VALUE},
        ioapiset,
        minwinbase::{LPOVERLAPPED, OVERLAPPED, OVERLAPPED_ENTRY},
        winbase::INFINITE,
        winnt::HANDLE,
        winsock2::{self},
    },
};

pub trait IOCPHandler {
    fn handle_completion(&self, entry: &OVERLAPPED_ENTRY) -> Result<()>;
}

#[derive(Default)]
pub struct IOCPHeaderPayload {
    pub ptr: Option<NonNull<u8>>,
    pub completion_key: usize,
}
#[derive(Default)]
pub struct IOCPHeader {
    overlapped: OVERLAPPED,
    payload: IOCPHeaderPayload,
}
unsafe impl Send for IOCPHeader {}
unsafe impl Sync for IOCPHeader {}
impl IOCPHeader {
    pub fn initialize(&mut self, ptr: NonNull<u8>, completion_key: usize) {
        self.overlapped = OVERLAPPED::default();
        self.payload.ptr = Some(ptr);
        self.payload.completion_key = completion_key;
    }
    pub fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        &self.overlapped as *const OVERLAPPED as *mut OVERLAPPED
    }
    pub unsafe fn get_payload_ptr(overlapped: *const OVERLAPPED) -> *const IOCPHeaderPayload {
        let header_ptr = overlapped as *const IOCPHeader;
        &(&*header_ptr).payload as *const IOCPHeaderPayload
    }
}
struct IOCPHandlerRegistration {
    completion_key: usize,
    obj: Box<dyn IOCPHandler + 'static>,
}
pub struct IOCPQueueBuilder {
    handlers: Vec<IOCPHandlerRegistration>,
    completion_buffer_size: usize,
    iocp_handle: IOCPQueueHandle,
}
impl IOCPQueueBuilder {
    pub fn new(thread_users: u32) -> Result<Self> {
        let mut this = Self {
            handlers: Vec::new(),
            completion_buffer_size: 1024,
            iocp_handle: IOCPQueueHandle(Arc::new(IOCPQueueHandleInner(unsafe {
                create_completion_port(thread_users)?
            }))),
        };
        this.register_handler(IOCP_FUTURE_SCHEDULE_KEY, IOCPFutureScheduler);
        Ok(this)
    }
    pub fn handle(&self) -> IOCPQueueHandle {
        self.iocp_handle.clone()
    }
    pub fn set_completion_buffer_size(&mut self, size: usize) -> &mut Self {
        self.completion_buffer_size = size;
        self
    }
    pub fn register_handler<T: IOCPHandler + 'static>(
        &mut self,
        completion_key: usize,
        handler: T,
    ) -> &mut Self {
        if self
            .handlers
            .iter()
            .any(|h| h.completion_key == completion_key)
        {
            // panic!("duplicate completion key registered");
            return self;
        }
        self.handlers.push(IOCPHandlerRegistration {
            completion_key,
            obj: Box::new(handler),
        });
        self
    }
    pub fn build(mut self) -> Result<IOCPQueue> {
        let call_table = self
            .handlers
            .iter()
            .map(|handler| handler.obj.as_ref() as *const dyn IOCPHandler)
            .collect();
        let inner = IOCPQueueInner {
            handle: self.iocp_handle,
            registrations: self.handlers,
            call_table,
        };
        Ok(IOCPQueue(
            Arc::new(inner),
            Vec::with_capacity(self.completion_buffer_size),
        ))
    }
}
struct IOCPQueueHandleInner(HANDLE);
unsafe impl Send for IOCPQueueHandleInner {}
unsafe impl Sync for IOCPQueueHandleInner {}
#[derive(Clone)]
pub struct IOCPQueueHandle(Arc<IOCPQueueHandleInner>);
impl Drop for IOCPQueueHandleInner {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}
struct IOCPQueueInner {
    handle: IOCPQueueHandle,
    registrations: Vec<IOCPHandlerRegistration>,
    call_table: Vec<*const dyn IOCPHandler>,
}

struct IOCPFutureScheduler;
impl IOCPHandler for IOCPFutureScheduler {
    fn handle_completion(&self, entry: &OVERLAPPED_ENTRY) -> Result<()> {
        let task = unsafe { async_task::Task::<()>::from_raw(entry.lpOverlapped as *const ()) };
        task.run();
        Ok(())
    }
}

pub const IOCP_FUTURE_SCHEDULE_KEY: usize = 0xDEED;
pub type JoinHandle<T> = async_task::JoinHandle<T, ()>;
impl IOCPQueueHandle {
    pub fn spawn<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
        &self,
        future: F,
    ) -> JoinHandle<R> {
        let handle = unsafe { self.raw() } as usize;
        let (task, join_handle) = async_task::spawn(
            future,
            move |task| unsafe {
                assert!(
                    ioapiset::PostQueuedCompletionStatus(
                        handle as HANDLE,
                        0,
                        IOCP_FUTURE_SCHEDULE_KEY,
                        task.into_raw() as LPOVERLAPPED,
                    ) != 0
                );
            },
            (),
        );
        task.schedule();
        join_handle
    }
    pub unsafe fn raw(&self) -> HANDLE {
        (self.0).0
    }
    pub unsafe fn associate_handle(&self, handle: HANDLE, completion_key: usize) -> Result<()> {
        let ret_val = ioapiset::CreateIoCompletionPort(handle, self.raw(), completion_key, 0);
        if ret_val == null_mut() {
            Err(Error::AssociateCompletionPortFailed)
        } else {
            Ok(())
        }
    }
}
pub struct IOCPQueue(Arc<IOCPQueueInner>, Vec<OVERLAPPED_ENTRY>);
unsafe impl Send for IOCPQueue {}
impl Clone for IOCPQueue {
    fn clone(&self) -> Self {
        Self(self.0.clone(), Vec::with_capacity(self.1.capacity()))
    }
}

impl IOCPQueue {
    pub fn handle(&self) -> IOCPQueueHandle {
        self.0.handle.clone()
    }
    #[inline]
    pub fn poll(&mut self, timeout_ms: Option<u32>) -> Result<bool> {
        unsafe {
            let completion_entries = &mut self.1;
            let mut num_received_entries = 0;
            let ret_val = ioapiset::GetQueuedCompletionStatusEx(
                self.0.handle.raw(),
                completion_entries.as_mut_ptr(),
                completion_entries.capacity() as ULONG,
                &mut num_received_entries,
                timeout_ms.unwrap_or(INFINITE),
                0,
            );
            if ret_val != 0 {
                completion_entries.set_len(num_received_entries as usize);
                // println!("entries {}", num_received_entries);
                for i in 0..num_received_entries as usize {
                    let entry = completion_entries.get_unchecked(i);
                    let key = if entry.lpCompletionKey == 0 {
                        let payload = IOCPHeader::get_payload_ptr(entry.lpOverlapped);
                        (&*payload).completion_key
                    } else {
                        entry.lpCompletionKey
                    };
                    for handler in &self.0.registrations {
                        if key == handler.completion_key {
                            handler.obj.handle_completion(entry)?;
                        }
                    }
                }
                Ok(false)
            } else if GetLastError() == WAIT_TIMEOUT {
                Ok(true)
            } else {
                Err(Error::WinError(
                    "GetQueuedCompletionStatusEx",
                    GetLastError(),
                ))?
            }
        }
    }
}

unsafe fn create_completion_port(max_thread_users: u32) -> Result<HANDLE> {
    let handle =
        ioapiset::CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_mut(), 0, max_thread_users);
    if handle != INVALID_HANDLE_VALUE {
        Ok(handle)
    } else {
        Err(Error::CreateCompletionPortFailed)
    }
}
