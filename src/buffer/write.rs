use super::buffer::{buffer_data_ptr, buffer_header_ptr, buffer_header_ptr_mut};
use super::chain_iter::{chain_begin_ptr, ChainIterForwardPtr};
use super::{BufferRef, RawBufferRef};
use core::ptr::NonNull;
use core::sync::atomic::Ordering;

pub(super) struct BufferWriter<'a> {
    buf_ref: &'a mut RawBufferRef,
    chain_iter: Option<NonNull<u8>>,
}
impl<'a> BufferWriter<'a> {
    pub fn new(buffer: &'a mut RawBufferRef) -> Self {
        unsafe {
            let mut chain_iter = None;
            for buffer in ChainIterForwardPtr::new(chain_begin_ptr(buffer.ptr)) {
                let header = &*buffer_header_ptr(buffer);
                if header.data_size() < header.data_capacity() {
                    chain_iter = Some(buffer);
                    break;
                }
            }
            Self {
                buf_ref: buffer,
                chain_iter,
            }
        }
    }
}
impl<T> std::io::Write for BufferRef<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        BufferWriter::new(&mut self.buffer_ref).write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        BufferWriter::new(&mut self.buffer_ref).flush()
    }
}
impl<'a> std::io::Write for BufferWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut bytes_written = 0;
        loop {
            if let Some(write_buffer) = self.chain_iter {
                let mut header = unsafe { &mut *buffer_header_ptr_mut(write_buffer) };
                let data_ptr = buffer_data_ptr(header, write_buffer);
                let bytes_remaining = header.data_capacity() - header.data_size();
                let to_write = core::cmp::min(bytes_remaining as usize, buf.len() - bytes_written);
                if to_write > 0 {
                    unsafe {
                        let write_start = data_ptr
                            .as_ptr()
                            .add((header.data_start_offset() + header.data_size()) as usize);
                        core::ptr::copy(buf.as_ptr(), write_start, to_write);
                    }
                    *header.data_size_mut() += to_write as u32;
                }
                bytes_written += to_write;
                if header.data_capacity() - header.data_size() == 0 {
                    self.chain_iter = NonNull::new(header.next.load(Ordering::Relaxed));
                }
                if buf.len() - bytes_written == 0 {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(bytes_written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
