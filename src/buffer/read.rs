use super::buffer::{buffer_data_ptr, buffer_header_ptr};
use super::{RawBufferHandle, RawBufferRef};
use core::ptr::NonNull;
/// Implements `std::io::Read` for a BufferRef
#[doc(hidden)]
pub struct BufferReader<'a> {
    pub(super) buffer: &'a RawBufferRef, // used to ensure lifetime is correct
    pub(super) chain_iter: Option<NonNull<u8>>,
    pub(super) chain_buffer_pos: u32, // position in chain_iter
}
impl<'a> std::io::Read for BufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            if let Some(read_buffer) = self.chain_iter {
                let header = unsafe { &*buffer_header_ptr(read_buffer) };
                let bytes_remaining = header.data_size() - self.chain_buffer_pos;
                let to_read = core::cmp::min(bytes_remaining as usize, buf.len() - bytes_read);
                if to_read > 0 {
                    unsafe {
                        let src_ptr = buffer_data_ptr(header, read_buffer).as_ptr().add(
                            header.data_start_offset() as usize + self.chain_buffer_pos as usize,
                        );
                        let dst_ptr = buf.as_mut_ptr().add(bytes_read);
                        core::ptr::copy(src_ptr, dst_ptr, to_read);
                    }
                }
                self.chain_buffer_pos += to_read as u32;
                bytes_read += to_read;
                if to_read == bytes_remaining as usize {
                    self.chain_iter = header.next;
                    unsafe {
                        self.chain_buffer_pos = self
                            .chain_iter
                            .map(|next_ptr| (&*buffer_header_ptr(next_ptr)).data_start_offset())
                            .unwrap_or(0);
                    }
                }
                if to_read == 0 {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(bytes_read as usize)
    }
}
