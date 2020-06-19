use super::{RawBufferHandle, RawBufferRef};
/// Implements `std::io::Read` for a BufferRef
#[doc(hidden)]
pub struct BufferReader<'a> {
    pub(super) buffer: &'a RawBufferRef, // used to ensure lifetime is correct
    pub(super) chain_iter: Option<RawBufferHandle>,
    pub(super) chain_buffer_pos: u32, // position in chain_iter
}
impl<'a> std::io::Read for BufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            if let Some(read_buffer) = self.chain_iter.as_ref() {
                let header = unsafe { read_buffer.buffer_header() };
                let bytes_remaining = header.data_size() - self.chain_buffer_pos;
                let to_read = core::cmp::min(bytes_remaining as usize, buf.len() - bytes_read);
                if to_read > 0 {
                    unsafe {
                        let src_ptr = read_buffer
                            .data_ptr()
                            .as_ptr()
                            .add(header.data_start_offset() as usize);
                        let dst_ptr = buf.as_mut_ptr();
                        core::ptr::copy(src_ptr, dst_ptr, to_read);
                    }
                }
                self.chain_buffer_pos += to_read as u32;
                bytes_read += to_read;
                if to_read == bytes_remaining as usize {
                    self.chain_buffer_pos = 0;
                    self.chain_iter = header
                        .next
                        .map(|ptr| unsafe { super::handle::make_handle(ptr) });
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
