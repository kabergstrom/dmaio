use super::RawBufferRef;
/// Implements `std::io::Read` for a BufferRef
#[doc(hidden)]
pub struct BufferReader<'a> {
    pub(super) buffer: &'a RawBufferRef,
    pub(super) pos: u32,
}
impl<'a> std::io::Read for BufferReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let header = self.buffer.buffer_header();
        let bytes_remaining = header.data_size() - self.pos;
        if bytes_remaining <= 0 {
            return Ok(0);
        }
        let to_read = core::cmp::min(bytes_remaining as usize, buf.len());
        if to_read > 0 {
            unsafe {
                let src_ptr = self
                    .buffer
                    .data_ptr()
                    .as_ptr()
                    .add(header.data_start_offset() as usize);
                let dst_ptr = buf.as_mut_ptr();
                core::ptr::copy(src_ptr, dst_ptr, to_read);
            }
        }
        self.pos += to_read as u32;
        Ok(to_read as usize)
    }
}
