use super::{BufferRef, RawBufferRef};
impl<T> std::io::Write for BufferRef<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer_ref.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer_ref.flush()
    }
}
impl std::io::Write for RawBufferRef {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let data_ptr = self.data_ptr();
        let mut header = self.buffer_header_mut();
        let bytes_remaining =
            header.data_capacity() - header.data_size() - header.data_start_offset();
        let to_write = core::cmp::min(bytes_remaining as usize, buf.len());
        if to_write > 0 {
            unsafe {
                let write_start = data_ptr
                    .as_ptr()
                    .add((header.data_start_offset() + header.data_size()) as usize);
                core::ptr::copy(buf.as_ptr(), write_start, to_write);
            }
            *header.data_size_mut() += to_write as u32;
        }
        Ok(to_write)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
