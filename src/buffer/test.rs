use super::*;
use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
unsafe fn create_bufpool(data_size: usize, num_buffers: usize) -> BufferPool<()> {
    BufferPool::new(
        num_buffers,
        Layout::from_size_align(data_size, 1).unwrap(),
        super::bufpool::virtual_alloc,
        |ptr, size| super::bufpool::virtual_free(ptr, size).unwrap(),
        (),
    )
    .unwrap()
}
#[test]
fn buf_alloc_free() {
    unsafe {
        let bufpool = create_bufpool(1024, 1);
        let buf = bufpool.alloc();
        assert!(buf.is_some());
        assert!(bufpool.alloc().is_none());
        drop(buf);
        let buf = bufpool.alloc();
        assert!(buf.is_some());
        drop(buf);
    }
}

#[test]
fn buf_read_write() {
    unsafe {
        use std::io::{Read, Write};
        let bufpool = create_bufpool(1024, 1);
        let buf = bufpool.alloc();
        assert!(buf.is_some());
        let mut buf = buf.unwrap();
        let ref_value = [1, 2, 3];
        assert_eq!(3, buf.write(&ref_value).unwrap());
        let mut value = [0u8; 3];
        let mut reader = buf.reader();
        assert_eq!(3, reader.read(&mut value).unwrap());
        assert_eq!(value, ref_value);
        assert_eq!(0, reader.read(&mut value).unwrap());
    }
}

#[test]
fn buf_write_overflow_fail() {
    unsafe {
        use std::io::{Read, Write};
        let buf_data_size = 256;
        let bufpool = create_bufpool(buf_data_size, 1);
        let buf = bufpool.alloc();
        assert!(buf.is_some());
        let mut buf = buf.unwrap();
        let ref_value = [3u8; 512]; // try to write more than the buffer can hold
        assert_eq!(buf_data_size, buf.write(&ref_value).unwrap());
        assert_eq!(0, buf.write(&ref_value).unwrap());
    }
}

#[test]
fn buf_reserve_front() {
    unsafe {
        use std::io::Write;
        let buf_data_size = 256;
        let bufpool = create_bufpool(buf_data_size, 1);
        let buf = bufpool.alloc();
        assert!(buf.is_some());
        let mut buf = buf.unwrap();
        assert_eq!(255, buf.reserve_front(255));
        let ref_value = [3u8; 512]; // try to write more than the buffer can hold
        assert_eq!(1, buf.write(&ref_value).unwrap());
        assert_eq!(0, buf.reserve_front(1));
    }
}
