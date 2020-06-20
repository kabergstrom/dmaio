use super::*;
use crate::alloc::{
    alloc::Layout,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use crate::iocp::{IOCPQueue, IOCPQueueBuilder, IOCPQueueHandle};
use crate::Result;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
use std::future::Future;
unsafe fn create_bufpool(data_size: usize, num_buffers: usize) -> BufferPool<()> {
    BufferPool::<()>::new(
        num_buffers,
        Layout::from_size_align(data_size, 1).unwrap(),
        super::bufpool::virtual_alloc,
        |ptr, size| super::bufpool::virtual_free(ptr.as_ptr(), size).unwrap(),
        (),
    )
    .unwrap()
}
unsafe fn create_iocp() -> IOCPQueue {
    let mut iocp_builder = IOCPQueueBuilder::new(1).unwrap();
    iocp_builder.build().unwrap()
}
fn with_ctx<F: Future<Output = ()> + Send + 'static, T: FnOnce(BufferPool<()>) -> F>(
    data_size: usize,
    num_buffers: usize,
    func: T,
) {
    unsafe {
        let finished = Arc::new(AtomicBool::new(false));
        let bufpool = create_bufpool(data_size, num_buffers);
        let mut iocp = create_iocp();
        let handle = iocp.handle();
        let future = func(bufpool);
        let finished_clone = finished.clone();
        handle.spawn(async move {
            future.await;
            finished_clone.store(true, Ordering::Relaxed)
        });
        let mut num_iterations = 0;
        while !finished.load(Ordering::Relaxed) {
            iocp.poll(Some(1));
            num_iterations += 1;
            if num_iterations > 5 {
                panic!("waited too long");
            }
        }
    }
}
#[test]
fn buf_alloc_free() {
    unsafe {
        with_ctx(256, 1, |bufpool| async move {
            let buf = bufpool.try_alloc(None);
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf);
            let buf = bufpool.try_alloc(None);
            assert!(buf.is_some());
            drop(buf);
        });
    }
}

#[test]
fn buf_read_write() {
    unsafe {
        with_ctx(256, 1, |bufpool| async move {
            use std::io::{Read, Write};
            let mut buf = bufpool.alloc().await;
            let ref_value = [1, 2, 3];
            assert_eq!(3, buf.write(&ref_value).unwrap());
            let mut value = [0u8; 3];
            let mut reader = buf.reader();
            assert_eq!(3, reader.read(&mut value).unwrap());
            assert_eq!(value, ref_value);
            assert_eq!(0, reader.read(&mut value).unwrap());
        });
    }
}

#[test]
fn buf_write_overflow_fail() {
    unsafe {
        with_ctx(256, 1, |bufpool| async move {
            use std::io::{Read, Write};
            let buf_data_size = 256;
            let mut buf = bufpool.alloc().await;
            let ref_value = [3u8; 512]; // try to write more than the buffer can hold
            assert_eq!(buf_data_size, buf.write(&ref_value).unwrap());
            assert_eq!(0, buf.write(&ref_value).unwrap());
        });
    }
}

#[test]
fn buf_reserve_front() {
    unsafe {
        use std::io::Write;
        let buf_data_size = 256;
        with_ctx(256, 1, |bufpool| async move {
            let mut buf = bufpool.alloc().await;
            assert_eq!(true, buf.try_reserve_front(255));
            let ref_value = [3u8; 512]; // try to write more than the buffer can hold
            assert_eq!(1, buf.write(&ref_value).unwrap());
            assert_eq!(false, buf.try_reserve_front(1));
        });
    }
}

#[test]
fn buf_append_buffer_read() {
    unsafe {
        use std::io::{Read, Write};
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut ref_value = [3u8; 512]; // try to write more than the buffer can hold
            for i in 0..ref_value.len() {
                ref_value[i] = i as u8;
            }
            assert_eq!(256, buf1.write(&ref_value[0..256]).unwrap());
            assert_eq!(256, buf2.write(&ref_value[256..512]).unwrap());
            buf1.append_buffer(buf2);
            let mut read_value = [0u8; 512];
            let mut reader = buf1.reader();
            assert_eq!(512, reader.read(&mut read_value).unwrap());
            for i in 0..512 {
                assert_eq!(read_value[i], ref_value[i]);
            }
        });
    }
}

#[test]
fn buf_prepend_buffer_read() {
    unsafe {
        use std::io::{Read, Write};
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut ref_value = [0u8; 512]; // try to write more than the buffer can hold
            for i in 0..ref_value.len() {
                ref_value[i] = i as u8;
            }
            assert_eq!(256, buf1.write(&ref_value[0..256]).unwrap());
            assert_eq!(256, buf2.write(&ref_value[256..512]).unwrap());
            println!("prepending");
            buf2.prepend_buffer(buf1);
            println!("prepended");
            let mut read_value = [0u8; 512];
            let mut reader = buf2.reader();
            println!("reading data");
            assert_eq!(512, reader.read(&mut read_value).unwrap());
            for i in 0..512 {
                assert_eq!(read_value[i], ref_value[i]);
            }
        });
    }
}

#[test]
#[should_panic(
    expected = "must have exclusive access to `buffer` when appending - no handles may be active"
)]
fn buf_append_fail_nonexclusive() {
    unsafe {
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let handle = buf2.make_handle();
            buf1.append_buffer(buf2);
            drop(handle);
        });
    }
}

#[test]
#[should_panic(
    expected = "must have exclusive access to `buffer` when appending - no handles may be active"
)]
fn buf_prepend_fail_nonexclusive() {
    unsafe {
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let handle = buf2.make_handle();
            buf1.prepend_buffer(buf2);
            drop(handle);
        });
    }
}

#[test]
fn buf_append_ok_nonexclusive_self() {
    unsafe {
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let handle = buf1.make_handle();
            buf1.append_buffer(buf2);
            drop(handle);
        });
    }
}

#[test]
fn buf_prepend_ok_nonexclusive_self() {
    unsafe {
        with_ctx(256, 2, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let handle = buf1.make_handle();
            buf1.prepend_buffer(buf2);
            drop(handle);
        });
    }
}

#[test]
fn buf_chain_append_prepend_drop() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.append_buffer(buf3);
            drop(buf1);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_prepend_drop() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.prepend_buffer(buf3);
            drop(buf1);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_append_drop() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.append_buffer(buf2);
            buf1.append_buffer(buf3);
            drop(buf1);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_append_drop_handle() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.append_buffer(buf2);
            buf1.append_buffer(buf3);
            let handle = buf1.make_handle();
            drop(buf1);
            drop(handle);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_prepend_drop_handle() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.prepend_buffer(buf3);
            let handle = buf1.make_handle();
            drop(buf1);
            drop(handle);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_prepend_append_drop_handle() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.append_buffer(buf3);
            let handle = buf1.make_handle();
            drop(buf1);
            drop(handle);
            let mut buf1 = bufpool.try_alloc(None);
            let mut buf2 = bufpool.try_alloc(None);
            let mut buf3 = bufpool.try_alloc(None);
            assert!(buf1.is_some());
            assert!(buf2.is_some());
            assert!(buf3.is_some());
            assert!(bufpool.try_alloc(None).is_none());
            drop(buf1);
            drop(buf2);
            drop(buf3);
        });
    }
}

#[test]
fn buf_chain_append_owner_test() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.append_buffer(buf2);
            buf1.append_buffer(buf3);

            let begin = buf1.chain_begin();
            let end = buf1.chain_end();
            assert!(begin == buf1);
            assert!(end != buf1);
            let buf1_handle = buf1.make_handle();
            assert!(end.chain_owner() == Some(buf1_handle));
        });
    }
}

#[test]
fn buf_chain_prepend_owner_test() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.prepend_buffer(buf3);

            let begin = buf1.chain_begin();
            let end = buf1.chain_end();
            assert!(begin != buf1);
            assert!(end == buf1);
            let buf1_handle = buf1.make_handle();
            assert!(end.chain_owner() == Some(buf1_handle));
        });
    }
}

#[test]
fn buf_chain_append_upgrade_test() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.append_buffer(buf2);
            buf1.append_buffer(buf3);

            let buf1_handle = buf1.make_handle();
            drop(buf1);
            for buf in buf1_handle.chain_begin().chain_iter_forward() {
                assert!(buf.try_upgrade(None).is_ok());
            }
        });
    }
}

#[test]
fn buf_chain_prepend_upgrade_test() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.prepend_buffer(buf3);

            let buf1_handle = buf1.make_handle();
            drop(buf1);
            for buf in buf1_handle.chain_end().chain_iter_reverse() {
                assert!(buf.try_upgrade(None).is_ok());
            }
        });
    }
}

#[test]
fn buf_chain_read_write() {
    unsafe {
        with_ctx(256, 3, |bufpool| async move {
            let mut buf1 = bufpool.alloc().await;
            let mut buf2 = bufpool.alloc().await;
            let mut buf3 = bufpool.alloc().await;
            buf1.prepend_buffer(buf2);
            buf1.prepend_buffer(buf3);
            use std::io::{Read, Write};
            let mut ref_value = [0u8; 256 * 3];
            for i in 0..ref_value.len() {
                ref_value[i] = i as u8;
            }
            assert_eq!(ref_value.len(), buf1.write(&ref_value).unwrap());
            let mut read_value = [0u8; 256 * 3];
            let mut reader = buf1.reader();
            assert_eq!(ref_value.len(), reader.read(&mut read_value).unwrap());
            assert_eq!(0, reader.read(&mut read_value).unwrap());
            for i in 0..ref_value.len() {
                assert_eq!(read_value[i], ref_value[i]);
            }
        });
    }
}

// #[test]
// fn buf_chain_reserve() {
//     todo!("test reserve in buffer chains")
// }

// #[test]
// fn buf_chain_prepend_write() {
//     todo!("test prepend write in buffer chains")
// }
