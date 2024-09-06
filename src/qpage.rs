use core::panic;
use std::cmp;
use std::sync::atomic::{AtomicUsize, Ordering};

use mmap_wrapper::MmapMutWrapper;

const DEFAULT_QUEUE_SIZE: usize = 4 + 2_usize.pow(32) - 1;
// 0000 0001 0000 ....
const QUEUE_MAGIC_NUM: usize = 0b1 << ((size_of::<usize>() * 8) - 8);
// 0000 0000 1111 ....
const QUEUE_MAGIC_MASK: usize = QUEUE_MAGIC_NUM - 1;

struct QPage {
    write_idx_lock: AtomicUsize,
    last_safe_write_idx: AtomicUsize,
    buf: [u8; DEFAULT_QUEUE_SIZE],
}

#[derive(Debug)]
struct QPageError;

enum QPagePopMsg<'a> {
    Msg(&'a [u8]),
    NoNewMsgs,
    PageDone,
}

impl QPage {
    fn new<P: AsRef<str>>(path: P) -> MmapMutWrapper<QPage> {
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())
            .unwrap();

        let _ = f.set_len(std::mem::size_of::<QPage>() as u64);

        let m = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };

        unsafe { MmapMutWrapper::<QPage>::new(m) }
    }

    fn get_write_idx_spin(&self, start_byte: usize) -> usize {
        let end_byte = self.last_safe_write_idx.load(Ordering::Relaxed);

        let end_byte = match start_byte.cmp(&end_byte) {
            cmp::Ordering::Greater => loop {
                let end_byte = self.write_idx_lock.load(Ordering::Acquire);

                if (end_byte & !QUEUE_MAGIC_MASK) == 0 {
                    let _ = self
                        .last_safe_write_idx
                        .fetch_max(end_byte, Ordering::Relaxed);

                    break end_byte;
                }

                core::hint::spin_loop();
            },
            _ => end_byte,
        };

        end_byte.min(DEFAULT_QUEUE_SIZE)
    }

    fn pop(&self, start_byte: usize) -> Result<QPagePopMsg, QPageError> {
        let end_byte = self.get_write_idx_spin(start_byte);

        if end_byte < start_byte {
            unreachable!();
        }

        if end_byte == start_byte {
            return Ok(QPagePopMsg::NoNewMsgs);
        }

        if self.buf[start_byte] == 0xFD {
            return Ok(QPagePopMsg::PageDone);
        }

        Ok(QPagePopMsg::Msg(&self.buf[start_byte..end_byte]))
    }

    //    u32        |  [u8]
    // length of msg |  msg
    fn push_raw(&mut self, msgs: &[u8]) -> Result<usize, QPageError> {
        let start_idx = self
            .write_idx_lock
            .fetch_add(QUEUE_MAGIC_NUM + msgs.len(), Ordering::Relaxed);

        if start_idx & !QUEUE_MAGIC_MASK == 0 {
            return Err(QPageError);
        }

        let start_idx = start_idx & QUEUE_MAGIC_MASK;

        // checking if the queue has enough space
        if start_idx + msgs.len() >= DEFAULT_QUEUE_SIZE - 1 {
            // adding marker that queue is full
            // this only has to happen once
            if start_idx < DEFAULT_QUEUE_SIZE {
                self.buf[start_idx] = 0xFD;
            }

            // subtracting number of writers
            self.write_idx_lock
                .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

            return Err(QPageError); // Page Full
        }

        self.buf[start_idx..start_idx + msgs.len()].copy_from_slice(msgs);

        self.write_idx_lock
            .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

        Ok(msgs.len())
    }

    fn push(&mut self, msg: &[u8]) -> Result<usize, QPageError> {
        if msg.len() > u32::MAX as usize {
            return Err(QPageError);
        }

        let start_idx = self.write_idx_lock.fetch_add(
            QUEUE_MAGIC_NUM + size_of::<u32>() + msg.len(),
            Ordering::Relaxed,
        );

        if start_idx & !QUEUE_MAGIC_MASK == 0 {
            return Err(QPageError);
        }

        let start_idx = start_idx & QUEUE_MAGIC_MASK;

        // checking if the queue has enough space
        if start_idx + size_of::<u32>() + msg.len() >= DEFAULT_QUEUE_SIZE - 1 {
            // adding marker that queue is full
            // this only has to happen once
            if start_idx < DEFAULT_QUEUE_SIZE {
                self.buf[start_idx] = 0xFD;
            }

            // subtracting number of writers
            self.write_idx_lock
                .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

            return Err(QPageError); // Page Full
        }

        self.buf[start_idx..start_idx + size_of::<u32>()]
            .copy_from_slice(&(msg.len() as u32).to_be_bytes());
        self.buf[start_idx + size_of::<u32>()..start_idx + size_of::<u32>() + msg.len()]
            .copy_from_slice(msg);

        self.write_idx_lock
            .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

        Ok(msg.len() + size_of::<u32>())
    }
}
