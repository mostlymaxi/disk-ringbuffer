use core::slice;
use std::cmp;
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use mmap_wrapper::MmapMutWrapper;
use static_assertions::const_assert;

type MsgLengthType = u32;
// const DEFAULT_QUEUE_SIZE: usize = 4 + 2_usize.pow(32) - 1;
pub const DEFAULT_QUEUE_SIZE: usize = 4 + 2_usize.pow(28) - 1;
pub const DEFAULT_MAX_MSG_SIZE: usize = 2_usize.pow(24) - 1;

const_assert!(DEFAULT_QUEUE_SIZE > DEFAULT_MAX_MSG_SIZE);
const_assert!(DEFAULT_MAX_MSG_SIZE < MsgLengthType::MAX as usize);
// 0000 0001 0000 ....
const QUEUE_MAGIC_NUM: usize = 0b1 << (usize::BITS - 8);
// 0000 0000 1111 ....
const QUEUE_MAGIC_MASK: usize = QUEUE_MAGIC_NUM - 1;

pub struct QPage {
    write_idx_lock: AtomicUsize,
    last_safe_write_idx: AtomicUsize,
    buf: [u8; DEFAULT_QUEUE_SIZE],
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    WriteIdxLockOverflow,
    MsgTooLong,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

pub enum PopResult<'a> {
    Msg(&'a [u8]),
    NoNewMsgs,
    PageDone,
}

pub enum PushResult {
    BytesWritten(usize),
    PageFull,
}

impl QPage {
    pub fn new<P: AsRef<Path>>(path: P) -> MmapMutWrapper<QPage> {
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();

        let _ = f.set_len(std::mem::size_of::<QPage>() as u64);

        let m = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };

        unsafe { MmapMutWrapper::<QPage>::new(m) }
    }

    fn get_write_idx_spin(&self, start_byte: usize) -> usize {
        let end_byte = self.last_safe_write_idx.load(Ordering::Relaxed);

        let end_byte = match start_byte.cmp(&end_byte) {
            cmp::Ordering::Greater | cmp::Ordering::Equal => loop {
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

    pub fn try_pop(&self, start_byte: usize) -> Result<PopResult, Error> {
        let end_byte = self.get_write_idx_spin(start_byte);

        if end_byte < start_byte {
            unreachable!();
        }

        if end_byte == start_byte {
            return Ok(PopResult::NoNewMsgs);
        }

        if self.buf[start_byte] == 0xFD {
            return Ok(PopResult::PageDone);
        }

        let msg_len = MsgLengthType::from_le_bytes(
            self.buf[start_byte..start_byte + size_of::<MsgLengthType>()]
                .try_into()
                .unwrap(),
        );

        let start_byte = start_byte + size_of::<MsgLengthType>();
        let end_byte = start_byte + msg_len as usize;

        Ok(PopResult::Msg(&self.buf[start_byte..end_byte]))
    }

    pub fn try_push_raw(&mut self, msgs: &[u8]) -> Result<PushResult, Error> {
        let start_idx = self
            .write_idx_lock
            .fetch_add(QUEUE_MAGIC_NUM + msgs.len(), Ordering::Relaxed);

        if ((start_idx + QUEUE_MAGIC_NUM) & !QUEUE_MAGIC_MASK) == 0 {
            return Err(Error::WriteIdxLockOverflow);
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

            return Ok(PushResult::PageFull);
        }

        self.buf[start_idx..start_idx + msgs.len()].copy_from_slice(msgs);

        self.write_idx_lock
            .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

        Ok(PushResult::BytesWritten(msgs.len()))
    }

    pub fn try_push(&self, msg: &[u8]) -> Result<PushResult, Error> {
        if msg.len() > DEFAULT_MAX_MSG_SIZE {
            return Err(Error::MsgTooLong);
        }

        let start_idx = self.write_idx_lock.fetch_add(
            QUEUE_MAGIC_NUM + size_of::<MsgLengthType>() + msg.len(),
            Ordering::Relaxed,
        );

        if ((start_idx + QUEUE_MAGIC_NUM) & !QUEUE_MAGIC_MASK) == 0 {
            return Err(Error::WriteIdxLockOverflow);
        }

        let start_idx = start_idx & QUEUE_MAGIC_MASK;

        // checking if the queue has enough space
        if start_idx + size_of::<MsgLengthType>() + msg.len() >= DEFAULT_QUEUE_SIZE - 1 {
            // adding marker that queue is full
            // this only has to happen once
            if start_idx < DEFAULT_QUEUE_SIZE {
                unsafe {
                    let super_scary_mut_buf = self.buf.as_ptr().cast_mut();
                    *super_scary_mut_buf.add(start_idx) = 0xFD;
                }
            }

            // subtracting number of writers
            self.write_idx_lock
                .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

            return Ok(PushResult::PageFull);
        }
        let super_scary_mutable_buf =
            unsafe { slice::from_raw_parts_mut(self.buf.as_ptr().cast_mut(), self.buf.len()) };

        super_scary_mutable_buf[start_idx..start_idx + size_of::<MsgLengthType>()]
            .copy_from_slice(&(msg.len() as MsgLengthType).to_le_bytes());
        super_scary_mutable_buf[start_idx + size_of::<MsgLengthType>()
            ..start_idx + size_of::<MsgLengthType>() + msg.len()]
            .copy_from_slice(msg);

        self.write_idx_lock
            .fetch_sub(QUEUE_MAGIC_NUM, Ordering::Release);

        Ok(PushResult::BytesWritten(
            msg.len() + size_of::<MsgLengthType>(),
        ))
    }
}
