use std::{borrow::Cow, slice, str, sync::atomic::AtomicUsize};

const QUEUE_SIZE: usize = (4096 * 20_000) - (usize::BITS / 8) as usize;

pub enum ReadResult<'a> {
    Msg(Cow<'a, str>),
    Continue,
}

#[repr(C)]
struct CSlice {
    len: cty::size_t,
    ptr: *const u8,
    read_status: cty::c_int,
}

#[repr(C)]
struct CPage {
    is_ready: AtomicUsize,
    write_idx_lock: AtomicUsize,
    last_safe_write_idx: AtomicUsize,
    buf: [cty::c_uchar; QUEUE_SIZE],
}

extern "C" {
    fn raw_qpage_new_rs(path: *const u8, path_len: usize) -> *mut CPage;
    fn raw_qpage_push(p: *mut CPage, buf: *const u8, len: usize) -> cty::c_int;
    fn raw_qpage_drop(p: *mut CPage);
    fn raw_qpage_pop(p: *const CPage, start_byte: usize) -> CSlice;
}

pub struct Page(*mut CPage);

unsafe impl Send for Page {}

impl Drop for Page {
    fn drop(&mut self) {
        unsafe { raw_qpage_drop(self.0) };
    }
}

impl Page {
    pub fn new<P: AsRef<str>>(path: P) -> Self {
        let path = path.as_ref();
        let c_page = unsafe { raw_qpage_new_rs(path.as_ptr(), path.len()) };

        Page(c_page)
    }

    pub fn try_push<T: AsRef<[u8]>>(&self, input: T) -> Result<usize, i32> {
        let input = input.as_ref();

        unsafe {
            match raw_qpage_push(self.0, input.as_ptr(), input.len()) {
                i @ ..=-1 => Err(i),
                // a return value of 0 implies the page is full
                i @ 0.. => Ok(i as usize),
            }
        }
    }

    // TODO: think of a better way to return result with READ_EMPTY in mind
    pub fn try_pop(&self, start_byte: usize) -> Result<Option<ReadResult>, i32> {
        let slice = unsafe {
            let cs = raw_qpage_pop(self.0, start_byte);

            let cs = match cs.read_status {
                i @ ..=-1 => return Err(i),
                0 => cs,
                1 => return Ok(Some(ReadResult::Continue)),
                2 => return Ok(None),
                _ => unreachable!(),
            };

            slice::from_raw_parts(cs.ptr, cs.len)
        };

        Ok(Some(ReadResult::Msg(String::from_utf8_lossy(slice))))
    }
}

// struct RingBuf {
// current page?
// current read page?
// current write page?
// }
//
// on push:
//  -

#[test]
fn sequential_test() {
    const NUM: usize = 5_000_000;
    const FILE: &str = "testing_sequential_test";

    let now = std::time::Instant::now();

    let x = Page::new(FILE);
    let mut read_idx = 0;

    for i in 0..NUM {
        let i = i.to_string();
        let _ = x.try_push(&i).unwrap();
        eprintln!("{:?}", i);
    }

    for i in 0..NUM {
        let p = x.try_pop(read_idx).unwrap().unwrap();
        let p = match p {
            ReadResult::Msg(m) => m,
            ReadResult::Continue => panic!("todo"),
        };

        read_idx += p.len() + 1;
        assert!(i.to_string() == p);
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_file(FILE).unwrap();
}

// TODO:
// make simple polling ringbuf
// (figure out steps needed for that lol^)
