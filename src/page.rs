use std::path::PathBuf;
use std::{borrow::Cow, slice, str, sync::atomic::AtomicUsize};

const QUEUE_SIZE: usize = 4096 * 32_000; // (4096 * 20_000) - (usize::BITS / 8) as usize;

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

pub struct Page {
    raw: *mut CPage,
    path: PathBuf,
}

unsafe impl Send for Page {}

impl Drop for Page {
    fn drop(&mut self) {
        unsafe { raw_qpage_drop(self.raw) };
    }
}

impl Clone for Page {
    fn clone(&self) -> Page {
        Page::new(self.path.to_str().expect("not unicode path"))
    }
}

impl Page {
    pub fn new<P: AsRef<str>>(path: P) -> Self {
        let path = path.as_ref();
        let c_page = unsafe { raw_qpage_new_rs(path.as_ptr(), path.len()) };

        Page {
            raw: c_page,
            path: path.into(),
        }
    }

    pub fn try_push<T: AsRef<[u8]>>(&self, input: T) -> Result<usize, i32> {
        let input = input.as_ref();

        unsafe {
            match raw_qpage_push(self.raw, input.as_ptr(), input.len()) {
                i @ ..=-1 => Err(i),
                // a return value of 0 implies the page is full
                i @ 0.. => Ok(i as usize),
            }
        }
    }

    // TODO: think of a better way to return result with READ_EMPTY in mind
    pub fn try_pop(&self, start_byte: usize) -> Result<Option<ReadResult>, i32> {
        let slice = unsafe {
            let cs = raw_qpage_pop(self.raw, start_byte);

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
    }

    for i in 0..NUM {
        let p = x.try_pop(read_idx).unwrap().unwrap();
        let p = match p {
            ReadResult::Msg(m) => m,
            ReadResult::Continue => panic!("todo"),
        };

        read_idx += p.len() + 1;
        assert_eq!(i.to_string(), p);
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_file(FILE).unwrap();
}
