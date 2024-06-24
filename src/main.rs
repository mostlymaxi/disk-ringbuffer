use std::{borrow::Cow, slice, str, sync::atomic::AtomicUsize};

const QUEUE_SIZE: usize = (4096 * 20_000) - (usize::BITS / 8) as usize;

#[derive(Debug)]
enum PageError {
    PageFull = -10,
    PageExists = -11,
    ReadError = -12,
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

struct Page(*mut CPage);

unsafe impl Send for Page {}

impl Drop for Page {
    fn drop(&mut self) {
        unsafe { raw_qpage_drop(self.0) };
    }
}

impl Page {
    fn new<P: AsRef<str>>(path: P) -> Self {
        let path = path.as_ref();
        let c_page = unsafe { raw_qpage_new_rs(path.as_ptr(), path.len()) };

        Page(c_page)
    }

    fn push<T: AsRef<[u8]>>(&self, input: T) -> Result<usize, PageError> {
        let input = input.as_ref();

        unsafe {
            match raw_qpage_push(self.0, input.as_ptr(), input.len()) {
                -10 => Err(PageError::PageFull),
                i @ 0.. => Ok(i as usize),
                _ => unreachable!(),
            }
        }
    }

    // TODO: think of a better way to return result with READ_EMPTY in mind
    fn pop(&self, start_byte: usize) -> Result<Cow<'_, str>, i32> {
        let slice = unsafe {
            let cs = raw_qpage_pop(self.0, start_byte);

            if cs.read_status < 0 {
                return Err(cs.read_status);
            }

            slice::from_raw_parts(cs.ptr, cs.len)
        };

        Ok(String::from_utf8_lossy(slice))
    }
}

struct RingBuf {}

#[test]
fn sequential_test() {
    const NUM: usize = 5_000_000;
    const FILE: &str = "testing_sequential_test";

    let now = std::time::Instant::now();

    let x = Page::new(FILE);
    let mut read_idx = 0;

    for i in 0..NUM {
        let i = i.to_string();
        let _ = x.push(&i).unwrap();
    }

    for i in 0..NUM {
        let p = x.pop(read_idx).unwrap();
        read_idx += p.len() + 1;
        assert!(i.to_string() == p);
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_file(FILE).unwrap();
}

/// TODO
/// make simple polling ringbuf
/// (figure out steps needed for that lol^)

fn main() {
    let x = Page::new("testing");
    x.push("asdf").unwrap();
    let out = x.pop(0).unwrap();

    eprintln!("{:#?}", out);

    std::fs::remove_file("testing").unwrap();
}
