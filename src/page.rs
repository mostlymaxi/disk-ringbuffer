use super::*;
use std::path::PathBuf;
use std::{borrow::Cow, slice, str};

/// a wrapper around read results that contains either the message or
/// informs the reader that it finished reading the page and should open
/// the next one
pub enum ReadResult<'a> {
    Msg(Cow<'a, str>),
    Continue,
}

/// a convenience wrapper around CPage to keep track of the directory it was made in (path)
pub struct Page {
    raw: *mut RawQPage,
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

    /// attemps to push a message into the page and returns the number of bytes written.
    /// a return value of zero implies that the page is full and that the writer should try
    /// again on a new page
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

    /// attempts to pop a message from the page and returns an optional ReadResult. a
    /// None value implies that there are no new messages to read
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
