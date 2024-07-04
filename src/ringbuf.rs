use super::raw_qpage_drop;
use crate::page::{Page, ReadResult};
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;

/// A thread safe ringbuf receiver
#[derive(Clone)]
pub struct Reader {
    path: PathBuf,
    write_page_count: Arc<RwLock<usize>>,
    read_page_no: usize,
    read_page: Page,
    read_start_byte: usize,
    max_total_pages: usize,
    _lock: Arc<fslock::LockFile>,
}

/// A thread safe ringbuf sender
#[derive(Clone)]
pub struct Writer {
    path: PathBuf,
    write_page_count: Arc<RwLock<usize>>,
    write_page_no: usize,
    write_page: Page,
    max_total_pages: usize,
    _lock: Arc<fslock::LockFile>,
}

#[derive(Error, Debug)]
pub enum RingbufError {
    #[error("invalid read")]
    ReadError,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("conflicting ringbuf path")]
    RingbufExists,
}

impl From<i64> for RingbufError {
    fn from(error: i64) -> RingbufError {
        match error {
            -1 => RingbufError::ReadError,
            _ => unreachable!(),
        }
    }
}

const PAGE_EXT: &str = "page.bin";

fn check_valid_page(entry: DirEntry) -> Option<usize> {
    let path = entry.path();
    let file_name = path.file_name()?;
    let file_name = file_name.to_str()?;

    if !file_name.ends_with(PAGE_EXT) {
        return None;
    }

    let num = path.file_stem()?.to_str()?.parse().ok()?;

    Some(num)
}

fn find_pages<P: AsRef<Path>>(path: P) -> Result<usize, RingbufError> {
    let mut write_page_count = 0;

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let Some(num) = check_valid_page(entry) else {
            continue;
        };

        write_page_count = std::cmp::max(write_page_count, num);
    }

    Ok(write_page_count)
}

pub fn new<P: Into<PathBuf>>(path: P, max_pages: usize) -> Result<(Writer, Reader), RingbufError> {
    let path = path.into();
    std::fs::create_dir_all(&path)?;

    let mut file = fslock::LockFile::open(&path.join("rb.lock"))?;
    if !file.try_lock()? {
        return Err(RingbufError::RingbufExists);
    }
    let _lock = Arc::new(file);

    let latest_file_no = find_pages(&path)?;
    let wp_count = Arc::new(RwLock::new(latest_file_no));
    let page = Page::new(
        path.join(latest_file_no.to_string())
            .with_extension(PAGE_EXT)
            .to_str()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "can't convert extension to utf-8",
                )
            })?,
    );

    Ok((
        Writer {
            path: path.clone(),
            write_page_count: wp_count.clone(),
            write_page_no: 0,
            write_page: page.clone(),
            max_total_pages: max_pages,
            _lock: _lock.clone(),
        },
        Reader {
            path,
            write_page_count: wp_count,
            read_page_no: 0,
            read_page: page,
            read_start_byte: 0,
            max_total_pages: max_pages,
            _lock,
        },
    ))
}

impl Writer {
    pub fn super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing(&mut self) {
        self.write_page
            .super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();
    }

    fn page_flip(&mut self) -> Result<(), std::io::Error> {
        let page_count = self.write_page_count.read().expect("poisoned lock!");

        if self.write_page_no < *page_count {
            self.write_page_no += 1;
            return Ok(());
        }

        if self.write_page_no == *page_count {
            drop(page_count);

            let mut page_count = self.write_page_count.write().expect("poisoned lock!");

            if self.write_page_no < *page_count {
                self.write_page_no += 1;
                return Ok(());
            }

            *page_count += 1;
            self.write_page_no += 1;

            // setting max_total_pages to zero implies an unbounded ringbuf / queue
            if self.max_total_pages == 0 {
                return Ok(());
            }

            if *page_count >= self.max_total_pages {
                std::fs::remove_file(
                    self.path
                        .join((*page_count - self.max_total_pages).to_string())
                        .with_extension(PAGE_EXT)
                        .to_str()
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "can't convert extension to utf-8",
                            )
                        })?,
                )?
            }
        }

        Ok(())
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) -> Result<usize, RingbufError> {
        loop {
            let i = self.write_page.try_push(&input)?;

            if i > 0 {
                return Ok(i);
            }

            // a result of 0 implies a full page
            self.page_flip()?;

            self.write_page = Page::new(
                self.path
                    .join(self.write_page_no.to_string())
                    .with_extension(PAGE_EXT)
                    .to_str()
                    .expect("this should always be unicode"),
            );
        }
    }
}

impl Iterator for Reader {
    type Item = Result<Option<String>, RingbufError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.pop())
    }
}

impl Reader {
    pub fn super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing(&mut self) {
        self.read_page
            .super_unsafe_page_cleanup_never_call_this_unless_you_know_what_youre_doing();
    }

    #[cfg(feature = "fast-read")]
    pub fn pop(&mut self) -> Result<Option<String>, RingbufError> {
        const sizeof_usize: usize = std::mem::size_of::<usize>();

        loop {
            match self.read_page.try_pop(self.read_start_byte)? {
                None => return Ok(None), // no new messages
                Some(ReadResult::Continue) => {}
                Some(ReadResult::Msg(m)) => {
                    self.read_start_byte += m.len() + sizeof_usize + 1;
                    return Ok(Some(m.to_string()));
                }
            };

            if self.max_total_pages > 0 {
                let page_count = self.write_page_count.read().expect("poisoned lock!");

                self.read_page_no = std::cmp::max(
                    self.read_page_no + 1,
                    page_count.saturating_sub(self.max_total_pages),
                );
            } else {
                self.read_page_no += 1;
            }

            self.read_start_byte = 0;
            self.read_page = Page::new(
                self.path
                    .join(self.read_page_no.to_string())
                    .with_extension(PAGE_EXT)
                    .to_str()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "can't convert extension to utf-8",
                        )
                    })?,
            );
        }
    }

    #[cfg(not(feature = "fast-read"))]
    pub fn pop(&mut self) -> Result<Option<String>, RingbufError> {
        loop {
            match self.read_page.try_pop(self.read_start_byte)? {
                None => return Ok(None), // no new messages
                Some(ReadResult::Continue) => {}
                Some(ReadResult::Msg(m)) => {
                    self.read_start_byte += m.len() + 1;
                    return Ok(Some(m.to_string()));
                }
            };

            if self.max_total_pages > 0 {
                let page_count = self.write_page_count.read().expect("poisoned lock!");

                self.read_page_no = std::cmp::max(
                    self.read_page_no + 1,
                    page_count.saturating_sub(self.max_total_pages),
                );
            } else {
                self.read_page_no += 1;
            }

            self.read_start_byte = 0;
            self.read_page = Page::new(
                self.path
                    .join(self.read_page_no.to_string())
                    .with_extension(PAGE_EXT)
                    .to_str()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "can't convert extension to utf-8",
                        )
                    })?,
            );
        }
    }
}

#[test]
fn lock_test() {
    let test_dir_path = "test-lock";
    let (_tx, _rx) = new(test_dir_path, 2).unwrap();
    assert!(new(test_dir_path, 2).is_err());

    drop(_tx);
    drop(_rx);
    let (_tx, _rx) = new(test_dir_path, 2).unwrap();
    std::fs::remove_dir_all(test_dir_path).unwrap();
}

#[test]
fn seq_test() {
    let test_dir_path = "test-seq";
    let (mut tx, mut rx) = new(test_dir_path, 0).unwrap();

    let now = std::time::Instant::now();
    for i in 0..50_000_000 {
        tx.push(i.to_string()).unwrap();
    }

    for i in 0..50_000_000 {
        let m = rx.pop().unwrap().unwrap();
        assert_eq!(m, i.to_string());
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_dir_all(test_dir_path).unwrap();
}

#[test]
fn spsc_test() {
    let test_dir_path = "test-spsc";
    let (mut tx, mut rx) = new(test_dir_path, 0).unwrap();

    let now = std::time::Instant::now();
    let t = std::thread::spawn(move || {
        for i in 0..50_000_000 {
            tx.push(i.to_string()).unwrap();
        }
    });

    let mut i = 0;
    loop {
        if i == 50_000_000 {
            break;
        }

        let m = match rx.pop().unwrap() {
            Some(m) => m,
            None => continue,
        };

        assert_eq!(m, i.to_string());
        i += 1;
    }

    let _ = t.join().unwrap();

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_dir_all(test_dir_path).unwrap();
}

#[test]
fn mpsc_test() {
    let test_dir_path = "test-mpsc";
    let num_threads = 4;
    let mut threads = Vec::new();

    let (tx, mut rx) = new(test_dir_path, 0).unwrap();

    let now = std::time::Instant::now();

    for _ in 0..num_threads {
        let mut tx_clone = tx.clone();
        threads.push(std::thread::spawn(move || {
            for i in 0..50_000_000 / num_threads {
                tx_clone.push(i.to_string()).unwrap();
            }
        }));
    }

    drop(tx);

    let mut i = 0;
    loop {
        if i == 50_000_000 {
            break;
        }

        let _m = match rx.pop().unwrap() {
            Some(_m) => _m,
            None => continue,
        };

        i += 1;
    }

    for t in threads {
        t.join().unwrap();
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_dir_all(test_dir_path).unwrap();
}
