use crate::qpage::{self, PopResult, PushResult, QPage};
use fs4::fs_std::FileExt;
use mmap_wrapper::MmapMutWrapper;
use static_assertions::const_assert;
use std::fs::{DirEntry, File};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

pub const DEFAULT_INTERNAL_BUF_SIZE: usize = 4096;
const_assert!(DEFAULT_INTERNAL_BUF_SIZE < qpage::DEFAULT_MAX_MSG_SIZE);

/// A thread safe ringbuf receiver
#[derive(Clone)]
pub struct Reader {
    info: MmapMutWrapper<ReaderInfo>,
    path: PathBuf,
    read_page_no: usize,
    read_page: MmapMutWrapper<QPage>,
    read_start_byte: usize,
    write_page_fslock: Arc<File>,
}

/// A thread safe ringbuf sender
pub struct Writer {
    path: PathBuf,
    write_page_count: Arc<RwLock<usize>>,
    write_page_no: usize,
    write_page: MmapMutWrapper<QPage>,
    _internal_buf: Vec<u8>,
    max_total_pages: usize,
    _lock: Arc<fslock::LockFile>,
}

struct Sender {}
struct Receiver {}

pub struct DiskRing<T> {
    _kind: PhantomData<T>,
    path: PathBuf,
    qpage_turn_lock: Arc<File>,
    qpage_no: usize,
    qpage: MmapMutWrapper<QPage>,
    diskring_info: MmapMutWrapper<DiskRingInfo>,
}

#[repr(C)]
pub struct DiskRingInfo {
    max_qpages: AtomicUsize,
    qpage_count: RwLock<usize>,
}

impl<T> DiskRing<T> {}
impl DiskRing<Receiver> {}
impl DiskRing<Sender> {
    fn page_flip(&mut self) -> Result<(), std::io::Error> {
        let qpage_count = self.diskring_info.get_inner().qpage_count.read().expect("poisoned lock!");

        if self.qpage_no < *qpage_count {
            self.qpage_no += 1;
            return Ok(());
        }

        if self.qpage_no == *qpage_count {
            drop(qpage_count);

            let mut qpage_count = self.diskring_info.get_inner().qpage_count.write().expect("poisoned lock!");

            if self.qpage_no < *qpage_count {
                self.qpage_no += 1;
                return Ok(());
            }

            *qpage_count += 1;
            self.qpage_no += 1;

            // setting max_total_pages to zero implies an unbounded ringbuf / queue
            if self.diskring_info.get_inner().max_qpages == 0 {
                return Ok(());
            }

            if *qpage_count >= self.diskring_info.get_inner().max_qpages {
                std::fs::remove_file(
                    self.path
                        .join((*qpage_count - self.diskring_info.get_inner().max_qpages.load(Ordering::Relaxed)).to_string())
                        .with_extension(PAGE_EXT))?;
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<usize, RingbufError> {
        if self._internal_buf.is_empty() {
            return Ok(0);
        }

        loop {
            match self
                .write_page
                .get_inner()
                .try_push_raw(&self._internal_buf)?
            {
                PushResult::BytesWritten(x) => {
                    self._internal_buf.clear();
                    return Ok(x);
                }
                PushResult::PageFull => {}
            }

            // a result of 0 implies a full page
            self.page_flip()?;

            self.write_page = QPage::new(
                self.path
                    .join(self.write_page_no.to_string())
                    .with_extension(PAGE_EXT)
                    .to_str()
                    .expect("this should always be unicode"),
                // TODO: YOU SHOULD NOT PANIC ON DROP OMG MAXI
            );
        }
    }

    pub fn push_buffered<T: AsRef<[u8]>>(&mut self, input: T) -> Result<usize, RingbufError> {
        if self._internal_buf.len() + input.as_ref().len() < DEFAULT_INTERNAL_BUF_SIZE {
            self.flush()?;
        }

        self._internal_buf
            .extend((input.as_ref().len() as qpage::MsgLengthType).to_le_bytes());
        self._internal_buf.extend_from_slice(input.as_ref());

        return Ok(input.as_ref().len() + size_of::<qpage::MsgLengthType>());
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) -> Result<usize, RingbufError> {
        loop {
            match self.write_page.get_inner().try_push(input.as_ref())? {
                PushResult::BytesWritten(x) => return Ok(x),
                PushResult::PageFull => {}
            }

            // a result of 0 implies a full page
            self.page_flip()?;

            self.write_page = QPage::new(
                self.path
                    .join(self.write_page_no.to_string())
                    .with_extension(PAGE_EXT)
                    .to_str()
                    .expect("this should always be unicode"),
            );
        }
    }
}

impl ReaderInfo {
    pub fn new<P: AsRef<Path>>(path: P) -> MmapMutWrapper<ReaderInfo> {
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();

        let _ = f.set_len(std::mem::size_of::<ReaderInfo>() as u64);

        let m = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };

        unsafe { MmapMutWrapper::<ReaderInfo>::new(m) }
    }
}

impl Clone for Writer {
    fn clone(&self) -> Self {
        Writer {
            path: self.path.clone(),
            write_page_count: self.write_page_count.clone(),
            write_page_no: self.write_page_no,
            write_page: self.write_page.clone(),
            _internal_buf: Vec::new(),
            max_total_pages: self.max_total_pages,
            _lock: self._lock.clone(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RingbufError {
    #[error("invalid read")]
    ReadError,
    #[error(transparent)]
    QError(#[from] crate::qpage::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("conflicting ringbuf path")]
    RingbufExists,
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

fn find_latest_page_num<P: AsRef<Path>>(path: P) -> Result<usize, RingbufError> {
    Ok(std::fs::read_dir(path)?
        .filter_map(|entry| entry.ok())
        .flat_map(check_valid_page)
        .max()
        .unwrap_or(0))
}

pub fn new<P: Into<PathBuf>>(path: P, max_pages: usize) -> Result<(Writer, Reader), RingbufError> {
    let path: PathBuf = path.into();
    std::fs::create_dir_all(&path)?;

    let latest_file_no = find_latest_page_num(&path)?;

    let wp_count = Arc::new(RwLock::new(latest_file_no));
    let page = QPage::new(
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

    // TODO: reset on clone otherwise side effects
    let _internal_buf = Vec::new();

    Ok((
        Writer {
            path: path.clone(),
            write_page_count: wp_count.clone(),
            write_page_no: 0,
            _internal_buf,
            write_page: page.clone(),
            max_total_pages: max_pages,
        },
        Reader {
            path,
            write_page_count: wp_count,
            read_page_no: 0,
            read_page: page,
            read_start_byte: 0,
            max_total_pages: max_pages,
        },
    ))
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl Writer {
impl Iterator for Reader {
    type Item = Result<Option<String>, RingbufError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.pop())
    }
}

impl Reader {
    fn page_flip(&mut self) -> Result<(), RingbufError> {
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
        self.read_page = QPage::new(
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

        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<String>, RingbufError> {
        loop {
            match self.read_page.get_inner().try_pop(self.read_start_byte)? {
                PopResult::Msg(m) => {
                    self.read_start_byte += m.len() + size_of::<qpage::MsgLengthType>();
                    return Ok(Some(String::from_utf8_lossy(m).to_string()));
                }
                PopResult::NoNewMsgs => return Ok(None),
                PopResult::PageDone => {}
            };

            self.page_flip()?;
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
        let m = rx.pop().unwrap();
        assert_eq!(m, Some(i.to_string()));
    }

    eprintln!("took {} ms", now.elapsed().as_millis());

    std::fs::remove_dir_all(test_dir_path).unwrap();
}

#[test]
fn seq_buffered_test() {
    let test_dir_path = "test-seq-buf";
    let (mut tx, mut rx) = new(test_dir_path, 0).unwrap();

    let now = std::time::Instant::now();
    for i in 0..50_000_000 {
        tx.push_buffered(i.to_string()).unwrap();
    }
    tx.flush().unwrap();

    for i in 0..50_000_000 {
        let m = rx.pop().unwrap();
        assert_eq!(m, Some(i.to_string()));
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

    t.join().unwrap();

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
