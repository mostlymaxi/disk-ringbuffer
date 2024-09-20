use crate::qpage::{self, PopResult, PushResult, QPage};
use mmap_wrapper::MmapMutWrapper;
use static_assertions::const_assert;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

pub const DEFAULT_INTERNAL_BUF_SIZE: usize = 4096;
const_assert!(DEFAULT_INTERNAL_BUF_SIZE < qpage::DEFAULT_MAX_MSG_SIZE);

#[derive(thiserror::Error, Debug)]
pub enum RingbufError {
    #[error("invalid read")]
    ReadError,
    #[error(transparent)]
    QError(#[from] crate::qpage::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

const PAGE_EXT: &str = "page.bin";
const INFO_NAME: &str = ".info";

#[derive(Clone)]
pub struct Sender {}
#[derive(Clone)]
pub struct Receiver {}

#[derive(Clone)]
pub struct DiskRing<T> {
    _kind: PhantomData<T>,
    path: PathBuf,
    read_byte: usize,
    qpage_no: usize,
    qpage: MmapMutWrapper<QPage>,
    diskring_info: MmapMutWrapper<DiskRingInfo>,
}

#[repr(C)]
pub struct DiskRingInfo {
    max_qpages: AtomicUsize,
    qpage_count: RwLock<usize>,
}

impl DiskRingInfo {
    fn new<P: AsRef<Path>>(path: P) -> Result<MmapMutWrapper<DiskRingInfo>, RingbufError> {
        // fails when disk is full
        // or when parent directories don't exist
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let _ = f.set_len(std::mem::size_of::<Self>() as u64);
        let m = unsafe { memmap2::MmapMut::map_mut(&f)? };

        Ok(unsafe { MmapMutWrapper::<Self>::new(m) })
    }
}

pub fn set_max_qpage<P: AsRef<Path>>(path: P, val: usize) -> Result<(), RingbufError> {
    let mut diskring_info = DiskRingInfo::new(path.as_ref().join(INFO_NAME))?;

    let _qpage_count_lock = diskring_info
        .get_inner()
        .qpage_count
        .write()
        .expect("unpoisoned lock");

    diskring_info
        .get_inner()
        .max_qpages
        .store(val, Ordering::Relaxed);

    Ok(())
}

pub fn new<P: AsRef<Path>>(
    path: P,
) -> Result<(DiskRing<Sender>, DiskRing<Receiver>), RingbufError> {
    std::fs::create_dir_all(path.as_ref())?;

    let qpage_no = get_qpage_count_static(&path);
    let qpage = QPage::new(
        path.as_ref()
            .join(qpage_no.to_string())
            .with_extension(PAGE_EXT),
    )?;

    let diskring_info = DiskRingInfo::new(path.as_ref().join(INFO_NAME))?;

    Ok((
        DiskRing {
            _kind: PhantomData,
            path: path.as_ref().into(),
            read_byte: 0,
            diskring_info: diskring_info.clone(),
            qpage: qpage.clone(),
            qpage_no,
        },
        DiskRing {
            _kind: PhantomData,
            path: path.as_ref().into(),
            read_byte: 0,
            diskring_info,
            qpage,
            qpage_no,
        },
    ))
}

impl Iterator for DiskRing<Receiver> {
    type Item = Result<Option<String>, RingbufError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.pop())
    }
}

impl DiskRing<Receiver> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<DiskRing<Receiver>, RingbufError> {
        let qpage_no = get_qpage_count_static(&path);
        let qpage = QPage::new(
            path.as_ref()
                .join(qpage_no.to_string())
                .with_extension(PAGE_EXT),
        )?;

        let diskring_info = DiskRingInfo::new(path.as_ref().join(INFO_NAME))?;

        Ok(DiskRing {
            _kind: PhantomData,
            path: path.as_ref().into(),
            read_byte: 0,
            diskring_info: diskring_info.clone(),
            qpage: qpage.clone(),
            qpage_no,
        })
    }

    fn page_flip(&mut self) -> Result<(), RingbufError> {
        let max_qpages = self
            .diskring_info
            .get_inner()
            .max_qpages
            .load(Ordering::Relaxed);

        if max_qpages > 0 {
            let qpage_count = self
                .diskring_info
                .get_inner()
                .qpage_count
                .read()
                .expect("unpoisoned lock");

            self.qpage_no =
                std::cmp::max(self.qpage_no + 1, qpage_count.saturating_sub(max_qpages));
        } else {
            self.qpage_no += 1;
        }

        self.read_byte = 0;
        self.qpage = QPage::new(
            self.path
                .join(self.qpage_no.to_string())
                .with_extension(PAGE_EXT),
        )?;

        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<String>, RingbufError> {
        loop {
            match self.qpage.get_inner().try_pop(self.read_byte)? {
                PopResult::Msg(m) => {
                    self.read_byte += m.len() + size_of::<qpage::MsgLengthType>();
                    return Ok(Some(String::from_utf8_lossy(m).to_string()));
                }
                PopResult::NoNewMsgs => return Ok(None),
                PopResult::PageDone => {}
            };

            self.page_flip()?;
        }
    }
}

impl DiskRing<Sender> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<DiskRing<Sender>, RingbufError> {
        let qpage_no = get_qpage_count_static(&path);
        let qpage = QPage::new(
            path.as_ref()
                .join(qpage_no.to_string())
                .with_extension(PAGE_EXT),
        )?;

        let diskring_info = DiskRingInfo::new(path.as_ref().join(INFO_NAME))?;

        Ok(DiskRing {
            _kind: PhantomData,
            path: path.as_ref().into(),
            read_byte: 0,
            diskring_info: diskring_info.clone(),
            qpage: qpage.clone(),
            qpage_no,
        })
    }

    fn page_flip(&mut self) -> Result<(), std::io::Error> {
        let qpage_count = self
            .diskring_info
            .get_inner()
            .qpage_count
            .read()
            .expect("unpoisoned lock");

        if self.qpage_no < *qpage_count {
            self.qpage_no += 1;
            return Ok(());
        }

        if self.qpage_no == *qpage_count {
            drop(qpage_count);

            let mut qpage_count = self
                .diskring_info
                .get_inner()
                .qpage_count
                .write()
                .expect("unpoisoned lock");

            if self.qpage_no < *qpage_count {
                self.qpage_no += 1;
                return Ok(());
            }

            *qpage_count += 1;
            self.qpage_no += 1;

            let max_qpages = self
                .diskring_info
                .get_inner()
                .max_qpages
                .load(Ordering::Relaxed);

            // setting max_total_pages to zero implies an unbounded ringbuf / queue
            if max_qpages == 0 {
                return Ok(());
            }

            if *qpage_count >= max_qpages {
                std::fs::remove_file(
                    self.path
                        .join((*qpage_count - max_qpages).to_string())
                        .with_extension(PAGE_EXT),
                )?;
            }
        }

        Ok(())
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) -> Result<usize, RingbufError> {
        loop {
            match self.qpage.get_inner().try_push(input.as_ref())? {
                PushResult::BytesWritten(x) => return Ok(x),
                PushResult::PageFull => {}
            }

            self.page_flip()?;

            self.qpage = QPage::new(
                self.path
                    .join(self.qpage_no.to_string())
                    .with_extension(PAGE_EXT),
            )?;
        }
    }
}

fn get_qpage_count_static<P: AsRef<Path>>(path: P) -> usize {
    let Ok(mut diskring_info) = DiskRingInfo::new(path.as_ref().join(INFO_NAME)) else {
        return 0;
    };

    let qpage_count = diskring_info
        .get_inner()
        .qpage_count
        .read()
        .expect("unpoisoned lock");

    *qpage_count
}

#[test]
fn seq_test() {
    let test_dir_path = "test-seq";
    let (mut tx, mut rx) = new(test_dir_path).unwrap();

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
    let (mut tx, mut rx) = new(test_dir_path).unwrap();

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
fn spsc_test() {
    let test_dir_path = "test-spsc";
    let (mut tx, mut rx) = new(test_dir_path).unwrap();

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

    let (tx, mut rx) = new(test_dir_path).unwrap();

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
