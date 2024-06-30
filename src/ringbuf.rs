use crate::page::{Page, ReadResult};
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use thiserror::Error;

#[derive(Clone)]
pub struct Reader {
    path: PathBuf,
    write_page_count: Arc<RwLock<usize>>,
    read_page_no: usize,
    read_page: Page,
    read_start_byte: usize,
    max_total_pages: usize,
}

#[derive(Clone)]
pub struct Writer {
    path: PathBuf,
    write_page_count: Arc<RwLock<usize>>,
    write_page_no: usize,
    write_page: Page,
    max_total_pages: usize,
}

#[derive(Error, Debug)]
pub enum RingbufError {
    #[error("invalid read")]
    ReadError,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

impl From<i32> for RingbufError {
    fn from(error: i32) -> RingbufError {
        match error {
            -1 => RingbufError::ReadError,
            _ => unreachable!(),
        }
    }
}

const TEMP_MAX_TOTAL_PAGES: usize = 4;
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

// #[derive(Debug)]
// struct RingbufExists;

// fn lock<P: AsRef<Path>>(path: P) -> Result<(), RingbufExists> {
//     let path: &Path = path.as_ref();
//     std::fs::File::create_new(path.join("lock")).map_err(|_| RingbufExists)?;
//     Ok(())
// }

pub fn new<P: Into<PathBuf>>(path: P) -> Result<(Writer, Reader), RingbufError> {
    let path = path.into();
    std::fs::create_dir_all(&path)?;

    let latest_file_no = find_pages(&path)?;
    let wp_count = Arc::new(RwLock::new(latest_file_no));
    let page = Page::new(
        &path
            .join(latest_file_no.to_string())
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
            max_total_pages: TEMP_MAX_TOTAL_PAGES,
        },
        Reader {
            path,
            write_page_count: wp_count,
            read_page_no: 0,
            read_page: page,
            read_start_byte: 0,
            max_total_pages: TEMP_MAX_TOTAL_PAGES,
        },
    ))
}

impl Writer {
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

            if *page_count >= self.max_total_pages {
                std::fs::remove_file(
                    &self
                        .path
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
                &self
                    .path
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

            let page_count = self.write_page_count.read().expect("poisoned lock!");

            self.read_page_no = std::cmp::max(
                self.read_page_no + 1,
                page_count.saturating_sub(self.max_total_pages),
            );

            self.read_start_byte = 0;
            self.read_page = Page::new(
                &self
                    .path
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
fn seq_test() {
    let (mut tx, mut rx) = new("test-seq").unwrap();

    let now = std::time::Instant::now();
    for i in 0..50_000_000 {
        tx.push(i.to_string()).unwrap();
    }

    for i in 0..50_000_000 {
        let m = rx.pop().unwrap().unwrap();
        assert_eq!(m, i.to_string());
    }

    eprintln!("took {} ms", now.elapsed().as_millis());
}

#[test]
fn spsc_test() {
    let (mut tx, mut rx) = new("test-spsc").unwrap();

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
}
// deleting pages on pop makes life much easier as opposed to deleting
// old pages on push which might screw things up
