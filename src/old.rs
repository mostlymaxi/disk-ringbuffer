use core::{f64, hash};
use std::{
    cell::UnsafeCell,
    cmp::min,
    fs::{self, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
    str,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread::{self, yield_now},
    time::{Duration, Instant},
};

#[macro_use]
extern crate static_assertions;

/// so... how does this work?
/// essentially, this is just a write-only-log backed by a memory map.
///
/// Writing:
/// 1. atomically increment writer queue to block any new readers
/// 2. wait for reader queue to reach zero
/// 3. atomically fetch and add input length to write_idx
/// 4. write input length bytes at write_idx
/// 5. atomically decrement writer queue
///
///
/// Reading:
/// (just the opposite of writing)
/// 1. atomically increment reader queue to block any new writers
/// 2. wait for writer queue to reach zero
/// 3. atomically fetch write_idx (last byte in map)
/// 4. read mmap from start byte to write_idx or mmap length
/// 5. atomically decrement reader queue
///
///
/// Page Switch:
/// 1. atomically increment reader and writer queue to block new readers & writers
/// 2.
/// 4. atomically load is_dirty
/// 5. if not dirty
///
///
use memmap2::MmapMut;

const PAGE_SIZE: usize = 4096 * 16_000;
const RINGBUF_PAGE_NUM: usize = 0b1000;
const RB_MASK: usize = RINGBUF_PAGE_NUM - 1;
const _MAX_QUEUE_SIZE: usize = 64;
// 1 in the 8th MOST significant byte
const QUEUE_MAGIC_NUM: usize = 0b1 << (usize::BITS - 8);
const QUEUE_MASK: usize = QUEUE_MAGIC_NUM - 1;

// The official SyncUnsafeCell is locked behind the nightly compiler
// since all it really does is unsafely implement Send and Sync arround
// an unsafe cell, i figured i could do it my self and not use nightly.
// read why this is a terrible idea here: https://github.com/rust-lang/rust/issues/95439
struct MySyncUnsafeCell<T: ?Sized>(UnsafeCell<T>);

impl<T> MySyncUnsafeCell<T> {
    fn get(&self) -> *mut T {
        self.0.get()
    }
}

unsafe impl<T> Send for MySyncUnsafeCell<T> {}
unsafe impl<T> Sync for MySyncUnsafeCell<T> {}

struct Page {
    map: MySyncUnsafeCell<MmapMut>,
    write_idx: AtomicUsize,
    read_idx: AtomicUsize,
    writer_queue: AtomicUsize,
    reader_queue: AtomicUsize,
    // this isn't an atomic bool because
    // i don't trust OS to implement in efficiently
    // but i could be wrong
    is_dirty: AtomicUsize,
}

#[derive(Debug)]
enum InternalPageWarning {
    PageFull,
    DirtyPage,
    _NoDataRead,
    _ReaderQueueFull,
    _WriterQueueFull,
}

#[derive(Debug)]
enum ClearPageStatus {
    Clear,
    Busy,
}

impl Page {
    fn new<P: AsRef<Path>>(path: P) -> Page {
        let _ = fs::remove_file(&path);
        Page::open(path)
    }

    fn open<P: AsRef<Path>>(path: P) -> Page {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();

        f.set_len(PAGE_SIZE as u64).unwrap();

        let map = unsafe { MmapMut::map_mut(&f).unwrap() };
        let map = MySyncUnsafeCell(map.into());
        let write_idx = AtomicUsize::new(0);
        let read_idx = AtomicUsize::new(0);
        let writer_queue = AtomicUsize::new(0);
        let reader_queue = AtomicUsize::new(0);
        let is_dirty = AtomicUsize::new(0);

        Page {
            map,
            write_idx,
            read_idx,
            writer_queue,
            reader_queue,
            is_dirty,
        }
    }

    fn clear_page(&self) -> ClearPageStatus {
        let now = Instant::now();
        // set is_dirty to prevent any new readers or writers from joining
        let Ok(_) = self
            .is_dirty
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
        else {
            return ClearPageStatus::Busy;
        };

        // spin lock to ensure no readers or writers in page
        while self.reader_queue.load(Ordering::Acquire) != 0 {
            yield_now();
        }
        while self.writer_queue.load(Ordering::Acquire) != 0 {
            yield_now();
        }
        // currently using the pray algorithm for this ^
        // NVM PRAY ALGORITHM DID NOT WORK D:

        // unsafe {
        //     let super_unsafe_shared_mut = &mut *self.map.get();
        //     super_unsafe_shared_mut.fill(0);
        // };

        self.read_idx.store(0, Ordering::Release);
        self.write_idx.store(0, Ordering::Release);

        // eprintln!("page clear took {} us", now.elapsed().as_micros());
        ClearPageStatus::Clear
    }

    fn unsafe_write<T: AsRef<[u8]>>(&self, input: T) -> Result<usize, InternalPageWarning> {
        let input = input.as_ref();
        let len = input.len();
        let idx = self
            .write_idx
            .fetch_add(QUEUE_MAGIC_NUM + len + 1, Ordering::Relaxed);
        let idx = idx & QUEUE_MASK;

        if idx + len >= PAGE_SIZE {
            let Ok(_) = self
                .is_dirty
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            else {
                return Err(InternalPageWarning::DirtyPage);
            };
            self.write_idx
                .fetch_sub(QUEUE_MAGIC_NUM + idx + len + 1, Ordering::Relaxed);
            return Err(InternalPageWarning::PageFull);
        }

        // we need this to be unsafe because we want internal mutability without the Page
        // being borrowed as mutable. this means we can share the same page between threads
        // without the need for locks. why no locks? they are "slow" and i'm having fun :D
        //
        // the chunk that we are writing to is changed atomically.
        // if we want to write to 0..5, write_idx will fetch 0 and set itself to 5 atomically
        // so no other writer will be able to interact with that range. but please check
        // this 1000 times to be sure!!!
        unsafe {
            let super_unsafe_shared_mut = &mut *self.map.get();
            super_unsafe_shared_mut[idx..idx + len].copy_from_slice(input);
            super_unsafe_shared_mut[idx + len] = 0xFF;
        };

        let _ = self.write_idx.fetch_sub(QUEUE_MAGIC_NUM, Ordering::Relaxed);

        Ok(len)
    }

    fn push<T: AsRef<[u8]>>(&self, input: T) -> Result<usize, InternalPageWarning> {
        if self.is_dirty.load(Ordering::Relaxed) == 1 {
            return Err(InternalPageWarning::DirtyPage);
        }

        self.unsafe_write(input)
    }

    fn unsafe_read(&self, start_idx: usize, buf: &mut [u8]) -> usize {
        let mut end_idx;

        loop {
            end_idx = self.write_idx.load(Ordering::Acquire);
            if end_idx & !QUEUE_MASK == 0 {
                break;
            }
            yield_now();
        }

        let end_idx = min(start_idx + buf.len(), end_idx);
        let end_idx = min(PAGE_SIZE, end_idx);

        if end_idx == start_idx {
            return 0;
        }

        // this is unsafe because the compiler can't guarantee
        // that map isn't being written at the same time.
        // but don't worry because instead you can just trust me!
        unsafe {
            let unsafe_shared_immut = &*self.map.get();
            let end_idx = unsafe_shared_immut[start_idx..end_idx]
                .iter()
                .position(|&b| b == 0xFF)
                .unwrap_or(start_idx);

            buf[0..end_idx - start_idx].copy_from_slice(&unsafe_shared_immut[start_idx..end_idx]);
        }

        let bytes_to_consume = buf.iter().rposition(|&b| b == 0xFF).unwrap_or(0);

        bytes_to_consume
    }

    fn read(&self, start_idx: usize, buf: &mut [u8]) -> Result<usize, InternalPageWarning> {
        if self.is_dirty.load(Ordering::Relaxed) == 1 {
            return Err(InternalPageWarning::DirtyPage);
        }

        const MAGIC_NUMBER_WIP: usize = 8192;

        let _ = self.reader_queue.fetch_add(1, Ordering::Relaxed);

        let bytes_read = self.unsafe_read(start_idx, buf);

        let _ = self.reader_queue.fetch_sub(1, Ordering::Release);

        // there is probably a smarter way without reallocating
        // but i can't figure it out rn
        //let mut res: Vec<String> = res
        //    .split(|b| b == &0xFF)
        //    .map(|s| String::from_utf8_lossy(s).into_owned())
        //    .collect();

        // let _ = res.pop();

        Ok(bytes_read)
    }
}

struct RingBuf {
    page: Arc<Page>,
    pages: Vec<Page>,
    page_idx: AtomicUsize,
    reader_idx: AtomicUsize,
}

impl RingBuf {
    fn new<P: AsRef<Path>>(path: P) -> Self {
        // RINGBUF_PAGE_NUM must be a power of 2
        const_assert!(RINGBUF_PAGE_NUM & (RINGBUF_PAGE_NUM - 1) == 0);

        fs::create_dir_all(&path).unwrap();

        let mut pages = Vec::new();
        for i in 0..RINGBUF_PAGE_NUM {
            pages.push(Page::new(path.as_ref().join(format!("{i}.bin"))));
        }
        let page = Arc::new(Page::new(path.as_ref().join(format!("big-test.bin"))));

        let page_idx = AtomicUsize::new(0);
        let reader_idx = AtomicUsize::new(0);

        RingBuf {
            page,
            pages,
            page_idx,
            reader_idx,
        }
    }

    fn push<T: AsRef<[u8]>>(&self, input: T) {
        // originally did this recursively but kept running into stack overflow
        // hahahahahahaha
        loop {
            let page_idx = self.page_idx.load(Ordering::Relaxed);

            match self.pages[page_idx & RB_MASK].push(&input) {
                Ok(_) => break,
                Err(InternalPageWarning::PageFull) => {
                    self.page = Page::new("test/big-test.bin");
                    self.page_idx.store(page_idx + 1, Ordering::Relaxed);
                    self.pages[(page_idx + 1) & RB_MASK]
                        .is_dirty
                        .store(0, Ordering::Relaxed);

                    continue;
                }
                Err(InternalPageWarning::DirtyPage) => {
                    yield_now();
                    continue;
                }
                Err(_) => {
                    eprintln!("something went wrong!");
                    break;
                }
            }
        }
    }

    // fn pop(&self) -> String {}
}

fn test_ringbuf_push() {
    let rb = RingBuf::new("test");
    rb.push("ahhhhhh");
}

fn test_ringbuf_push_2() {
    let total_writes = 100_000_000;
    let rb = RingBuf::new("test");
    let mut test = Vec::new();
    for i in 0..total_writes {
        test.push(i.to_string());
    }

    let now = Instant::now();
    for x in test {
        rb.push(&x);
    }

    let writes_per_ms = total_writes as f64 / now.elapsed().as_secs_f64();
    eprintln!(
        "{} writes per s ({}/ms)",
        writes_per_ms,
        writes_per_ms / 1000 as f64
    );
}

fn test_ringbuf_push_3() {
    let total_threads = 4;
    let total_writes = 5_000_000 / total_threads;

    let rb = Arc::new(RingBuf::new("test"));

    let mut handles = Vec::new();
    let mut test = Vec::new();

    let barrier = Arc::new(Barrier::new(total_threads));

    let mut total_bytes = 0;
    let mut h = DefaultHasher::new();

    for i in 0..total_writes {
        // i.to_string().hash(&mut h);
        // let i = h.finish();
        // let i = format!("ksdjflksjflskjfklajdlakjalskdjfaskldjfaslkdjf {i}");

        let i = i.to_string();
        total_bytes += i.as_bytes().len();
        test.push(i);
    }

    for t in 1..=total_threads {
        let rb_clone = rb.clone();
        let test_clone = test.clone();
        let b = barrier.clone();

        handles.push(thread::spawn(move || {
            b.wait();
            eprintln!("ping!");
            let now = Instant::now();

            for x in test_clone {
                rb_clone.push(&x);
            }

            let writes_per_s = total_writes as f64 / now.elapsed().as_secs_f64();
            let bytes_per_s = total_bytes as f64 / now.elapsed().as_secs_f64();
            eprintln!(
                "thread {t}: {} writes per s ({}/ms) took {} s",
                writes_per_s,
                writes_per_s / 1000 as f64,
                now.elapsed().as_secs_f64()
            );
            eprintln!("thread {t}: {} mb/s", bytes_per_s / 1_000_000 as f64)
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

fn test_clear_page() {
    let test_page = Arc::new(Page::new("test_clear_page.bin"));
    let _ = test_page.push("abc");
    let mut buf = vec![0; 8192];
    let _a = test_page.read(0, &mut buf).unwrap();
    //eprintln!("{:#?}", a);
    test_page.clear_page();
}

fn main() {
    //test_clear_page();
    //test_ringbuf_push();
    //test_ringbuf_push_2();
    test_ringbuf_push_3();
}
