use crate::page::{Page, ReadResult};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct Ringbuf {
    name: PathBuf,
    // write page count should be atomic and set to max of increment when incrementing?
    write_page_count: Arc<RwLock<usize>>,
    write_page_no: usize,
    write_page: Page,
    read_page_count: usize,
    read_page: Page,
    read_start_byte: usize,
    max_total_pages: usize,
}

impl Ringbuf {
    pub fn new<P: Into<PathBuf>>(path: P) -> Ringbuf {
        const MAX_TOTAL_PAGES: usize = 3;
        let name = path.into();
        let _ = std::fs::create_dir_all(&name);

        Ringbuf {
            name: name.clone(),
            write_page_count: Arc::new(RwLock::new(0)),
            write_page_no: 0,
            read_page_count: 0,
            read_start_byte: 0,
            max_total_pages: MAX_TOTAL_PAGES,
            // should open lowest number page in the directory rather than 0
            write_page: Page::new(&name.join("0.test.bin").to_string_lossy()),
            read_page: Page::new(&name.join("0.test.bin").to_string_lossy()),
        }
    }

    fn page_flip(&mut self) {
        let page_count = self
            .write_page_count
            .read()
            .expect("something went really bad with your lock");

        if *page_count == self.write_page_no {
            drop(page_count);

            let mut page_count = self
                .write_page_count
                .write()
                .expect("something went really bad with your lock");

            if *page_count < self.write_page_no {
                self.write_page_no += 1;
                return;
            }

            *page_count += 1;
            self.write_page_no += 1;

            if *page_count >= self.max_total_pages {
                std::fs::remove_file(
                    &self
                        .name
                        .join(format!("{}.test.bin", *page_count - self.max_total_pages)),
                )
                .expect("something went wrong deleting an old file");
            }
        }
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) {
        loop {
            let _ = match self.write_page.try_push(&input) {
                Ok(0) => 0, // PAGE FULL / Continue
                Ok(_) => break,
                Err(e) => panic!("{:#?}", e),
            };

            self.page_flip();

            self.write_page = Page::new(
                &self
                    .name
                    .join(format!("{}.test.bin", self.write_page_no))
                    .to_string_lossy(),
            );
        }
    }

    pub fn pop(&mut self) -> Option<String> {
        loop {
            match self.read_page.try_pop(self.read_start_byte) {
                Ok(None) => return None, // no new messages
                Ok(Some(ReadResult::Continue)) => {}
                Ok(Some(ReadResult::Msg(m))) => {
                    self.read_start_byte += m.len() + 1;
                    return Some(m.into());
                }
                Err(e) => panic!("{e}"),
            };

            let page_count = self
                .write_page_count
                .read()
                .expect("something went really wrong with your lock");

            self.read_page_count = std::cmp::max(
                self.read_page_count + 1,
                page_count.saturating_sub(self.max_total_pages),
            );

            self.read_start_byte = 0;
            self.read_page = Page::new(
                &self
                    .name
                    .join(format!("{}.test.bin", self.read_page_count))
                    .to_string_lossy(),
            );
        }
    }
}

#[test]
fn ringbuf_sequential_test() {
    let mut r = Ringbuf::new("test");

    let now = std::time::Instant::now();
    for i in 0..50_000_000 {
        r.push(i.to_string());
    }

    for i in 0..50_000_000 {
        let m = r.pop().unwrap();
        assert_eq!(m, i.to_string());
    }

    eprintln!("took {} ms", now.elapsed().as_millis());
}
// deleting pages on pop makes life much easier as opposed to deleting
// old pages on push which might screw things up
