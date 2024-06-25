use crate::page::{Page, ReadResult};
use std::path::PathBuf;

pub struct Ringbuf {
    name: PathBuf,
    write_page_count: usize,
    write_page: Page,
    read_page_count: usize,
    read_page: Page,
    read_start_byte: usize,
}

impl Ringbuf {
    pub fn new<P: Into<PathBuf>>(path: P) -> Ringbuf {
        let name = path.into();
        let _ = std::fs::create_dir_all(&name);

        Ringbuf {
            name: name.clone(),
            write_page_count: 0,
            read_page_count: 0,
            read_start_byte: 0,
            // should open lowest number page in the directory rather than 0
            write_page: Page::new(&name.join("0.test.bin").to_string_lossy()),
            read_page: Page::new(&name.join("0.test.bin").to_string_lossy()),
        }
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) {
        loop {
            let _ = match self.write_page.try_push(&input) {
                Ok(0) => 0,
                Ok(_) => break,
                Err(e) => panic!("{:#?}", e),
            };

            self.write_page_count += 1;
            self.write_page = Page::new(
                &self
                    .name
                    .join(format!("{}.test.bin", self.write_page_count))
                    .to_string_lossy(),
            );
        }
    }

    pub fn pop(&mut self) -> Option<String> {
        const SOME_NUMBER: usize = 2;

        loop {
            match self.read_page.try_pop(self.read_start_byte) {
                Ok(None) => return None,
                Ok(Some(ReadResult::Continue)) => {}
                Ok(Some(ReadResult::Msg(m))) => {
                    self.read_start_byte += m.len() + 1;
                    return Some(m.into());
                }
                Err(e) => panic!("{e}"),
            };

            for i in self.read_page_count..self.write_page_count.saturating_sub(SOME_NUMBER) {
                let _ = std::fs::remove_file(&self.name.join(format!("{}.test.bin", i)));
            }

            self.read_page_count = std::cmp::max(
                self.read_page_count + 1,
                self.write_page_count.saturating_sub(SOME_NUMBER),
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

// gotta make ringbuf a c struct cri

#[test]
fn ringbuf_sequential_test() {
    let mut r = Ringbuf::new("test");

    for i in 0..50_000_000 {
        r.push(i.to_string());
    }

    for i in 0..10_000_000 {
        let m = r.pop().unwrap();
        assert_eq!(m, i.to_string());
    }

    for _ in 10_000_000..50_000_000 {
        let _ = r.pop().unwrap();
    }
}
// deleting pages on pop makes life much easier as opposed to deleting
// old pages on push which might screw things up
