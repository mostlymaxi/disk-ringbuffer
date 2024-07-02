/*!

# On Disk Ringbuffer

This is an extremely simple implementation of an on disk write-only log that
sort of pretends to be a ringbuffer! It uses memory-mapped pages to have interprocess,
lock-free, reads and writes. It's blazingly fast, but tends to hog disk-space for better
efficiency (less but bigger memory-mapped pages).


## Example
```rust
use disk_ringbuffer::ringbuf;

fn example() {
    // takes directory to use as ringbuf storage as input
    let (mut tx, mut rx) = ringbuf::new("test-example").unwrap();

    // you can clone readers and writers to use in other threads!
    let tx2 = tx.clone();

    for i in 0..500_000 {
        tx.push(i.to_string());
    }

    for i in 0..500_000 {
        let m = rx.pop().unwrap().unwrap();
        assert_eq!(m, i.to_string());
    }
}
```

senders are also completely thread safe!
```rust
use disk_ringbuffer::ringbuf::new;

fn thread_example() {

    let (mut tx, mut rx) = new("test-thread-example").unwrap();
    let mut tx2 = tx.clone();

    let t = std::thread::spawn(move || {
        for i in 0..500_000 {
            tx.push(i.to_string()).unwrap();
        }
    });

    tx2.push("asdf").unwrap();

    t.join().unwrap();
}
```
*/

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub mod page;
pub mod ringbuf;
