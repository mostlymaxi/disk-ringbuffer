# On-Disk Ringbuffer

This is an extremely simple implementation of an on-disk broadcast channel that
sort of pretends to be a ringbuffer! It uses memory-mapped pages to have interprocess,
lock-free, reads and writes. It's blazingly fast, but tends to hog disk-space for better
efficiency (fewer but bigger memory-mapped pages).


 Example
```rust
use disk_ringbuffer::ringbuf;

fn example() {
    // takes directory to use as ringbuf storage and the total number of pages to store as input.
    // note that each page takes 80Mb and setting the max_pages to zero implies an unbounded queue
    let (mut tx, mut rx) = ringbuf::new("test-example", 2).unwrap();

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



