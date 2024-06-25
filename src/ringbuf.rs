struct Ringbuf {
    write_page: usize,
}

impl Ringbuf {
    fn push() {}
    fn pop() {}
}

// deleting pages on pop makes life much easier as opposed to deleting
// old pages on push which might screw things up
