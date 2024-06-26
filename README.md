# On Disk Ringbuffer

### Layer 0
What is RSV???

a, b, c - comma seperated values
0x61 0x2c 0x62 0x2c 0x63 - in unicode
     0xFF      0xFF

### Layer 1
What are pages (not unix pages - MY pages)
pages are data structures

is_ready long - not used
write_idx - where are we writing
another_write_idx - the last confirmed safe write
and_another_write_idx - not used
empty space - this is the empty space where we actually write stuff

0000 0000 - is_ready
0000 0000 - write_idx
0000 0000 - another write idx
0000 0000 - and anotheru

0000000000000000000000000000000000000000000000000000000000000000000000000000000000

0000 0000 + 3 = 0000 0012 - write_idx
00000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000abc000000000000000000000000000000000000000000000000000000000000000000000
0000 0000 + 3 = 0000 0003 - finished_write_idx

0000 0000 + 1 0003 = 0001 0012 - write_idx
abc0xffdef0xffghi0xff000000000000000000000000000000000000000000000000000000000000

0001 0003 - 1 0000 = 0000 0003 - write_idx

1111 0000
    &
0000 0010  0010 0100
-> max(safe_write_idx, 0000 0012) -> safe write idx

0000 0003

write_idx < read_idx + length => no new data / empty read


