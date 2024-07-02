#ifndef _PAGE_H
#define _PAGE_H

#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

// this adds the length of the message to the front of the message
// which allows the pop operation to not perform a linear search.
// the drawback is that the length itself can be up to 8 bytes on 64bit systems
// which might be longer than the message itself in some cases.
//
// #define CONSTANT_TIME_READ 1 - fix this

#define QUEUE_SIZE 4096 * 16000
#define READ_ERROR -1
#define VALUE_TERM_BYTE 0xFF

// i use a single atomic size_t to keep track of both:
// - how many writers are currently writing (in queue)
// - the next available index to write to
//
// this is annoying as hell because it means i have to use the fancy bit
// arithmetic to keep track of which bits are for what (most significant 8 are
// for keeping track of number of writers, everything else is index). but this
// means that readers are able to check the last available index and if any
// writers are writing in one atomic load.
//
// essentially, if a reader loads a zero for the number of writers and an index
// 100, it is GUARANTEED that everything up to index 100 has been written to by
// ALL writers even if there are multiple processes.
const size_t QUEUE_MAGIC_NUM = (size_t)0b1 << ((sizeof(size_t) * 8) - 8);

const size_t QUEUE_MAGIC_MASK = QUEUE_MAGIC_NUM - 1;

#define READ_SUCCESS 0
#define READ_FINISHED 1
#define READ_EMPTY 2
#define WRITE_PAGE_FULL 0

typedef struct {
  size_t len;
  unsigned char *ptr;
  int read_status;
} CSlice;

typedef struct {
  atomic_size_t is_ready;
  atomic_size_t write_idx_lock;
  atomic_size_t last_safe_write_idx;
  unsigned char buf[QUEUE_SIZE];
} RawQPage;

RawQPage *raw_qpage_new(char *path);

// rust doesn't use null terminated strings because
// it's so much better not to... but unfortunately
// this becomes a pain in the ass when passing strings
// to c
RawQPage *raw_qpage_new_rs(const unsigned char *path, size_t path_len);

int raw_qpage_push_fast_read(RawQPage *p, char *buf, size_t len);

int raw_qpage_push(RawQPage *p, const unsigned char *buf, size_t len);

CSlice raw_qpage_pop_fast_read(RawQPage *p, size_t start_byte);

size_t _get_write_idx_spin(RawQPage *p, size_t start_byte);

CSlice raw_qpage_pop(RawQPage *p, size_t start_byte);

void raw_qpage_drop(RawQPage *p);
#endif
