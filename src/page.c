#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <threads.h>
#include <unistd.h>

// this adds the length of the message to the front of the message
// which allows the pop operation to not perform a linear search.
// the drawback is that the length itself can be up to 8 bytes on 64bit systems
// which might be longer than the message itself in some cases.
//
// #define CONSTANT_TIME_READ 1 - fix this

#define QUEUE_SIZE 4096 * 32000
#define PAGE_FULL -10
#define PAGE_EXISTS -11
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

#define READ_ERROR -12
#define READ_SUCCESS 0
#define READ_FINISHED 1
#define READ_EMPTY 2
#define WRITE_PAGE_FULL 0
typedef struct {
  size_t len;
  char *ptr;
  int read_status;
} CSlice;

typedef struct {
  atomic_size_t is_ready;
  atomic_size_t write_idx_lock;
  atomic_size_t last_safe_write_idx;
  atomic_size_t last_idx;
  unsigned char buf[QUEUE_SIZE];
} RawQPage;

RawQPage *raw_qpage_new(char *path) {
  RawQPage *p;
  int fd;

  fd = open(path, O_RDWR | O_CREAT, 0644);

  if (fd < 0) {
    perror("failed to open file");
    exit(EXIT_FAILURE);
  }

  ftruncate(fd, sizeof(RawQPage));

  p = mmap(0, sizeof(RawQPage), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

  if (p == MAP_FAILED) {
    close(fd);
    perror("failed to mmap file");
    exit(EXIT_FAILURE);
  }

  close(fd);
  return p;
}

// rust doesn't use null terminated strings because
// it's so much better not to... but unfortunately
// this becomes a pain in the ass when passing strings
// to c
RawQPage *raw_qpage_new_rs(char *path, size_t path_len) {
  char path_with_null_term[path_len + 1];

  memcpy(path_with_null_term, path, path_len);
  path_with_null_term[path_len] = '\0';

  return raw_qpage_new(path_with_null_term);
}

int raw_qpage_push_fast_read(RawQPage *p, char *buf, size_t len) {
  size_t start;

  start = atomic_fetch_add_explicit(&p->write_idx_lock,
                                    QUEUE_MAGIC_NUM + sizeof(size_t) + len + 1,
                                    memory_order_relaxed);

  start &= QUEUE_MAGIC_MASK;

  if (start + len >= QUEUE_SIZE) {
    // TODO: add PAGEFULL logic eventually
    atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                              memory_order_relaxed);
    return PAGE_FULL;
  }

  memcpy(&p->buf[start], &len, sizeof(size_t));
  memcpy(&p->buf[start + sizeof(size_t)], buf, len);
  p->buf[start + sizeof(size_t) + len] = VALUE_TERM_BYTE;

  atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                            memory_order_release);

  return len + sizeof(size_t) + 1;
}

int raw_qpage_push(RawQPage *p, char *buf, size_t len) {
#ifdef CONSTANT_TIME_READ
  return raw_qpage_push_fast_read(p, buf, len);
#else

  size_t start;

  start = atomic_fetch_add_explicit(
      &p->write_idx_lock, QUEUE_MAGIC_NUM + len + 1, memory_order_relaxed);

  start &= QUEUE_MAGIC_MASK;

  if (start + len >= QUEUE_SIZE - 1) {
    if (start < QUEUE_SIZE - 1) {
      p->buf[start] = 0xFD;
    }

    atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                              memory_order_relaxed);
    return WRITE_PAGE_FULL;
  }

  memcpy(&p->buf[start], buf, len);
  p->buf[start + len] = VALUE_TERM_BYTE;

  atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                            memory_order_release);

  return len + 1;

#endif
}

CSlice raw_qpage_pop_fast_read(RawQPage *p, size_t start_byte) {
  size_t end;
  CSlice cs;

  end = atomic_load_explicit(&p->last_safe_write_idx, memory_order_relaxed);

  if (end <= start_byte) {
    while (1) {
      end = atomic_load_explicit(&p->write_idx_lock, memory_order_acquire);

      if ((end & !QUEUE_MAGIC_MASK) == 0) {
        break;
      }

      sleep(0);
    }

    // TODO: maybe this should be atomic fetch min - C26 type stuff
    atomic_store_explicit(&p->last_safe_write_idx, end, memory_order_relaxed);
  }

  end = (QUEUE_SIZE < end) ? QUEUE_SIZE : end;

  cs.len = *(size_t *)&p->buf[start_byte];
  cs.ptr = (char *)&p->buf[start_byte + sizeof(size_t)];

  if (p->buf[start_byte + cs.len + sizeof(size_t)] != VALUE_TERM_BYTE) {
    cs.len = 0;
  }

  return cs;
}

size_t _get_write_idx_spin(RawQPage *p, size_t start_byte) {
  size_t end;

  end = atomic_load_explicit(&p->last_safe_write_idx, memory_order_relaxed);

  if (end <= start_byte) {

    while (1) {
      end = atomic_load_explicit(&p->write_idx_lock, memory_order_acquire);

      // check if there are any writers
      if ((end & !QUEUE_MAGIC_MASK) == 0) {
        break;
      }

      sleep(0);
    }

    // TODO: maybe this should be atomic fetch min - C26 type stuff
    atomic_store_explicit(&p->last_safe_write_idx, end, memory_order_relaxed);
  }

  end = (QUEUE_SIZE < end) ? QUEUE_SIZE : end;

  return end;
}

CSlice raw_qpage_pop(RawQPage *p, size_t start_byte) {
#ifdef CONSTANT_TIME_READ
  return raw_qpage_pop_fast_read(p, start_byte);
#else
  size_t i, end;
  CSlice cs;

  end = _get_write_idx_spin(p, start_byte);

  if (end == start_byte) {
    cs.len = 0;
    cs.ptr = 0;
    cs.read_status = READ_EMPTY;

    return cs;
  }

  if (p->buf[start_byte] == 0xFD) {
    cs.len = 0;
    cs.ptr = 0;
    cs.read_status = READ_FINISHED;

    return cs;
  }

  for (i = start_byte; i < end; i++) {
    if (p->buf[i] == VALUE_TERM_BYTE) {
      break;
    }
  }

  if (p->buf[i] != VALUE_TERM_BYTE) {
    cs.len = 0;
    cs.ptr = 0;
    cs.read_status = READ_ERROR;
  }

  cs.len = i - start_byte;
  cs.ptr = (char *)&p->buf[start_byte];
  cs.read_status = READ_SUCCESS;

  return cs;
#endif
}

void raw_qpage_drop(RawQPage *p) { munmap(p, sizeof(RawQPage)); }
