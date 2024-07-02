#include "page.h"

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
RawQPage *raw_qpage_new_rs(const unsigned char *path, size_t path_len) {
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
                              memory_order_release);
    return WRITE_PAGE_FULL;
  }

  memcpy(&p->buf[start], &len, sizeof(size_t));
  memcpy(&p->buf[start + sizeof(size_t)], buf, len);
  p->buf[start + sizeof(size_t) + len] = VALUE_TERM_BYTE;

  atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                            memory_order_release);

  return len + sizeof(size_t) + 1;
}

int raw_qpage_push(RawQPage *p, const unsigned char *buf, size_t len) {
#ifdef CONSTANT_TIME_READ
  return raw_qpage_push_fast_read(p, buf, len);
#else

  size_t start;

  start = atomic_fetch_add_explicit(
      &p->write_idx_lock, QUEUE_MAGIC_NUM + len + 1, memory_order_relaxed);

  start &= QUEUE_MAGIC_MASK;

  if (start + len >= QUEUE_SIZE - 1) {
    if (start <= QUEUE_SIZE - 1) {
      p->buf[start] = 0xFD;
    }

    atomic_fetch_sub_explicit(&p->write_idx_lock, QUEUE_MAGIC_NUM,
                              memory_order_release);
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
    }

    // TODO: maybe this should be atomic fetch min - C26 type stuff
    atomic_store_explicit(&p->last_safe_write_idx, end, memory_order_relaxed);
  }

  end = (QUEUE_SIZE < end) ? QUEUE_SIZE : end;

  cs.len = *(size_t *)&p->buf[start_byte];
  cs.ptr = &p->buf[start_byte + sizeof(size_t)];

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
      if ((end & ~QUEUE_MAGIC_MASK) == 0) {
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

  if (end <= start_byte) {
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

    return cs;
  }

  cs.len = i - start_byte;
  cs.ptr = &p->buf[start_byte];
  cs.read_status = READ_SUCCESS;

  return cs;
#endif
}

void raw_qpage_drop(RawQPage *p) { munmap(p, sizeof(RawQPage)); }
