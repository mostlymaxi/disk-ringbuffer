#include <fcntl.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <threads.h>
#include <unistd.h>

typedef struct {
  atomic_size_t current_page;

} Ringbuf;
