#include <errno.h>
#include <linux/fs.h>
#include <linux/io_uring.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

// Dynamic arrays implementation
#define da_append(xs, x)                                                       \
  do {                                                                         \
    if (xs.count >= xs.capacity) {                                             \
      if (xs.capacity == 0)                                                    \
        xs.capacity = 256;                                                     \
      xs.capacity = xs.capacity * 2;                                           \
      xs.items = realloc(xs.items, xs.capacity * sizeof(*xs.items));           \
    }                                                                          \
    xs.items[xs.count++] = x;                                                  \
  } while (0)

#define da_capacity(xs, x)                                                     \
  do {                                                                         \
    xs.capacity = x;                                                           \
    xs.items = realloc(xs.items, xs.capacity * sizeof(*xs.items));             \
  } while (0)

#define da_replace(xs, i, x)                                                   \
  do {                                                                         \
    if (i >= xs.capacity) {                                                    \
      xs.capacity = (i + 1) * 2;                                               \
      xs.items = realloc(xs.items, xs.capacity * sizeof(*xs.items));           \
    }                                                                          \
    if (i >= xs.count) {                                                       \
      xs.count = i + 1;                                                        \
    }                                                                          \
    xs.items[i] = x;                                                           \
  } while (0)

#define da_free(xs)                                                            \
  do {                                                                         \
    free(xs.items);                                                            \
    xs.items = NULL;                                                           \
    xs.count = 0;                                                              \
  } while (0)

#define da_empty(xs)                                                           \
  do {                                                                         \
    da_free(xs);                                                               \
    xs.items = malloc(xs.capacity * sizeof(*xs.items));                        \
  } while (0)

#define da_swap(xs, i, j)                                                      \
  do {                                                                         \
    typeof(xs.items[i]) tmp = xs.items[i];                                     \
    xs.items[i] = xs.items[j];                                                 \
    xs.items[j] = tmp;                                                         \
  } while (0)

#define da_remove_swap(xs, i)                                                  \
  do {                                                                         \
    assert(xs.count > 0);                                                      \
    assert(i < xs.count);                                                      \
    da_swap(xs, i, xs.count - 1);                                              \
    xs.count -= 1;                                                             \
  } while (0)

#define da_remove_shuffle(xs, i)                                               \
  do {                                                                         \
    assert(xs.count > 0);                                                      \
    assert(i < xs.count);                                                      \
    for (size_t i = 0; i < xs.count - 1; i++) {                                \
      xs.items[i] = xs.items[i + 1];                                           \
    }                                                                          \
    xs.count -= 1;                                                             \
  } while (0)

#define da_clone(xs)                                                           \
  ({                                                                           \
    typeof(xs) ys = {0};                                                       \
    ys.capacity = xs.capacity;                                                 \
    ys.count = xs.count;                                                       \
    ys.items = malloc(ys.capacity * sizeof(*ys.items));                        \
    memcpy(ys.items, xs.items, xs.count * sizeof(*xs.items));                  \
    ys;                                                                        \
  })

#define da_quicksort(xs, cmp)                                                  \
  do {                                                                         \
    if (xs.count <= 1)                                                         \
      break;                                                                   \
    int stack[128];                                                            \
    int sp = 0;                                                                \
    stack[sp++] = 0;                                                           \
    stack[sp++] = xs.count - 1;                                                \
    while (sp > 0) {                                                           \
      int end = stack[--sp];                                                   \
      int start = stack[--sp];                                                 \
      if (start >= end)                                                        \
        continue;                                                              \
      int pivot = start + (end - start) / 2;                                   \
      int left = start;                                                        \
      int right = end;                                                         \
      while (left <= right) {                                                  \
        while (cmp(xs.items[left], xs.items[pivot]) < 0)                       \
          left++;                                                              \
        while (cmp(xs.items[right], xs.items[pivot]) > 0)                      \
          right--;                                                             \
        if (left <= right) {                                                   \
          da_swap(xs, left, right);                                            \
          left++;                                                              \
          right--;                                                             \
        }                                                                      \
      }                                                                        \
      if (start < right) {                                                     \
        stack[sp++] = start;                                                   \
        stack[sp++] = right;                                                   \
      }                                                                        \
      if (left < end) {                                                        \
        stack[sp++] = left;                                                    \
        stack[sp++] = end;                                                     \
      }                                                                        \
    }                                                                          \
  } while (0)

// io_uring related
#define QUEUE_DEPTH 1
#define BLOCK_SZ 1024

#define io_uring_smp_store_release(p, v)                                       \
  atomic_store_explicit((_Atomic typeof(*(p)) *)(p), (v), memory_order_release)

#define io_uring_smp_load_acquire(p)                                           \
  atomic_load_explicit((_Atomic typeof(*(p)) *)(p), memory_order_acquire)

int ring_fd;
unsigned *sring_tail, *sring_mask, *sring_array, *cring_head, *cring_tail,
    *cring_mask;
struct io_uring_sqe *sqes;
struct io_uring_cqe *cqes;
char buff[BLOCK_SZ];
off_t offset;

int io_uring_setup(unsigned entries, struct io_uring_params *p) {
  int ret;
  ret = syscall(__NR_io_uring_setup, entries, p);
  return (ret < 0) ? -errno : ret;
}

int io_uring_enter(int ring_fd, unsigned int to_submit,
                   unsigned int min_complete, unsigned int flags) {
  int ret;
  ret = syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags,
                NULL, 0);
  return (ret < 0) ? -errno : ret;
}

int app_setup_uring(void) {
  struct io_uring_params p = {0};
  void *sq_ptr, *cq_ptr;

  ring_fd = io_uring_setup(QUEUE_DEPTH, &p);
  if (ring_fd < 0) {
    perror("io_uring_setup");
    return 1;
  }

  int sring_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
  int cring_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);

  if (p.features & IORING_FEAT_SINGLE_MMAP) {
    if (cring_sz > sring_sz) {
      sring_sz = cring_sz;
    }
    cring_sz = sring_sz;
  }

  sq_ptr = mmap(0, sring_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
                ring_fd, IORING_OFF_SQ_RING);
  if (sq_ptr == MAP_FAILED) {
    perror("mmap");
    return 1;
  }

  if (p.features & IORING_FEAT_SINGLE_MMAP) {
    cq_ptr = sq_ptr;
  } else {
    cq_ptr = mmap(0, cring_sz, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, ring_fd, IORING_OFF_CQ_RING);
    if (cq_ptr == MAP_FAILED) {
      perror("mmap");
      return 1;
    }
  }

  sring_tail = sq_ptr + p.sq_off.tail;
  sring_mask = sq_ptr + p.sq_off.ring_mask;
  sring_array = sq_ptr + p.sq_off.array;

  sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe),
              PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,
              IORING_OFF_SQES);
  if (sqes == MAP_FAILED) {
    perror("mmap");
    return 1;
  }

  cring_head = cq_ptr + p.cq_off.head;
  cring_tail = cq_ptr + p.cq_off.tail;
  cring_mask = cq_ptr + p.cq_off.ring_mask;
  cqes = cq_ptr + p.cq_off.cqes;

  return 0;
}

int read_from_cq() {
  struct io_uring_cqe *cqe;
  unsigned head;

  head = io_uring_smp_load_acquire(cring_head);
  if (head == *cring_tail) {
    return -1;
  }

  cqe = &cqes[head & (*cring_mask)];
  if (cqe->res < 0) {
    fprintf(stderr, "Error: %s\n", strerror(abs(cqe->res)));
  }

  head++;

  io_uring_smp_store_release(cring_head, head);

  return cqe->res;
}

int submit_accept_to_sq(int fd, struct sockaddr *addr, socklen_t *addrlen,
                        int flags) {
  unsigned index, tail;

  tail = *sring_tail;
  index = tail & *sring_mask;
  struct io_uring_sqe *sqe = &sqes[index];

  sqe->opcode = IORING_OP_ACCEPT;
  sqe->fd = fd;
  sqe->off = (unsigned long)addrlen;
  sqe->addr = (unsigned long)addr;
  sqe->len = 0;
  sqe->accept_flags = flags;

  sring_array[index] = index;
  tail++;

  io_uring_smp_store_release(sring_tail, tail);

  int ret = io_uring_enter(ring_fd, 1, 1, IORING_ENTER_GETEVENTS);

  if (ret < 0) {
    perror("io_uring_enter");
    return -1;
  }

  return ret;
}

int submit_to_sq(int fd, int op) {
  unsigned index, tail;

  tail = *sring_tail;
  index = tail & *sring_mask;
  struct io_uring_sqe *sqe = &sqes[index];

  sqe->opcode = op;
  sqe->fd = fd;
  sqe->addr = (unsigned long)buff;
  if (op == IORING_OP_READ) {
    memset(buff, 0, sizeof(buff));
    sqe->len = BLOCK_SZ;
  } else {
    sqe->len = strlen(buff);
  }
  sqe->off = offset;

  sring_array[index] = index;
  tail++;

  io_uring_smp_store_release(sring_tail, tail);

  int ret = io_uring_enter(ring_fd, 1, 1, IORING_ENTER_GETEVENTS);

  if (ret < 0) {
    perror("io_uring_enter");
    return -1;
  }

  return ret;
}

#define PORT 8080
#define MAX_REQUEST_SIZE 1024 * 1024 * 100
#define MAX_RESPONSE_SIZE 1024 * 1024 * 100

typedef struct {
  const char *buf;
  size_t len;
} String;

typedef struct {
  const char *buf;
  size_t len;
  size_t offset;
} Reader;

typedef struct {
  char *buf;
  size_t offset;
  size_t limit;
} Writer;

typedef enum {
  GET,
  POST,
  PUT,
  DELETE,
  OPTION,
} HttpMethod;

typedef enum {
  HTTP_1_1,
} HttpVersion;

typedef struct {
  String key;
  String value;
} Header;

typedef struct {
  Header *items;
  size_t count;
  size_t capacity;
} Headers;

typedef struct {
  String name;
  String filename;
} FormData;

typedef struct {
  Headers headers;
  Reader body;
  FormData formData;
} BodyPart;

typedef struct {
  BodyPart *items;
  size_t count;
  size_t capacity;
} BodyParts;

typedef struct {
  HttpMethod method;
  String resource;
  HttpVersion version;
  Headers headers;
  Reader body;
  BodyParts multipart;
} Request;

typedef enum {
  Continue = 100,
  SwitchingProtocols = 101,
  Processing = 102,
  EarlyHints = 103,
  Ok = 200,
  Created = 201,
  Accepted = 202,
  NonAuthoritativeInformation = 203,
  NoContent = 204,
  ResetContent = 205,
  PartialContent = 206,
  MultiStatus = 207,
  AlreadyReported = 208,
  IMUsed = 226,
  MultipleChoices = 300,
  MovedPermanently = 301,
  Found = 302,
  SeeOther = 303,
  NotModified = 304,
  TemporaryRedirect = 307,
  PermanentRedirect = 308,
  BadRequest = 400,
  Unauthorized = 401,
  PaymentRequired = 402,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  NotAcceptable = 406,
  ProxyAuthenticationRequired = 407,
  RequestTimeout = 408,
  Conflict = 409,
  Gone = 410,
  LengthRequired = 411,
  PreconditionFailed = 412,
  ContentTooLarge = 413,
  URITooLong = 414,
  UnsupportedMediaType = 415,
  RangeNotSatisfiable = 416,
  ExpectationFailed = 417,
  ImATeapot = 418,
  MisdirectedRequest = 421,
  UnprocessableRequest = 422,
  Locked = 423,
  FailedDependency = 424,
  TooEarly = 425,
  UpgradeRequired = 426,
  PreconditionRequired = 428,
  TooManyRequests = 429,
  RequestHeaderFieldsTooLarge = 431,
  UnavailableForLegalReasons = 451,
  InternalServerError = 500,
  NotImplemented = 501,
  BadGateway = 502,
  ServiceUnavailable = 503,
  GatewayTimeout = 504,
  HTTPVersionNotSupported = 505,
  VariantAlsoNegotiates = 506,
  InsufficientStorage = 507,
  LoopDetected = 508,
  NotExtended = 510,
  NetworkAuthenticationRequired = 511,
} StatusCode;

typedef enum {
  PARSE_OK = 0,
  PARSE_MISSING_METHOD = 1,
  PARSE_MISSING_RESOURCE = 2,
  PARSE_MISSING_VERSION = 3,
  PARSE_MISSING_HEADER_KEY = 4,
  PARSE_MISSING_HEADER_VALUE = 5,
  PARSE_ILLEGAL_METHOD = 101,
  PARSE_ILLEGAL_RESOURCE = 102,
  PARSE_ILLEGAL_VERSION = 103,
  PARSE_ILLEGAL_HEADER_KEY = 104,
  PARSE_ILLEGAL_HEADER_VALUE = 105,
  PARSE_TEMP_BUFFER_OVERFLOW = 200,
} ParseResult;

typedef enum {
  WRITE_OK = 0,
  WRITE_OUT_OF_MEMORY = 1,
} WriteResult;

const char SP = ' ';
const char CR = '\r';
const char LF = '\n';
const char COLON = ':';
const char *CRLF = "\r\n";
const char *HEADER_DELIMETER = ": ";

String string_empty() { return (String){0}; }

String string_from_cstring(const char *string) {
  String str;
  str.buf = string;
  str.len = strlen(string);
  return str;
}

String string_new(const char *buffer, size_t len) {
  String string;
  string.buf = buffer;
  string.len = len;
  return string;
}

Writer writer_new(char *buffer, size_t limit) {
  Writer writer;
  writer.buf = buffer;
  writer.limit = limit;
  writer.offset = 0;
  return writer;
}

Reader reader_new(const char *buf, size_t buflen) {
  Reader reader;
  reader.buf = buf;
  reader.len = buflen;
  reader.offset = 0;
  return reader;
}

Reader reader_from_cstring(char *string) {
  return reader_new(string, strlen(string));
}

Reader reader_from_string(String string) {
  return reader_new(string.buf, string.len);
}

String reader_to_string(Reader reader) {
  return string_new(&reader.buf[reader.offset], reader.len - reader.offset);
}

String writer_to_string(Writer writer) {
  return string_new(writer.buf, writer.offset);
}

bool string_equals(String a, String b) {
  if (a.len != b.len) {
    return false;
  }
  for (int i = 0; i < a.len; i++) {
    if (a.buf[i] != b.buf[i]) {
      return false;
    }
  }
  return true;
}

bool string_begins_with(String string, String prefix) {
  if (string.len < prefix.len) {
    return false;
  }
  for (int i = 0; i < prefix.len; i++) {
    if (string.buf[i] != prefix.buf[i]) {
      return false;
    }
  }
  return true;
}

Reader read_until_delimeter(Reader *reader, String delimeter) {
  if (reader->offset >= reader->len) {
    return (Reader){0};
  }
  size_t readlen = reader->len - reader->offset;

  Reader newreader = reader_new(&reader->buf[reader->offset], 0);

  for (newreader.len = 0; newreader.len < readlen; newreader.len++) {
    bool match = true;
    for (int i = 0; i < delimeter.len; i++) {
      if (newreader.buf[newreader.len + i] != delimeter.buf[i]) {
        match = false;
        break;
      }
    }
    if (match) {
      reader->offset += delimeter.len;
      break;
    }
  }

  reader->offset += newreader.len;

  return newreader;
}

Reader read_until_end(Reader *buffer) {
  if (buffer->offset >= buffer->len) {
    return (Reader){0};
  }

  size_t readlen = buffer->len - buffer->offset;

  Reader out = reader_new(&buffer->buf[buffer->offset], readlen);

  buffer->offset = buffer->len;

  return out;
}

WriteResult write_byte(Writer *writer, const char byte) {
  if (writer->offset >= writer->limit) {
    return WRITE_OUT_OF_MEMORY;
  }
  writer->buf[writer->offset] = byte;
  writer->offset++;
  return WRITE_OK;
}

WriteResult write_bytes(Writer *writer, const char *src, size_t len) {
  if (writer->offset + len > writer->limit) {
    return WRITE_OUT_OF_MEMORY;
  }
  memcpy(&writer->buf[writer->offset], src, len);
  writer->offset += len;
  return WRITE_OK;
}

WriteResult write_string(Writer *writer, String string) {
  return write_bytes(writer, string.buf, string.len);
}

#define WRITE_FMT(writer, fmt, ...)                                            \
  ({                                                                           \
    int written =                                                              \
        snprintf(&(writer)->buf[(writer)->offset],                             \
                 (writer)->limit - (writer)->offset, fmt, __VA_ARGS__);        \
    WriteResult result;                                                        \
    if (written >= (writer)->limit - (writer)->offset) {                       \
      result = WRITE_OUT_OF_MEMORY;                                            \
    } else {                                                                   \
      (writer)->offset += written;                                             \
      result = WRITE_OK;                                                       \
    }                                                                          \
    result;                                                                    \
  })

#define WRITE_OR_ERR(result)                                                   \
  do {                                                                         \
    WriteResult r = (result);                                                  \
    if (r != WRITE_OK) {                                                       \
      return r;                                                                \
    }                                                                          \
  } while (0);

WriteResult write_join(String *parts, size_t parts_len, String delimeter,
                       Writer *writer) {
  for (int i = 0; i < parts_len; i++) {
    String part = parts[i];
    if (i > 0) {
      WRITE_OR_ERR(write_string(writer, delimeter));
    }
    WRITE_OR_ERR(write_string(writer, part));
  }

  return WRITE_OK;
}

WriteResult write_multipart_boundary(Reader boundary, char *buf, size_t buflen,
                                     String *out) {
  Writer writer = writer_new(buf, buflen);
  String join_parts[2] = {string_from_cstring("--"),
                          reader_to_string(boundary)};
  WRITE_OR_ERR(write_join(join_parts, 2, string_empty(), &writer));
  *out = writer_to_string(writer);
  return WRITE_OK;
}

bool string_is_empty(String string) {
  return string.len == 0 || string.buf == NULL;
}

bool reader_is_empty(Reader reader) {
  return reader.len == 0 || reader.buf == NULL;
}

bool reader_is_exhausted(Reader reader) { return reader.offset >= reader.len; }

String http_status_to_string(StatusCode status_code) {
  char *string;
  switch (status_code) {
  case Continue:
    string = "Continue";
    break;
  case SwitchingProtocols:
    string = "Switching Protocols";
    break;
  case Processing:
    string = "Processing";
    break;
  case EarlyHints:
    string = "Early Hints";
    break;
  case Ok:
    string = "Ok";
    break;
  case Created:
    string = "Created";
    break;
  case Accepted:
    string = "Accepted";
    break;
  case NonAuthoritativeInformation:
    string = "Non-Authoritative Information";
    break;
  case NoContent:
    string = "No Content";
    break;
  case ResetContent:
    string = "Reset Content";
    break;
  case PartialContent:
    string = "Partial Content";
    break;
  case MultiStatus:
    string = "Multi-Status";
    break;
  case AlreadyReported:
    string = "Already Reported";
    break;
  case IMUsed:
    string = "IM Used";
    break;
  case MultipleChoices:
    string = "Multiple Choices";
    break;
  case MovedPermanently:
    string = "Moved Permanently";
    break;
  case Found:
    string = "Found";
    break;
  case SeeOther:
    string = "See Other";
    break;
  case NotModified:
    string = "Not Modified";
    break;
  case TemporaryRedirect:
    string = "Temporary Redirect";
    break;
  case PermanentRedirect:
    string = "Permanent Redirect";
    break;
  case BadRequest:
    string = "Bad Request";
    break;
  case Unauthorized:
    string = "Unauthorized";
    break;
  case PaymentRequired:
    string = "Payment Required";
    break;
  case Forbidden:
    string = "Forbidden";
    break;
  case NotFound:
    string = "Not Found";
    break;
  case MethodNotAllowed:
    string = "Method Not Allowed";
    break;
  case NotAcceptable:
    string = "Not Acceptable";
    break;
  case ProxyAuthenticationRequired:
    string = "Proxy Authentication Required";
    break;
  case RequestTimeout:
    string = "Request Timeout";
    break;
  case Conflict:
    string = "Conflict";
    break;
  case Gone:
    string = "Gone";
    break;
  case LengthRequired:
    string = "Length Required";
    break;
  case PreconditionFailed:
    string = "Precondition Failed";
    break;
  case ContentTooLarge:
    string = "Content Too Large";
    break;
  case URITooLong:
    string = "URI Too Long";
    break;
  case UnsupportedMediaType:
    string = "Unsupported Media Type";
    break;
  case RangeNotSatisfiable:
    string = "Range Not Satisfiable";
    break;
  case ExpectationFailed:
    string = "Expectation Failed";
    break;
  case ImATeapot:
    string = "I'm a teapot";
    break;
  case MisdirectedRequest:
    string = "Misdirected Request";
    break;
  case UnprocessableRequest:
    string = "Unprocessable Request";
    break;
  case Locked:
    string = "Locked";
    break;
  case FailedDependency:
    string = "Failed Dependency";
    break;
  case TooEarly:
    string = "Too Early";
    break;
  case UpgradeRequired:
    string = "Upgrade Required";
    break;
  case PreconditionRequired:
    string = "Precondition Required";
    break;
  case TooManyRequests:
    string = "Too Many Requests";
    break;
  case RequestHeaderFieldsTooLarge:
    string = "Request Header Fields Too Large";
    break;
  case UnavailableForLegalReasons:
    string = "Unavailable For Legal Reasons";
    break;
  case InternalServerError:
    string = "Internal Server Error";
    break;
  case NotImplemented:
    string = "Not Implemented";
    break;
  case BadGateway:
    string = "BadGateway";
    break;
  case ServiceUnavailable:
    string = "Service Unavailable";
    break;
  case GatewayTimeout:
    string = "Gateway Timeout";
    break;
  case HTTPVersionNotSupported:
    string = "HTTP Version Not Supported";
    break;
  case VariantAlsoNegotiates:
    string = "Variant Also Negotiates";
    break;
  case InsufficientStorage:
    string = "Insufficient Storage";
    break;
  case LoopDetected:
    string = "Loop Detected";
    break;
  case NotExtended:
    string = "Not Extended";
    break;
  case NetworkAuthenticationRequired:
    string = "Network Authentication Required";
    break;
  default:
    string = "";
  }

  return string_from_cstring(string);
}

String version_to_string(HttpVersion version) {
  char *string;
  switch (version) {
  case HTTP_1_1:
    string = "HTTP/1.1";
    break;
  default:
    string = "";
  }
  return string_from_cstring(string);
}

String method_to_string(HttpMethod method) {
  const char *string;
  switch (method) {
  case GET:
    string = "GET";
    break;
  case POST:
    string = "POST";
    break;
  case PUT:
    string = "PUT";
    break;
  case DELETE:
    string = "DELETE";
    break;
  case OPTION:
    string = "OPTION";
    break;
  default:
    string = "";
  }

  return string_from_cstring(string);
}

ParseResult parse_method(Reader *request_line, Request *request) {
  Reader method = read_until_delimeter(request_line, string_from_cstring(" "));
  if (reader_is_empty(method)) {
    return PARSE_MISSING_METHOD;
  }

  if (method.len == 3 && strncmp(method.buf, "GET", 3) == 0) {
    request->method = GET;
  } else if (method.len == 4 && strncmp(method.buf, "POST", 4) == 0) {
    request->method = POST;
  } else if (method.len == 3 && strncmp(method.buf, "PUT", 3) == 0) {
    request->method = PUT;
  } else if (method.len == 6 && strncmp(method.buf, "DELETE", 6) == 0) {
    request->method = DELETE;
  } else if (method.len == 6 && strncmp(method.buf, "OPTION", 6) == 0) {
    request->method = OPTION;
  } else {
    return PARSE_ILLEGAL_METHOD;
  }

  return PARSE_OK;
}

ParseResult parse_resource(Reader *request_line, Request *request) {
  Reader resource =
      read_until_delimeter(request_line, string_from_cstring(" "));
  if (reader_is_empty(resource)) {
    return PARSE_MISSING_RESOURCE;
  }

  request->resource = reader_to_string(resource);

  return PARSE_OK;
}

ParseResult parse_version(Reader *request_line, Request *request) {
  Reader version = read_until_end(request_line);
  if (reader_is_empty(version)) {
    return PARSE_MISSING_VERSION;
  }
  if (strncmp(version.buf, "HTTP/1.1", version.len) == 0) {
    request->version = HTTP_1_1;
  } else {
    return PARSE_ILLEGAL_VERSION;
  }

  return PARSE_OK;
}

ParseResult parse_request_line(Reader *request_line, Request *request) {
  ParseResult result;

  result = parse_method(request_line, request);
  if (result != PARSE_OK) {
    return result;
  }

  result = parse_resource(request_line, request);
  if (result != PARSE_OK) {
    return result;
  }

  result = parse_version(request_line, request);
  if (result != PARSE_OK) {
    return result;
  }

  return PARSE_OK;
}

ParseResult parse_header(Reader *header_line, Header *header) {
  Reader key = read_until_delimeter(header_line, string_from_cstring(": "));
  if (reader_is_empty(key)) {
    return PARSE_MISSING_HEADER_KEY;
  }

  Reader value = read_until_end(header_line);
  if (reader_is_empty(value)) {
    return PARSE_MISSING_HEADER_VALUE;
  }

  header->key = reader_to_string(key);
  header->value = reader_to_string(value);
  return PARSE_OK;
}

ParseResult parse_headers(Reader *request_reader, Headers *headers) {
  Reader line;

  Headers h = *headers;

  da_capacity(h, 10);

  while (
      (line = read_until_delimeter(request_reader, string_from_cstring(CRLF)))
          .len != 0) {
    Header header;
    ParseResult header_parse_result = parse_header(&line, &header);
    if (header_parse_result != PARSE_OK) {
      return header_parse_result;
    }

    da_append(h, header);
  }

  *headers = h;

  return PARSE_OK;
}

void parse_body_part_form_data(BodyPart *body_part, Reader header_reader) {
  if (string_begins_with(reader_to_string(header_reader),
                         string_from_cstring("name=\""))) {
    read_until_delimeter(&header_reader, string_from_cstring("name=\""));
    Reader name_reader =
        read_until_delimeter(&header_reader, string_from_cstring("\""));

    body_part->formData.name = reader_to_string(name_reader);

    read_until_delimeter(&header_reader, string_from_cstring("; "));

    parse_body_part_form_data(body_part, header_reader);
    return;
  }

  if (string_begins_with(reader_to_string(header_reader),
                         string_from_cstring("filename=\""))) {
    read_until_delimeter(&header_reader, string_from_cstring("filename=\""));
    Reader filename_reader =
        read_until_delimeter(&header_reader, string_from_cstring("\""));
    body_part->formData.filename = reader_to_string(filename_reader);
    read_until_delimeter(&header_reader, string_from_cstring("; "));

    parse_body_part_form_data(body_part, header_reader);
    return;
  }
}

void parse_body_part_headers(BodyPart *body_part) {
  Reader header_reader = {0};
  for (int j = 0; j < body_part->headers.count; j++) {
    Header header = body_part->headers.items[j];
    if (!string_equals(string_from_cstring("Content-Disposition"),
                       header.key)) {
      continue;
    }
    header_reader = reader_from_string(header.value);
  }

  Reader disposition =
      read_until_delimeter(&header_reader, string_from_cstring("; "));

  if (!string_equals(reader_to_string(disposition),
                     string_from_cstring("form-data"))) {
    return;
  }

  parse_body_part_form_data(body_part, header_reader);
}

ParseResult parse_body(Reader *request_reader, Request *request) {
  ParseResult result;

  request->body = read_until_end(request_reader);

  request->multipart = (BodyParts){0};

  String multipart_boundary = string_empty();

  char multipart_boundary_buf[1024] = {0};

  for (int i = 0; i < request->headers.count; i++) {
    Header header = request->headers.items[i];
    if (!string_equals(header.key, string_from_cstring("Content-Type"))) {
      continue;
    }

    Reader header_value = reader_from_string(header.value);
    Reader type_reader =
        read_until_delimeter(&header_value, string_from_cstring("; "));
    String type = reader_to_string(type_reader);
    if (string_equals(type, string_from_cstring("multipart/form-data"))) {
      read_until_delimeter(&header_value, string_from_cstring("boundary="));
      Reader boundary_reader = read_until_end(&header_value);
      WriteResult result = write_multipart_boundary(
          boundary_reader, multipart_boundary_buf, 1024, &multipart_boundary);

      if (result != WRITE_OK) {
        return PARSE_TEMP_BUFFER_OVERFLOW;
      }
    }
  }

  if (!string_is_empty(multipart_boundary)) {
    Reader body = request->body;
    Reader part;
    read_until_delimeter(&body, multipart_boundary);
    while (!reader_is_empty(
        (part = read_until_delimeter(&body, multipart_boundary)))) {
      // Skip trailing CRLF or -- on boundary
      part.offset += 2;
      if (reader_is_exhausted(part)) {
        break;
      }
      BodyPart body_part = {0};
      result = parse_headers(&part, &body_part.headers);
      if (result != PARSE_OK) {
        fprintf(stderr, "Error parsing header: %d\n", result);
        return result;
      }
      body_part.body = read_until_end(&part);
      parse_body_part_headers(&body_part);

      da_append(request->multipart, body_part);
    }
  }

  return PARSE_OK;
}

ParseResult read_request(int request_socket, char *request_buffer,
                         Request *request) {
  size_t request_size =
      read(request_socket, request_buffer, MAX_REQUEST_SIZE - 1);

  Reader reader = reader_new(request_buffer, request_size);

  ParseResult result;

  Reader request_line =
      read_until_delimeter(&reader, string_from_cstring(CRLF));

  result = parse_request_line(&request_line, request);
  if (result != PARSE_OK) {
    return result;
  }

  request->headers = (Headers){0};
  result = parse_headers(&reader, &request->headers);
  if (result != PARSE_OK) {
    return result;
  }

  result = parse_body(&reader, request);
  if (result != PARSE_OK) {
    return result;
  }

  return PARSE_OK;
}

WriteResult write_crlf(Writer *writer) {
  WRITE_OR_ERR(write_bytes(writer, CRLF, strlen(CRLF)));
  return WRITE_OK;
}

WriteResult write_response_status_line(HttpVersion version, StatusCode status,
                                       Writer *writer) {
  String version_str = version_to_string(version);
  WRITE_OR_ERR(write_string(writer, version_str));
  WRITE_OR_ERR(write_byte(writer, ' '));
  WRITE_OR_ERR(WRITE_FMT(writer, "%d", status));
  WRITE_OR_ERR(write_byte(writer, ' '));

  String reason_phrase = http_status_to_string(status);
  WRITE_OR_ERR(write_string(writer, reason_phrase));
  WRITE_OR_ERR(write_crlf(writer));

  return WRITE_OK;
}

WriteResult write_response_header(Header header, Writer *writer) {
  WRITE_OR_ERR(write_string(writer, header.key));
  WRITE_OR_ERR(write_bytes(writer, HEADER_DELIMETER, strlen(HEADER_DELIMETER)));
  WRITE_OR_ERR(write_string(writer, header.value));
  WRITE_OR_ERR(write_crlf(writer));

  return WRITE_OK;
}

WriteResult write_response_headers(Headers headers, Writer *writer) {
  for (int i = 0; i < headers.count; i++) {
    Header header = headers.items[i];
    WRITE_OR_ERR(write_response_header(header, writer));
  }
  return WRITE_OK;
}

WriteResult write_error_html(HttpVersion version, StatusCode status,
                             Writer *writer) {
  Header contentType;
  contentType.key = string_from_cstring("Content-Type");
  contentType.value = string_from_cstring("text/html");

  WRITE_OR_ERR(write_response_status_line(version, status, writer));
  WRITE_OR_ERR(write_response_header(contentType, writer));
  WRITE_OR_ERR(write_crlf(writer));

  String reason_phrase = http_status_to_string(status);
  WRITE_OR_ERR(WRITE_FMT(writer, "<h1>%.*s</h1>", (int)reason_phrase.len,
                         reason_phrase.buf));

  return WRITE_OK;
}

WriteResult write_replace(Reader *in, String *needles, String *replacements,
                          size_t needles_count, Writer *out) {
  for (; in->offset < in->len;) {
    int match_idx = -1;
    for (int i = 0; i < needles_count; i++) {
      bool match = true;
      String needle = needles[i];
      for (int j = 0; j < needle.len; j++) {
        if (in->buf[in->offset + j] != needle.buf[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        match_idx = i;
        break;
      }
    }
    if (match_idx != -1) {
      WRITE_OR_ERR(write_string(out, replacements[match_idx]));
      in->offset += needles[match_idx].len;
      continue;
    }
    WRITE_OR_ERR(write_byte(out, in->buf[in->offset]));
    in->offset++;
  }
  return WRITE_OK;
}

WriteResult write_echo_html(HttpVersion version, StatusCode status,
                            Request request, Writer *writer) {
  Header contentType;
  contentType.key = string_from_cstring("Content-Type");
  contentType.value = string_from_cstring("text/html");

  WRITE_OR_ERR(write_response_status_line(version, status, writer));
  WRITE_OR_ERR(write_response_header(contentType, writer));
  WRITE_OR_ERR(write_crlf(writer));

  String method_string = method_to_string(request.method);
  String version_string = version_to_string(request.version);

  WRITE_OR_ERR(WRITE_FMT(writer, "\
      <h1>Hello!</h1>\
      <h2>Status line</h2>\
      <table>\
        <tbody>\
          <tr>\
            <td>\
              <b>Method</b>\
            </td>\
            <td>\
              %.*s\
            </td>\
          </tr>\
          <tr>\
            <td>\
              <b>Resource</b>\
            </td>\
            <td>\
              %.*s\
            </td>\
          </tr>\
          <tr>\
            <td>\
              <b>Version</b>\
            </td>\
            <td>\
              %.*s\
            </td>\
          </tr>\
        </tbody>\
      </table>\
      <h2>Headers</h2>\
      <table>\
        <tbody>",
                         (int)method_string.len, method_string.buf,
                         (int)request.resource.len, request.resource.buf,
                         (int)version_string.len, version_string.buf));

  for (int i = 0; i < request.headers.count; i++) {
    Header header = request.headers.items[i];

    WRITE_OR_ERR(WRITE_FMT(writer, "\
      <tr>\
        <td>\
          <b>%.*s</b>\
        </td>\
        <td>\
          %.*s\
        </td>\
      </tr>",
                           (int)header.key.len, header.key.buf,
                           (int)header.value.len, header.value.buf));
  }

  WRITE_OR_ERR(write_string(writer, string_from_cstring("\
      </tbody>\
    </table>")));

  if (request.multipart.count > 0) {
    WRITE_OR_ERR(
        write_string(writer, string_from_cstring("<h2>Multipart body</h2>")));
    for (int i = 0; i < request.multipart.count; i++) {
      BodyPart part = request.multipart.items[i];
      WRITE_OR_ERR(WRITE_FMT(writer, "\
            <h3>Part %d</h3>\
            ",
                             i));
      if (!string_is_empty(part.formData.name)) {
        WRITE_OR_ERR(WRITE_FMT(writer, "\
              <h4>Name</h4>\
              <p>%.*s</p>",
                               (int)part.formData.name.len,
                               part.formData.name.buf));
      }
      if (!string_is_empty(part.formData.filename)) {
        WRITE_OR_ERR(WRITE_FMT(writer, "\
              <h4>Filename</h4>\
              <p>%.*s</p>",
                               (int)part.formData.filename.len,
                               part.formData.filename.buf));
      }
      WRITE_OR_ERR(write_string(writer, string_from_cstring("\
              <h4>Headers</h4>\
              <table>\
              <tbody>\
              ")));
      for (int j = 0; j < part.headers.count; j++) {
        Header header = part.headers.items[j];
        WRITE_OR_ERR(WRITE_FMT(writer, "\
              <tr>\
                <td>\
                  <b>%.*s</b>\
                </td>\
                <td>\
                  %.*s\
                </td>\
              </tr>\
              ",
                               (int)header.key.len, header.key.buf,
                               (int)header.value.len, header.value.buf));
      }

      WRITE_OR_ERR(WRITE_FMT(writer, "\
              </tbody>\
            </table>\
            <h4>Body</h4>\
            <pre style=\"border: 1px solid red; padding: 10px;\">%.*s</pre>",
                             (int)part.body.len, part.body.buf));
    }
  } else if (!reader_is_empty(request.body)) {
    WRITE_OR_ERR(WRITE_FMT(writer, "\
          <h2>Body</h2>\
          <pre style=\"border: 1px solid red; padding: 10px;\">%.*s</pre>",
                           (int)request.body.len, request.body.buf));
  }

  WRITE_OR_ERR(write_string(writer, string_from_cstring("\
        <h2>Navigate</h2>\
        <a href=\"/foo/bar\">/foo/bar</a>")));

  WRITE_OR_ERR(write_string(writer, string_from_cstring("\
        <h2>Form</h2>\
        <form action=\"/form/submit\" method=\"POST\" enctype=\"multipart/form-data\">\
          <h3>Title</h3>\
          <input name=\"title\" />\
          <h3>Text</h3>\
          <textarea name=\"text\"></textarea><br />\
          <h3>Attachment</h3>\
          <input type=\"file\" name=\"file\" /><br />\
          <input type=\"submit\"/>\
        </form>")));

  return WRITE_OK;
}

void write_panic(WriteResult write_result) {
  fprintf(stderr, "Panic: Error writing response: %d", write_result);
  exit(EXIT_FAILURE);
}

void accept_and_handle_request(int server_fd, struct sockaddr *address,
                               socklen_t addrlen, int request_socket,
                               char *request_buffer, char *response_buffer) {

  Request request;
  ParseResult parse_result =
      read_request(request_socket, request_buffer, &request);
  Writer writer = writer_new(response_buffer, MAX_RESPONSE_SIZE);
  WriteResult write_result;

  switch (parse_result) {
  case PARSE_OK:
    write_result = write_echo_html(HTTP_1_1, Ok, request, &writer);
    break;
  default:
    write_result = write_error_html(HTTP_1_1, BadRequest, &writer);
  }
  if (write_result != WRITE_OK) {
    write_panic(write_result);
    return;
  }

  printf("Responding and closing socket\n");
  send(request_socket, writer.buf, writer.offset, 0);
  close(request_socket);
}

int main(void) {
  printf("Starting server\n");

  int server_fd, request_socket;
  int opt = 1;
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("socket failed");
    return EXIT_FAILURE;
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    return EXIT_FAILURE;
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(PORT);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
    return EXIT_FAILURE;
  }

  if (listen(server_fd, 3) < 0) {
    perror("listen");
    return EXIT_FAILURE;
  }

  printf("Initializing event queue via io_uring\n");
  if (app_setup_uring()) {
    fprintf(stderr, "Unable to setup uring!\n");
    return EXIT_FAILURE;
  }

  int uring_res;

  while (1) {
    submit_to_sq(STDIN_FILENO, IORING_OP_READ);
    uring_res = read_from_cq();
    if (uring_res > 0) {
      submit_to_sq(STDOUT_FILENO, IORING_OP_WRITE);
      read_from_cq();
    } else if (uring_res == 0) {
      break;
    } else if (uring_res < 0) {
      fprintf(stderr, "Error: %s\n", strerror(abs(uring_res)));
      return EXIT_FAILURE;
    }
    offset += uring_res;
  }

  printf("Listening to port %d\n", PORT);

  while (1) {
    static char request_buffer[MAX_REQUEST_SIZE] = {0};
    static char response_buffer[MAX_RESPONSE_SIZE] = {0};

    submit_to_sq(STDIN_FILENO, IORING_OP_READ);
    uring_res = read_from_cq();

    uring_res = read_from_cq();
    if (uring_res > 0) {
    }

    int request_socket;

    if ((request_socket =
             accept(server_fd, (struct sockaddr *)&address, &addrlen)) < 0) {
      perror("accept");
      return EXIT_FAILURE;
    }

    accept_and_handle_request(server_fd, (struct sockaddr *)&address, addrlen,
                              request_socket, request_buffer, response_buffer);
  }

  close(server_fd);
  goto exit_success;

exit_failure:
  close(server_fd);
  return EXIT_FAILURE;

exit_success:
  return 0;
}
