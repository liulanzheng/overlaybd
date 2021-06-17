#pragma once
#include <inttypes.h>
#include <sys/socket.h>

#include "../iovector.h"
#include "../photon/thread.h"
#include "../timeout.h"
#include "../utility.h"

namespace Net {
int socket(int domain, int type, int protocol);

int connect(int fd, const struct sockaddr *addr, socklen_t addrlen,
            uint64_t timeout = -1);

int accept(int fd, struct sockaddr *addr, socklen_t *addrlen,
           uint64_t timeout = -1);

ssize_t read(int fd, void *buf, size_t count, uint64_t timeout = -1);

ssize_t readv(int fd, const struct iovec *iov, int iovcnt,
              uint64_t timeout = -1);

ssize_t write(int fd, const void *buf, size_t count, uint64_t timeout = -1);

ssize_t writev(int fd, const struct iovec *iov, int iovcnt,
               uint64_t timeout = -1);

ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count,
                 uint64_t timeout = -1);

ssize_t read_n(int fd, void *buf, size_t count, uint64_t timeout = -1);

ssize_t write_n(int fd, const void *buf, size_t count, uint64_t timeout = -1);

ssize_t readv_n(int fd, struct iovec *iov, int iovcnt, uint64_t timeout = -1);

ssize_t writev_n(int fd, struct iovec *iov, int iovcnt, uint64_t timeout = -1);

ssize_t sendfile_n(int out_fd, int in_fd, off_t *offset, size_t count,
                   uint64_t timeout = -1);

ssize_t zerocopy_n(int fd, iovec* iov, int iovcnt, uint32_t& num_calls, uint64_t timeout = -1);

int set_socket_nonblocking(int fd);

int set_fd_nonblocking(int fd);

#define LAMBDA(expr) [&]() __INLINE__ { return expr; }
#define LAMBDA_TIMEOUT(expr)              \
    [&]() __INLINE__ {                    \
        Timeout __tmo(timeout);           \
        DEFER(timeout = __tmo.timeout()); \
        return expr;                      \
    }

template <typename IOCB>
__FORCE_INLINE__ ssize_t doio_n(void *&buf, size_t &count, IOCB iocb) {
    auto count0 = count;
    while (count > 0) {
        ssize_t ret = iocb();
        if (ret <= 0) return ret;
        (char *&)buf += ret;
        count -= ret;
    }
    return count0;
}

template <typename IOCB>
__FORCE_INLINE__ ssize_t doiov_n(iovector_view &v, IOCB iocb) {
    ssize_t count = 0;
    while (v.iovcnt > 0) {
        ssize_t ret = iocb();
        if (ret <= 0) return ret;
        count += ret;

        uint64_t bytes = ret;
        auto extracted = v.extract_front(bytes);
        assert(extracted == bytes);
        _unused(extracted);
    }
    return count;
}
}  // namespace Net