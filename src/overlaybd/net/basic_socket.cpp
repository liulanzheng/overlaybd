#include "basic_socket.h"

#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/socket.h>
#ifndef __APPLE__
#include <sys/sendfile.h>
#endif
#include <sys/uio.h>
#include "../photon/syncio/fd-events.h"
#include "../photon/thread.h"
#include "../iovector.h"
#include "../utility.h"

#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY    0x4000000
#endif

namespace Net {
int set_fd_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return (flags < 0) ? flags : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
int set_socket_nonblocking(int fd) {
#ifdef __APPLE__
    return set_fd_nonblocking(fd);
#else
    int flags = 1;
    return ioctl(fd, FIONBIO, &flags);
#endif
}
static int set_socket_nonblocking_close_on_fail(int fd) {
    int ret = set_socket_nonblocking(fd);
    if (ret < 0) {
        int x = errno;
        close(fd);
        errno = x;
        return ret;
    }
    return fd;
}
int socket(int domain, int type, int protocol) {
    int fd = ::socket(domain, type, protocol);
    return (fd < 0) ? fd : set_socket_nonblocking_close_on_fail(fd);
}
int connect(int fd, const struct sockaddr *addr, socklen_t addrlen,
            uint64_t timeout) {
    int err = 0;
    while (true) {
        int ret = ::connect(fd, addr, addrlen);
        if (ret < 0) {
            auto e = errno;  // errno is usually a macro that expands to a
                             // function call
            if (e == EINTR) {
                err = 1;
                continue;
            }
            if (e == EINPROGRESS || (e == EADDRINUSE && err == 1)) {
                photon::wait_for_fd_writable(fd, timeout);
                socklen_t n = sizeof(err);
                ret = getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &n);
                if (ret < 0) return -1;
                if (err) {
                    errno = err;
                    return -1;
                }
                return 0;
            }
        }
        return ret;
    }
}
template <typename IOCB, typename WAITCB>
static __FORCE_INLINE__ ssize_t doio(IOCB iocb, WAITCB waitcb) {
    while (true) {
        ssize_t ret = iocb();
        if (ret < 0) {
            auto e = errno;  // errno is usually a macro that expands to a
                             // function call
            if (e == EINTR) continue;
            if (e == EAGAIN || e == EWOULDBLOCK) {
                if (waitcb())  // non-zero result means timeout or interrupt,
                               // need to return
                    return ret;
                continue;
            }
        }
        return ret;
    }
}

int accept(int fd, struct sockaddr *addr, socklen_t *addrlen,
           uint64_t timeout) {
    int newfd =
        (int)doio(LAMBDA(::accept(fd, addr, addrlen)),
                  LAMBDA_TIMEOUT(photon::wait_for_fd_readable(fd, timeout)));
    return (newfd < 0) ? newfd : set_socket_nonblocking_close_on_fail(newfd);
}
ssize_t read(int fd, void *buf, size_t count, uint64_t timeout) {
    return doio(LAMBDA(::read(fd, buf, count)),
                LAMBDA_TIMEOUT(photon::wait_for_fd_readable(fd, timeout)));
}
ssize_t readv(int fd, const struct iovec *iov, int iovcnt, uint64_t timeout) {
    if (iovcnt <= 0) {
        errno = EINVAL;
        return -1;
    }
    if (iovcnt == 1) return read(fd, iov->iov_base, iov->iov_len, timeout);
    return doio(LAMBDA(::readv(fd, iov, iovcnt)),
                LAMBDA_TIMEOUT(photon::wait_for_fd_readable(fd, timeout)));
}
ssize_t write(int fd, const void *buf, size_t count, uint64_t timeout) {
    return doio(LAMBDA(::write(fd, buf, count)),
                LAMBDA_TIMEOUT(photon::wait_for_fd_writable(fd, timeout)));
}
ssize_t writev(int fd, const struct iovec *iov, int iovcnt, uint64_t timeout) {
    if (iovcnt <= 0) {
        errno = EINVAL;
        return -1;
    }
    if (iovcnt == 1) return write(fd, iov->iov_base, iov->iov_len, timeout);
    return doio(LAMBDA(::writev(fd, iov, iovcnt)),
                LAMBDA_TIMEOUT(photon::wait_for_fd_writable(fd, timeout)));
}
ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count,
                 uint64_t timeout) {
#ifdef __APPLE__
    off_t len = count;
    ssize_t ret =
        doio(LAMBDA(::sendfile(out_fd, in_fd, *offset, &len, nullptr, 0)),
             LAMBDA_TIMEOUT(wait_for_fd_writable(out_fd, timeout)));
    return (ret == 0) ? len : (int)ret;
#else
    return doio(LAMBDA(::sendfile(out_fd, in_fd, offset, count)),
                LAMBDA_TIMEOUT(photon::wait_for_fd_writable(out_fd, timeout)));
#endif
}

ssize_t sendmsg_zerocopy(int fd, iovec* iov, int iovcnt, uint32_t& num_calls, uint64_t timeout) {
    msghdr msg = {};
    msg.msg_iov = iov;
    msg.msg_iovlen = iovcnt;
    ssize_t ret = doio(LAMBDA(::sendmsg(fd, &msg, MSG_ZEROCOPY)),
                       LAMBDA_TIMEOUT(photon::wait_for_fd_writable(fd, timeout)));
    num_calls++;
    return ret;
}

ssize_t read_n(int fd, void *buf, size_t count, uint64_t timeout) {
    return doio_n(buf, count, LAMBDA_TIMEOUT(read(fd, buf, count, timeout)));
}
ssize_t write_n(int fd, const void *buf, size_t count, uint64_t timeout) {
    return doio_n((void *&)buf, count,
                  LAMBDA_TIMEOUT(write(fd, buf, count, timeout)));
}
ssize_t sendfile_n(int out_fd, int in_fd, off_t *offset, size_t count,
                   uint64_t timeout) {
    void* buf_unused = nullptr;
    return doio_n(buf_unused, count,
        LAMBDA_TIMEOUT(sendfile(out_fd, in_fd, offset, count, timeout)));
}

ssize_t readv_n(int fd, struct iovec *iov, int iovcnt, uint64_t timeout) {
    iovector_view v(iov, iovcnt);
    return doiov_n(v, LAMBDA_TIMEOUT(readv(fd, v.iov, v.iovcnt, timeout)));
}
ssize_t writev_n(int fd, struct iovec *iov, int iovcnt, uint64_t timeout) {
    iovector_view v(iov, iovcnt);
    return doiov_n(v, LAMBDA_TIMEOUT(writev(fd, v.iov, v.iovcnt, timeout)));
}

ssize_t zerocopy_n(int fd, iovec* iov, int iovcnt, uint32_t& num_calls, uint64_t timeout) {
    iovector_view v(iov, iovcnt);
    return doiov_n(v, LAMBDA_TIMEOUT(sendmsg_zerocopy(fd, v.iov, v.iovcnt, num_calls, timeout)));
}

}  // namespace Net