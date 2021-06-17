#include "socket.h"

#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "../alog.h"
#include "../iovector.h"
#include "../photon/syncio/fd-events.h"
#include "../photon/thread.h"
#include "../photon/thread11.h"
#include "../utility.h"
#include "abstract_socket.h"
#include "basic_socket.h"
#include "tlssocket.h"

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif

namespace Net {

static int fill_path(struct sockaddr_un& name, const char* path, size_t count) {
    const int LEN = sizeof(name.sun_path) - 1;
    if (count == 0) count = strlen(path);
    if (count > LEN)
        LOG_ERROR_RETURN(ENAMETOOLONG, -1, "pathname is too long (`>`)", count,
                         LEN);

    memset(&name, 0, sizeof(name));
    memcpy(name.sun_path, path, count + 1);
#ifndef __linux__
    name.sun_len = 0;
#endif
    name.sun_family = AF_UNIX;
    return 0;
}

class KernelSocket : public SocketBase {
public:
    int fd;
    bool m_autoremove = false;
    explicit KernelSocket(int fd) : fd(fd) {}
    KernelSocket(int socket_family, bool autoremove)
        : m_autoremove(autoremove) {
        fd = Net::socket(socket_family, SOCK_STREAM, 0);
    }
    virtual ~KernelSocket() {
        if (fd > 0) {
            if (m_autoremove) {
                char filename[PATH_MAX];
                if (0 == getsockname(filename, PATH_MAX)) {
                    unlink(filename);
                }
            }
        }
        close();
    }
    virtual int close() override {
        auto ret = ::close(fd);
        fd = -1;
        return ret;
    }
    typedef int (*Getter)(int sockfd, struct sockaddr* addr,
                          socklen_t* addrlen);
    int do_getname(EndPoint& addr, Getter getter) {
        struct sockaddr_in addr_in;
        socklen_t len = sizeof(addr_in);
        int ret = getter(fd, (struct sockaddr*)&addr_in, &len);
        if (ret < 0 || len != sizeof(addr_in)) return -1;
        addr.from_sockaddr_in(addr_in);
        return 0;
    }
    virtual int getsockname(EndPoint& addr) override {
        return do_getname(addr, &::getsockname);
    }
    virtual int getpeername(EndPoint& addr) override {
        return do_getname(addr, &::getpeername);
    }
    int do_getname(char* path, size_t count, Getter getter) {
        struct sockaddr_un addr_un;
        socklen_t len = sizeof(addr_un);
        int ret = getter(fd, (struct sockaddr*)&addr_un, &len);
        // if len larger than size of addr_un, or less than prefix in addr_un
        if (ret < 0 || len > sizeof(addr_un) ||
            len <= sizeof(addr_un.sun_family))
            return -1;

        strncpy(path, addr_un.sun_path, count);
        return 0;
    }
    virtual int getsockname(char* path, size_t count) override {
        return do_getname(path, count, &::getsockname);
    }
    virtual int getpeername(char* path, size_t count) override {
        return do_getname(path, count, &::getpeername);
    }
    virtual int setsockopt(int level, int option_name, const void* option_value,
                           socklen_t option_len) override {
        return ::setsockopt(fd, level, option_name, option_value, option_len);
    }
    virtual int getsockopt(int level, int option_name, void* option_value,
                           socklen_t* option_len) override {
        return ::getsockopt(fd, level, option_name, option_value, option_len);
    }
};

class KernelSocketStream : public KernelSocket {
public:
    photon::mutex m_rmutex, m_wmutex;
    uint64_t m_timeout = -1;

    using KernelSocket::KernelSocket;
    virtual ~KernelSocketStream() { shutdown(ShutdownHow::ReadWrite); }
    virtual ssize_t read(void* buf, size_t count) override {
        photon::scoped_lock lock(m_rmutex);
        return Net::read_n(fd, buf, count, m_timeout);
    }
    virtual ssize_t write(const void* buf, size_t count) override {
        photon::scoped_lock lock(m_wmutex);
        return Net::write_n(fd, buf, count, m_timeout);
    }
    virtual ssize_t readv(const struct iovec* iov, int iovcnt) override {
        SmartCloneIOV<32> ciov(iov, iovcnt);
        return readv_mutable(ciov.ptr, iovcnt);
    }
    virtual ssize_t readv_mutable(struct iovec* iov, int iovcnt) override {
        photon::scoped_lock lock(m_rmutex);
        return Net::readv_n(fd, iov, iovcnt, m_timeout);
    }
    virtual ssize_t writev(const struct iovec* iov, int iovcnt) override {
        SmartCloneIOV<32> ciov(iov, iovcnt);
        return writev_mutable(ciov.ptr, iovcnt);
    }
    virtual ssize_t writev_mutable(struct iovec* iov, int iovcnt) override {
        photon::scoped_lock lock(m_wmutex);
        return Net::writev_n(fd, iov, iovcnt, m_timeout);
    }
    virtual ssize_t recv(void* buf, size_t count) override {
        photon::scoped_lock lock(m_rmutex);
        return Net::read(fd, buf, count, m_timeout);
    }
    virtual ssize_t recv(const struct iovec* iov, int iovcnt) override {
        photon::scoped_lock lock(m_rmutex);
        return Net::readv(fd, iov, iovcnt, m_timeout);
    }
    virtual ssize_t send(const void* buf, size_t count) override {
        photon::scoped_lock lock(m_wmutex);
        return Net::write(fd, buf, count, m_timeout);
    }
    virtual ssize_t send(const struct iovec* iov, int iovcnt) override {
        photon::scoped_lock lock(m_wmutex);
        return Net::writev(fd, iov, iovcnt, m_timeout);
    }
    virtual uint64_t timeout() override { return m_timeout; }
    virtual void timeout(uint64_t tm) override { m_timeout = tm; }
    virtual int shutdown(ShutdownHow how) override {
        // shutdown how defined as 0 for RD, 1 for WR and 2 for RDWR
        // in sys/socket.h, cast ShutdownHow into int just fits
        return ::shutdown(fd, static_cast<int>(how));
    }
};

// factory class
class KernelSocketClient : public SocketBase {
public:
    int m_socket_family;
    bool m_autoremove = false;
    SockOptBuffer opts;
    uint64_t m_timeout = -1;

    KernelSocketClient(int socket_family, bool autoremove)
        : m_socket_family(socket_family), m_autoremove(autoremove) {}

    virtual int setsockopt(int level, int option_name, const void* option_value,
                           socklen_t option_len) override {
        return opts.put_opt(level, option_name, option_value, option_len);
    }

    KernelSocket* create_socket() {
        auto sock = new KernelSocketStream(m_socket_family, m_autoremove);
        if (sock->fd < 0)
            LOG_ERROR_RETURN(0, nullptr, "Failed to create socket fd");
        for (auto& opt : opts) {
            auto ret = sock->setsockopt(opt.level, opt.opt_name, opt.opt_val,
                                        opt.opt_len);
            if (ret < 0) {
                delete sock;
                LOG_ERROR_RETURN(EINVAL, nullptr, "Failed to setsockopt ",
                                 VALUE(opt.level), VALUE(opt.opt_name));
            }
        }
        sock->timeout(m_timeout);
        return sock;
    }

    virtual ISocketStream* connect(const EndPoint& ep) override {
        auto addr_in = ep.to_sockaddr_in();
        auto sock = create_socket();
        if (!sock) return nullptr;
        auto ret = Net::connect(sock->fd, (struct sockaddr*)&addr_in,
                                sizeof(addr_in), m_timeout);
        if (ret < 0) {
            delete sock;
            LOG_ERRNO_RETURN(0, nullptr, "Failed to connect to ", ep);
        }
        return sock;
    }
    virtual ISocketStream* connect(const char* path, size_t count) override {
        struct sockaddr_un addr_un;
        int ret = fill_path(addr_un, path, count);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, nullptr, "Failed to fill uds addr");
        }
        auto sock = create_socket();
        if (!sock) return nullptr;
        ret = Net::connect(sock->fd, (struct sockaddr*)&addr_un,
                           sizeof(addr_un), m_timeout);
        if (ret < 0) return nullptr;
        return sock;
    }
    virtual uint64_t timeout() override { return m_timeout; }
    virtual void timeout(uint64_t tm) override { m_timeout = tm; }
};

LogBuffer& operator<<(LogBuffer& log, const IPAddr addr) {
    return log.printf(addr.d, '.', addr.c, '.', addr.b, '.', addr.a);
}

LogBuffer& operator<<(LogBuffer& log, const EndPoint ep) {
    return log << ep.addr << ':' << ep.port;
}

LogBuffer& operator<<(LogBuffer& log, const in_addr& iaddr) {
    return log << Net::IPAddr(ntohl(iaddr.s_addr));
}

LogBuffer& operator<<(LogBuffer& log, const sockaddr_in& addr) {
    return log << Net::EndPoint(addr);
}

LogBuffer& operator<<(LogBuffer& log, const sockaddr& addr) {
    if (addr.sa_family == AF_INET) {
        log << (const sockaddr_in&)addr;
    } else {
        log.printf("<sockaddr>");
    }
    return log;
}

template <typename SocketType>
static SocketType* new_socket(int socket_family, bool autoremove,
                              ALogStringL socktype) {
    auto sock = new SocketType(socket_family, autoremove);
    if (sock->fd < 0) {
        delete sock;
        LOG_ERROR_RETURN(0, nullptr, "Failed to create ` socket", socktype);
    }
    return sock;
}

extern "C" ISocketClient* new_tcp_socket_client() {
    return new KernelSocketClient(AF_INET, false);
}


#ifndef NET_UNIT_TEST
// TCPSocket* __attribute__((weak)) new_tls_socket() {
//     LOG_ERROR_RETURN(
//         ENXIO, nullptr,
//         "Entry the weak implemention, link libease_tls befor libease");
// }
#endif
}  // namespace Net
