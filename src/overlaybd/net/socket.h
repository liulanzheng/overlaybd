#pragma once
#include <inttypes.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <string.h>
#include <functional>

#include "../stream.h"
#include "../callback.h"
#include "../object.h"

struct LogBuffer;
namespace Net
{
    union IPAddr
    {
        uint32_t addr = 0;
        struct { uint8_t a, b, c, d; };
        explicit IPAddr(uint32_t nl)
        {
            from_nl(nl);
        }
        explicit IPAddr(const char* s)
        {
            struct in_addr addr;
            if (inet_aton(s, &addr) == 0)
                return;  // invalid IPv4 address
            from_nl(addr.s_addr);
        }
        IPAddr() = default;
        IPAddr(const IPAddr& rhs) = default;
        uint32_t to_nl() const
        {
            return htonl(addr);
        }
        void from_nl(uint32_t nl)
        {
            addr = ntohl(nl);
        }
    };

    struct EndPoint
    {
        IPAddr addr;
        uint16_t port;
        EndPoint() = default;
        EndPoint(IPAddr ip, uint16_t port): addr(ip), port(port) {}
        EndPoint(const struct sockaddr_in& addr_in)
        {
            from(addr_in);
        }
        sockaddr_in to_sockaddr_in() const
        {
            struct sockaddr_in addr_in;
            addr_in.sin_family = AF_INET;
            addr_in.sin_addr.s_addr = addr.to_nl();
            addr_in.sin_port = htons(port);
            return addr_in;
        }
        void from_sockaddr_in(const struct sockaddr_in& addr_in)
        {
            addr.from_nl(addr_in.sin_addr.s_addr);
            port = ntohs(addr_in.sin_port);
        }
        void from(const struct sockaddr_in& addr_in)
        {
            from_sockaddr_in(addr_in);
        }
        bool operator==(const EndPoint& rhs) const {
            return rhs.addr.addr == addr.addr && rhs.port == port;
        }
        bool operator!=(const EndPoint& rhs) const {
            return !operator==(rhs);
        }
    };

    // operators to help with logging IP addresses
    LogBuffer& operator << (LogBuffer& log, const IPAddr addr);
    LogBuffer& operator << (LogBuffer& log, const EndPoint ep);
    LogBuffer& operator << (LogBuffer& log, const in_addr& iaddr);
    LogBuffer& operator << (LogBuffer& log, const sockaddr_in& addr);
    LogBuffer& operator << (LogBuffer& log, const sockaddr& addr);

    class ISocket
    {
    public:
        virtual ~ISocket() = default;

        virtual int setsockopt(int level, int option_name,
                const void *option_value, socklen_t option_len) = 0;
        virtual int getsockopt(int level, int option_name,
                void* option_value, socklen_t* option_len) = 0;
        template<typename T>
        int setsockopt(int level, int option_name, T value)
        {
            return setsockopt(level, option_name, &value, sizeof(value));
        }
        template<typename T>
        int getsockopt(int level, int option_name, T* value)
        {
            socklen_t len = sizeof(*value);
            return getsockopt(level, option_name, value, &len);
        }

        virtual int getsockname(EndPoint& addr) = 0;
        virtual int getpeername(EndPoint& addr) = 0;
        virtual int getsockname(char* path, size_t count) = 0;
        virtual int getpeername(char* path, size_t count) = 0;
        EndPoint getsockname() { EndPoint ep; getsockname(ep); return ep; }
        EndPoint getpeername() { EndPoint ep; getpeername(ep); return ep; }

        // get/set timeout, in us, (default +∞)
        virtual uint64_t timeout() = 0;
        virtual void timeout(uint64_t tm) = 0;
    };

    class ISocketStream : public IStream, public ISocket
    {
    public:
        // recv some bytes from the socket;
        // return actual # of bytes recvd, which may be LESS than `count`;
        // may block once at most, when there's no data yet in the socket;
        virtual ssize_t recv(void *buf, size_t count) = 0;
        virtual ssize_t recv(const struct iovec *iov, int iovcnt) = 0;

        // send some bytes to the socket;
        // return actual # of bytes sent, which may be LESS than `count`;
        // may block once at most, when there's no free space in the socket's internal buffer;
        virtual ssize_t send(const void *buf, size_t count) = 0;
        virtual ssize_t send(const struct iovec *iov, int iovcnt) = 0;
    };

    /// ISocketClient is kind of SocketStream factory
    class ISocketClient : public ISocket
    {
    public:
        virtual ISocketStream* connect(const EndPoint& ep) = 0;
        virtual ISocketStream* connect(const char* path, size_t count = 0) = 0;
    };

    class ISocketServer : public ISocket
    {
    public:
        virtual int bind(uint16_t port = 0, IPAddr addr = IPAddr()) = 0;
        virtual int bind(const char* path, size_t count) = 0;
        int bind(const char* path) { return bind(path, strlen(path)); }

        virtual int listen(int backlog = 1024) = 0;

        virtual ISocketStream* accept() = 0;
        virtual ISocketStream* accept(EndPoint* remote_endpoint) = 0;

        using Handler = Callback<ISocketStream*>;
        virtual ISocketServer* set_handler(Handler handler) = 0;
        virtual int start_loop(bool block = false) = 0;
        virtual void terminate() = 0;
    };

    extern "C" ISocketClient* new_tcp_socket_client();
}

namespace std {
template<>
struct hash<Net::EndPoint> {
    hash<uint64_t> hasher;
    size_t operator()(const Net::EndPoint& x) const {
        return hasher((x.addr.to_nl() << 16) | x.port);
    }
};
}
