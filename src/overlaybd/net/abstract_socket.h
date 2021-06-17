#pragma once

/***
Internal header provides abstract socket base class
***/

#include <vector>

#include "../alog.h"
#include "../photon/thread11.h"
#include "socket.h"

#define UNIMPLEMENTED(method)                                   \
    virtual method override {                                   \
        LOG_ERROR_RETURN(ENOSYS, -1, #method " unimplemented"); \
    }

#define UNIMPLEMENTED_PTR(method)                                    \
    virtual method override {                                        \
        LOG_ERROR_RETURN(ENOSYS, nullptr, #method " unimplemented"); \
    }

#define UNIMPLEMENTED_VOID(method)                            \
    virtual method override {                                 \
        LOG_ERROR_RETURN(ENOSYS, , #method " unimplemented"); \
    }

namespace Net {
/// Abstract base class combines all socket interfaces
class SocketBase : public ISocketStream,
                   public ISocketClient,
                   public ISocketServer {
public:
    UNIMPLEMENTED(int setsockopt(int level, int option_name,
                                 const void* option_value,
                                 socklen_t option_len))
    UNIMPLEMENTED(int getsockopt(int level, int option_name, void* option_value,
                                 socklen_t* option_len))
    UNIMPLEMENTED(int getsockname(EndPoint& addr))
    UNIMPLEMENTED(int getpeername(EndPoint& addr))
    UNIMPLEMENTED(int getsockname(char* path, size_t count))
    UNIMPLEMENTED(int getpeername(char* path, size_t count))
    UNIMPLEMENTED(uint64_t timeout())
    UNIMPLEMENTED_VOID(void timeout(uint64_t tm))
    UNIMPLEMENTED(int close())
    UNIMPLEMENTED(ssize_t read(void* buf, size_t count))
    UNIMPLEMENTED(ssize_t readv(const struct iovec* iov, int iovcnt))
    UNIMPLEMENTED(ssize_t write(const void* buf, size_t count))
    UNIMPLEMENTED(ssize_t writev(const struct iovec* iov, int iovcnt))
    UNIMPLEMENTED(ssize_t recv(void* buf, size_t count))
    UNIMPLEMENTED(ssize_t recv(const struct iovec* iov, int iovcnt))
    UNIMPLEMENTED(ssize_t send(const void* buf, size_t count))
    UNIMPLEMENTED(ssize_t send(const struct iovec* iov, int iovcnt))
    UNIMPLEMENTED_PTR(ISocketStream* connect(const EndPoint& ep))
    UNIMPLEMENTED_PTR(ISocketStream* connect(const char* path, size_t count))
    UNIMPLEMENTED(int bind(uint16_t port, IPAddr addr))
    UNIMPLEMENTED(int bind(const char* path, size_t count))
    UNIMPLEMENTED(int listen(int backlog = 1024))
    UNIMPLEMENTED_PTR(ISocketStream* accept())
    UNIMPLEMENTED_PTR(ISocketStream* accept(EndPoint* remote_endpoint))
    UNIMPLEMENTED_PTR(ISocketServer* set_handler(Handler handler))
    UNIMPLEMENTED(int start_loop(bool block))
    UNIMPLEMENTED_VOID(void terminate())
};

struct SocketOpt {
    int level;
    int opt_name;
    void* opt_val;
    socklen_t opt_len;
};
class SockOptBuffer : public std::vector<SocketOpt> {
protected:
    static constexpr uint64_t BUFFERSIZE = 8192;
    char buffer[BUFFERSIZE];
    char* ptr = buffer;

public:
    int put_opt(int level, int name, const void* val, socklen_t len) {
        if (ptr + len >= buffer + BUFFERSIZE) {
            return -1;
        }
        memcpy(ptr, val, len);
        ptr += len;
        push_back(SocketOpt{level, name, ptr, len});
        return 0;
    }
};

}  // namespace Net