#pragma once
#include <netinet/tcp.h>
#include <sys/types.h>

#include <string>
#include <vector>

#include "../../alog.h"
#include "../../net/socket.h"
#include "../../object.h"
#include "../../photon/thread.h"
#include "../../rpc/rpc.h"
#include "protocol.h"

namespace FileSystem {
struct NodeID;
class IFile;
class IFileSystem;
class RootSelector;
class ConnectionTracer;


class P2PClient : public Object {
public:
    virtual ssize_t preadv(const char* filename, const struct iovec* iov,
                           int iovcnt, off_t offset) = 0;

    // preadv with certian node id.
    // once server returned EBUSY, the id will be in use to store
    // the redirection node
    virtual ssize_t preadv(Net::EndPoint* node, const char* cfilename,
                           const struct iovec* iov, int iovcnt,
                           off_t offset) = 0;

    virtual ssize_t cachehit(const char* filename, off_t offset,
                             size_t count) = 0;

    virtual ssize_t cachehit(const Net::EndPoint& node, const char* filename,
                             off_t offset, size_t count) = 0;

    virtual ssize_t prefetch(const char* filename, off_t offset,
                             size_t count) = 0;

    virtual ssize_t prefetch(const Net::EndPoint& node, const char* filename,
                             off_t offset, size_t count) = 0;

    virtual ssize_t fstat(const Net::EndPoint& node, const char* filename,
                          struct stat* stat) = 0;

    virtual ssize_t fstat(const char* filename, struct stat* stat) = 0;

    virtual ssize_t fstat_v2(Net::EndPoint* node, const char* filename,
                          struct stat* stat) = 0;

    virtual ssize_t fstat_v2(const char* filename, struct stat* stat) = 0;

    virtual int evictKeys(const char* prefix) = 0;

    virtual ssize_t evict(const Net::EndPoint& node, const char* filename,
                          off_t offset, size_t count) = 0;

    virtual ssize_t pwritev(const char* filename, const struct iovec* iov,
                            int iovcnt, off_t offset) = 0;

    virtual ssize_t pwritev(Net::EndPoint* node, const char* cfilename,
                            const struct iovec* iov, int iovcnt,
                            off_t offset) = 0;

    //version = -1UL means forced announced; owner = 0.0.0.0:0 means this client has the latest data
    virtual ssize_t announce_latest(const char* filename, const Net::EndPoint& owner = Net::EndPoint(), uint64_t version = -1UL) = 0;

    virtual ssize_t announce_latest(const Net::EndPoint& node,
                                    const char* filename, const Net::EndPoint& owner = Net::EndPoint(), uint64_t version = -1UL) = 0;

    virtual ssize_t request_latest(const char* filename,
                                   uint64_t& latest_Version,
                                   Net::EndPoint& storage_ep) = 0;

    virtual ssize_t request_latest(const Net::EndPoint& node,
                                   const char* filename,
                                   uint64_t& latest_Version,
                                   Net::EndPoint& storage_ep) = 0;

    virtual IFileSystem* getfs() = 0;

    virtual int get_queue_count(Net::EndPoint ep) = 0;

    virtual int rename(const char* oldname, const char* newname) = 0;

    virtual int rename(Net::EndPoint& node, const char* oldname, const char* newname) = 0;

    virtual int truncate(const char* filename, off_t length) = 0;

    virtual int truncate(const Net::EndPoint& node, const char* filename, off_t length) = 0;
};

// Create an simple wrapper for P2P file client to an object of IFile.
// This object does NOT include caching mechanism
IFile* new_p2p_client_file(const char* filename, P2PClient* client);


P2PClient* new_p2pclient(const NodeID& id, const NodeID& root,
                        int ttl = 200,
                         int retry_time = 5, const char* domain = nullptr,
                         uint64_t connect_timeout = 1000UL * 1000,
                         uint64_t rpc_timeout = -1,
                         IFileSystem* checkedfs = nullptr,
                         bool root_ssl = false);

}  // namespace FileSystem
