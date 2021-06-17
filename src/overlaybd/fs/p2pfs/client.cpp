#include "client.h"

#include <cxxabi.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/stat.h>
#include "../../alog-audit.h"
#include "../../alog-stdstring.h"
#include "../../alog.h"
#include "../../callback.h"
#include "../../expirecontainer.h"
#include "../../iovector.h"
#include "../../net/socket.h"
#include "../../net/tlssocket.h"
#include "../../photon/thread.h"
#include "../../rpc/rpc.h"
#include "../../rpc/serialize.h"
#include "../../timeout.h"
#include "../checkedfs/checkedfs.h"
#include "../forwardfs.h"
#include "../localfs.h"
#include "../range-split-vi.h"
#include "../range-split.h"
#include "../virtual-file.h"
#include "protocol.h"
#include "root_selector.h"

using namespace std;
using namespace Net;
using namespace RPC;
using namespace photon;

namespace FileSystem {

class ClientFSAdaptor : public IFileSystem {
public:
    ClientFSAdaptor(P2PClient* client) : m_client(client) {}

    UNIMPLEMENTED_POINTER(IFile* creat(const char*, mode_t) override);
    UNIMPLEMENTED(int mkdir(const char*, mode_t) override);
    UNIMPLEMENTED(int rmdir(const char*) override);
    UNIMPLEMENTED(int link(const char*, const char*) override);
    UNIMPLEMENTED(int symlink(const char*, const char*) override);
    UNIMPLEMENTED(ssize_t readlink(const char*, char*, size_t) override);
    UNIMPLEMENTED(int chmod(const char*, mode_t) override);
    UNIMPLEMENTED(int chown(const char*, uid_t, gid_t) override);
    UNIMPLEMENTED(int statfs(const char* path, struct statfs* buf) override);
    UNIMPLEMENTED(int statvfs(const char* path, struct statvfs* buf) override);
    UNIMPLEMENTED(int lstat(const char* path, struct stat* buf) override);
    UNIMPLEMENTED(int access(const char* pathname, int mode) override);
    UNIMPLEMENTED(int syncfs() override);
    UNIMPLEMENTED(int unlink(const char* filename) override);
    UNIMPLEMENTED(int lchown(const char* pathname, uid_t owner, gid_t group)
                      override);
    UNIMPLEMENTED_POINTER(DIR* opendir(const char*) override);

    virtual IFile* open(const char* pathname, int flags) override {
        return new_p2p_client_file(pathname, m_client);
    }

    virtual IFile* open(const char* filename, int flags, mode_t mode) override {
        return open(filename, flags);  // mode will be ignored
    }

    virtual int stat(const char* path, struct stat* buf) override {
        return m_client->fstat(path, buf);
    }

    int rename(const char* oldname, const char* newname) override {
        return m_client->rename(oldname, newname);
    }

    int truncate(const char* path, off_t length) override {
        return m_client->truncate(path, length);
    }

protected:
    IFileSystem* m_metafs;
    P2PClient* m_client;
};



class P2PClientImpl : public P2PClient {
public:
    P2PClientImpl(const NodeID& myid, RootSelector* root,
                  uint64_t expire_timeout = 10UL * 1000 * 1000,
                  uint64_t expire_timer = 5UL * 1000 * 1000,
                  int ttl = 200,
                  int retry_limit = 5, const char* domain = nullptr,
                  uint64_t connect_timeout = 1000UL * 1000,
                  uint64_t rpc_timeout = -1, IFileSystem* checkedfs = nullptr,
                  bool root_ssl = false)
        : m_myid(myid),
          m_root(root),
          m_ttl(ttl),
          m_blacklist(expire_timeout),
          m_retry_limit(retry_limit),
          m_domain(domain ? domain : ""),
          m_checkedfs(checkedfs),
          m_root_ssl(root_ssl) {
        m_pool =
            RPC::new_stub_pool(expire_timeout, connect_timeout, rpc_timeout);
        m_metas = new ObjectCache<std::string, FileMeta*>(expire_timeout);
    }

    ~P2PClientImpl() {
        delete m_pool;
        delete m_checkedfs;
    }

    virtual ssize_t preadv(Net::EndPoint* ep, const char* filename,
                           const struct iovec* iov, int iovcnt,
                           off_t offset) override {
        if (ep == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "node id cannot be NULL");
        }
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }

        P2PReadV::Response resp;
        resp.buf.assign((iovec*)iov, iovcnt);

        P2PReadV::Request req;
        req.filename.assign(filename);
        req.domain.assign(m_domain);
        req.count = resp.buf.summed_size;
        req.id = m_myid;
        req.ttl = m_ttl;
        req.offset = offset;
        auto local = thread_get_local();
        if (local) {
            req.ttl = *static_cast<int*>(local);
        }

        int retry = m_retry_limit + 1;
    again:
        if (--retry < 0) {
            ERRNO err;
            LOG_ERROR_RETURN(0, -err.no, "Retry too many times");
        }
        std::vector<NodeID> blacklist;
        for (const auto& x : m_blacklist) {
            blacklist.emplace_back(x);
        }
        req.blacklist.assign(blacklist);
        auto ret = rpc_call<P2PReadV>(*ep, req, resp);
        if (ret < 0) return ret;
        if (resp.ret < 0) {
            switch (resp.ret) {
                case -EAGAIN:
                    LOG_INFO("Server said AGAIN, try in 2 secs");
                    thread_sleep(2);
                    resp.buf.assign((iovec*)iov, iovcnt);
                    errno = -resp.ret;
                    goto again;
                case -EBUSY:
                    LOG_INFO("Server said BUSY, redirect");
                    *ep = resp.redirect2.endpoint();
            }
            auto eno = -resp.ret;
            errno = eno;
        } else if (m_checkedfs) {
            SmartCloneIOV<32> viov(iov, iovcnt);
            iovector_view vioview(viov.ptr, iovcnt);
            vioview.shrink_to(resp.ret);

            auto ctor = [&]() { return get_meta_v1(m_checkedfs, filename); };
            auto meta = m_metas->acquire(filename, ctor);
            if (!meta) {
                return -EIO;  // for no meta
            }
            DEFER(m_metas->release(filename));
            auto chk = meta->verify(vioview.iov, vioview.iovcnt, offset);
            if (chk < 0) {
                LOG_ERROR_RETURN(EVERIFY, -EVERIFY, "VERIFY FAILED ",
                                 VALUE(filename), VALUE(resp.ret),
                                 VALUE(offset));
            }
        }
        return resp.ret;
    }

    virtual ssize_t preadv(const char* filename, const struct iovec* iov,
                           int iovcnt, off_t offset) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        auto local = thread_get_local();
        if (local) {
            auto ttl = *static_cast<int*>(local);
            if (ttl <= 0)
                LOG_ERROR_RETURN(0, -EDEADLK, "ttl less than one, myid:`",
                                 m_myid.endpoint())
        }
        return call_along_upstream<ssize_t>(
            filename,
            [&](Net::EndPoint* ep, bool isroot) {
                auto ret = preadv(ep, filename, iov, iovcnt, offset);
                ERRNO err;
                if (/*isroot &&*/ ret < 0 && err.no == EVERIFY) {
                    // means checksum failure
                    // so tells upstream to evict
                    LOG_INFO(
                        "Get EVERIFY during read, caused by checksum verify.");
                    LOG_INFO("Ask upstream root ` to evict this piece of data");
                    iovector_view iovec((struct iovec*)iov, iovcnt);
                    evict(*ep, filename, offset, iovec.sum());
                    errno = err.no;
                }
                return ret;
            },
            [&](Net::EndPoint old, Net::EndPoint ep, bool isroot) {
                return true;
            });
    }

    virtual ssize_t cachehit(const Net::EndPoint& ep, const char* filename,
                             off_t offset, size_t count) override {
        P2PCacheHit::Request req;
        P2PCacheHit::Response resp;
        req.filename.assign(filename);
        req.offset = offset;
        req.count = count;
        int ret = rpc_call<P2PCacheHit>(ep, req, resp);
        if (ret < 0) return ret;
        return resp.ret;
    }

    virtual ssize_t cachehit(const char* filename, off_t offset,
                             size_t count) override {
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return cachehit(*ep, filename, offset, count);
            });
    }

    virtual ssize_t prefetch(const Net::EndPoint& ep, const char* filename,
                             off_t offset, size_t count) override {
        P2PPrefetch::Request req;
        P2PPrefetch::Response resp;
        req.filename.assign(filename);
        req.offset = offset;
        req.count = count;
        int ret = rpc_call<P2PPrefetch>(ep, req, resp);
        if (ret < 0) return ret;
        return resp.ret;
    }

    virtual ssize_t prefetch(const char* filename, off_t offset,
                             size_t count) override {
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return prefetch(*ep, filename, offset, count);
            });
    }

    virtual ssize_t fstat(const Net::EndPoint& ep, const char* filename,
                          struct stat* stat) override {
        if (m_checkedfs) {
            // checkfile exists
            // so just return checksum meta instead of remote stat
            auto ctor = [&]() { return get_meta_v1(m_checkedfs, filename); };
            auto meta = m_metas->acquire(filename, ctor);
            if (!meta) {
                return -ENOENT;  // for no meta
            }
            DEFER(m_metas->release(filename));
            stat->st_size = meta->size;
            stat->st_blocks = meta->segSize;
            stat->st_mode = S_IFREG;
            return 0;
        } else {
            // remote stat
            P2PStat::Request req;
            P2PStat::Response resp;
            req.filename.assign(filename);
            int ret = rpc_call<P2PStat>(ep, req, resp);
            if (ret < 0) return ret;
            memcpy(stat, &resp.stat, sizeof(*stat));
            return resp.ret;
        }
    }

    virtual ssize_t fstat_v2(Net::EndPoint* ep, const char* filename,
                          struct stat* stat) override {
        if (m_checkedfs) {
            // checkfile exists
            // so just return checksum meta instead of remote stat
            auto ctor = [&]() { return get_meta_v1(m_checkedfs, filename); };
            auto meta = m_metas->acquire(filename, ctor);
            if (!meta) {
                return -ENOENT;  // for no meta
            }
            DEFER(m_metas->release(filename));
            stat->st_size = meta->size;
            stat->st_blocks = meta->segSize;
            stat->st_mode = S_IFREG;
            return 0;
        } else {
            // remote stat
            P2PStat_V2::Request req;
            P2PStat_V2::Response resp;
            req.filename.assign(filename);
            int ret = rpc_call<P2PStat_V2>(*ep, req, resp);
            if (ret < 0) return ret;
            if (resp.ret < 0) {
                switch (resp.ret) {
                    case -EBUSY:
                        *ep = resp.redirect2.endpoint();
                        LOG_INFO("Server said BUSY, redirect");
                }
                auto eno = -resp.ret;
                errno = eno;
            }
            memcpy(stat, &resp.stat, sizeof(*stat));
            return resp.ret;
        }
    }

    virtual ssize_t fstat(const char* filename, struct stat* stat) override {
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return fstat(*ep, filename, stat);
            });
    }

    virtual ssize_t fstat_v2(const char* filename, struct stat* stat) override {
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return fstat_v2(ep, filename, stat);
            },
            [&](Net::EndPoint old, Net::EndPoint ep, bool isroot) {
                return true;
            });
    }

    virtual int evictKeys(const char* prefix) override {
        auto nodes = m_root->get_root_list();
        std::string sprefix(prefix);
        for (auto& node : nodes) {
            P2PEvictKeys::Request req;
            P2PEvictKeys::Response resp;
            req.prefix.assign(sprefix);
            int ret = rpc_call<P2PEvictKeys>(node.endpoint(), req, resp);
            if (ret < 0) {
                LOG_ERROR("evict failed, ret:`,error:`,node:`", ret, ERRNO(),
                          node.endpoint());
            }
            photon::thread_usleep(100ul * 1000);
        }
        return 0;
    }

    virtual ssize_t evict(const Net::EndPoint& ep, const char* filename,
                          off_t offset, size_t count) override {
        P2PEvict::Request req;
        P2PEvict::Response resp;
        req.filename.assign(filename);
        req.offset = offset;
        req.count = count;
        auto ret = rpc_call<P2PEvict>(ep, req, resp);
        if (ret < 0) return ret;
        return resp.ret;
    }

    virtual ssize_t pwritev(const char* filename, const struct iovec* iov,
                            int iovcnt, off_t offset) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return pwritev(ep, filename, iov, iovcnt, offset);
            });
    }

    virtual ssize_t pwritev(Net::EndPoint* ep, const char* filename,
                            const struct iovec* iov, int iovcnt,
                            off_t offset) override {
        if (ep == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "node id cannot be NULL");
        }
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }

        P2PWriteV::Request req;
        req.filename.assign(filename);
        req.buf.assign((iovec*)iov, iovcnt);
        req.offset = offset;

        P2PWriteV::Response resp;

        int retry = m_retry_limit + 1;
    again:
        if (--retry < 0) {
            ERRNO err;
            LOG_ERROR_RETURN(0, -err.no, "Retry too many times");
        }
        auto ret = rpc_call<P2PWriteV>(*ep, req, resp);
        if (ret < 0) return ret;
        if (resp.ret < 0) {
            switch (resp.ret) {
                case -EAGAIN:
                    LOG_INFO("Server said AGAIN, try in 2 secs");
                    thread_sleep(2);
                    errno = -resp.ret;
                    goto again;
            }
            auto eno = -resp.ret;
            errno = eno;
        }
        return resp.ret;
    }

    virtual ssize_t announce_latest(const char* filename,
                                    const Net::EndPoint& owner,
                                    uint64_t version) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        return call_along_upstream<ssize_t>(
            filename, [&](Net::EndPoint* ep, bool) {
                return announce_latest(*ep, filename, owner, version);
            });
    }

    virtual ssize_t announce_latest(const Net::EndPoint& node,
                                    const char* filename,
                                    const Net::EndPoint& owner,
                                    uint64_t version) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        P2PAnnounceLatest::Request req;
        req.filename.assign(filename);
        if (owner == Net::EndPoint()) {
            req.owner_ep = m_myid.endpoint();
        } else {
            req.owner_ep = owner;
        }
        req.version = version;
        P2PAnnounceLatest::Response resp;
        auto ret = rpc_call<P2PAnnounceLatest>(node, req, resp);
        if (ret < 0) return ret;
        return resp.ret;
    }

    virtual ssize_t request_latest(const char* filename,
                                   uint64_t& latest_version,
                                   Net::EndPoint& storage_ep) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        return call_along_upstream<ssize_t>(filename, [&](Net::EndPoint* ep,
                                                          bool) {
            return request_latest(*ep, filename, latest_version, storage_ep);
        });
    }

    virtual ssize_t request_latest(const Net::EndPoint& ep,
                                   const char* filename,
                                   uint64_t& latest_version,
                                   Net::EndPoint& storage_ep) override {
        if (filename == nullptr) {
            LOG_ERROR_RETURN(EINVAL, -EINVAL, "filename cannot be NULL");
        }
        P2PReqLatest::Request req;
        req.filename.assign(filename);
        P2PReqLatest::Response resp;
        auto ret = rpc_call<P2PReqLatest>(ep, req, resp);
        if (ret < 0) return ret;
        storage_ep = resp.server_ep;
        latest_version = resp.version;
        return resp.ret;
    }

    virtual IFileSystem* getfs() override { return new ClientFSAdaptor(this); }

    virtual int get_queue_count(Net::EndPoint ep) override {
        auto ref = m_pool->acquire(ep);
        if (ref) {
            DEFER(m_pool->put_stub(ep, false));
            return ref->get_queue_count();
        }
        return 0;
    }

    virtual void set_root_selector(RootSelector* rs) override { m_root = rs; }

    virtual int rename(Net::EndPoint& ep, const char* oldname, const char* newname) override {
        P2PRename::Request req;
        P2PRename::Response resp;
        req.oldname.assign(oldname);
        req.newname.assign(newname);
        int ret = rpc_call<P2PRename>(ep, req, resp);
        if (ret < 0) return ret;
        if (resp.ret < 0) {
            switch (resp.ret) {
                case -EBUSY:
                    LOG_INFO("Server said BUSY, redirect");
                    ep = resp.redirect2.endpoint();
            }
            auto eno = -resp.ret;
            errno = eno;
        }
        return resp.ret;
    }

    virtual int rename(const char* oldname, const char* newname) override {
        return call_along_upstream<int>(
            oldname, [&](Net::EndPoint* ep, bool) {
                return rename(*ep, oldname, newname);
            },
            [&](Net::EndPoint old, Net::EndPoint ep, bool isroot) {
                return true;
            });
    }

    virtual int truncate(const Net::EndPoint& ep, const char* filename, off_t length) override {
        P2PTruncate::Request req;
        P2PTruncate::Response resp;
        req.filename.assign(filename);
        req.length = length;
        int ret = rpc_call<P2PTruncate>(ep, req, resp);
        if (ret < 0) return ret;
        return resp.ret;
    }

    virtual int truncate(const char* filename, off_t length) override {
        return call_along_upstream<int>(
            filename, [&](Net::EndPoint* ep, bool) {
                return truncate(*ep, filename, length);
            });
    }

protected:
    NodeID m_myid;
    RootSelector* m_root;
    ConnectionTracer* m_tracer = nullptr;
    int m_ttl;
    ExpireList<Net::EndPoint> m_blacklist;
    RPC::StubPool* m_pool;
    int m_retry_limit;
    std::string m_domain;
    bool owned_tracer = false;
    IFileSystem* m_checkedfs;
    ObjectCache<std::string, FileMeta*>* m_metas;
    bool m_root_ssl;

    template <typename RET, typename CALL, typename BUSY>
    RET call_along_upstream(const char* filename, CALL call, BUSY busy) {
        // atleast retry once
        int retry = m_retry_limit + 1;
        bool isroot = false;
    again:
        if (--retry < 0) {
            ERRNO err;
            LOG_ERROR_RETURN(0, -err.no, "Too many retry, last error is ", err);
        }
        auto id = m_myid;
        auto node = id.endpoint();
        // LOG_DEBUG("Call to ", node);
        auto ret = call(&node, false);
        if (ret < 0) {
            ERRNO err(-ret);
            if (err.no == ETIMEDOUT || err.no == ENOSYS || err.no == EFAULT ||
                err.no == ECONNREFUSED || err.no == ENOENT ||
                err.no == ECONNRESET) {
                // this may caused by long-session no transfer disconnect
                // first time retry should not mark as failed access
                if (retry == m_retry_limit) {
                    if (retry > 0) photon::thread_usleep(100UL * 1000);
                }
                errno = err.no;
                goto again;
            } else if (err.no == EBUSY) {
                if (busy(id.endpoint(), node, isroot)) {
                    // redirect, consider it is not retry
                    retry++;
                    errno = err.no;
                    goto again;
                }
            }
        }
        return ret;
    }

    template <typename RET, typename CALL>
    RET call_along_upstream(const char* filename, CALL call) {
        return call_along_upstream<RET>(
            filename, call,
            [](Net::EndPoint, Net::EndPoint, bool) { return false; });
    }

    template <typename T>
    int rpc_call(Net::EndPoint ep, typename T::Request& req,
                 typename T::Response& resp) {
        int ret = -1;
        bool immediately = false;
        bool tls = m_root_ssl && m_root->is_root(NodeID(ep));
        WITH_Release(auto stub = m_pool->get_stub(ep, tls),
                     m_pool->put_stub(ep, immediately)) {
            ret = stub->call<T>(req, resp, m_pool->get_timeout());
            if (ret < 0) {
                ERRNO err;
                const char* name = typeid(T).name();
                char* caller =
                    abi::__cxa_demangle(name, nullptr, nullptr, nullptr);
                DEFER(free(caller));
                immediately = (err.no == ECONNRESET);
                LOG_ERROR_RETURN(0, -err.no, "failed to ", caller);
            }
            return ret;
        }
        LOG_ERROR_RETURN(0, -errno, "failed to get stub");
    }
};


class P2PClientFile : public VirtualFile {
public:
    std::string m_filename;
    std::string m_param;
    P2PClient* m_client;

    IFileSystem* filesystem() override { return nullptr; }

    P2PClientFile(const char* filename, P2PClient* client)
        : m_filename(filename), m_client(client) {}

    virtual ~P2PClientFile() override { close(); }

    UNIMPLEMENTED(int fsync() override);
    UNIMPLEMENTED(int fdatasync() override);
    UNIMPLEMENTED(int fchmod(mode_t mode) override);
    UNIMPLEMENTED(int fchown(uid_t owner, gid_t group) override);
    virtual int ftruncate(off_t length) override {
        auto ret = m_client->truncate(m_filename.c_str(), length);
        if (ret < 0) {
            errno = -ret;
            return -1;
        }
        return ret;
    }

    virtual int close() override { return 0; }

    virtual int fstat(struct stat* buf) override {
        memset(buf, 0, sizeof(*buf));
        auto fn = m_param.empty() ? m_filename : m_filename + "?" + m_param;
        auto ret = m_client->fstat(fn.c_str(), buf);
        if (ret < 0) {
            errno = -ret;
            return -1;
        }
        return ret;
    }

    virtual ssize_t preadv(const struct iovec* iov, int iovcnt,
                           off_t offset) override {
        iovector_view view((iovec*)iov, iovcnt);
        if (iovcnt == 0 || view.sum() == 0) return 0;
        auto fn = m_param.empty() ? m_filename : m_filename + "?" + m_param;
        ssize_t ret = m_client->preadv(fn.c_str(), iov, iovcnt, offset);
        if (ret < 0) {
            errno = -ret;
            return -1;
        }
        return ret;
    }

    ssize_t pwritev(const struct iovec* iov, int iovcnt,
                    off_t offset) override {
        iovector_view view((iovec*)iov, iovcnt);
        if (iovcnt == 0 || view.sum() == 0) return 0;
        auto ret = m_client->pwritev(m_filename.c_str(), iov, iovcnt, offset);
        if (ret < 0) {
            errno = -ret;
            return -1;
        }
        return ret;
    }

    int vioctl(int request, va_list arg) override {
        m_param = va_arg(arg, const char*);
        return 0;
    }
};

IFile* new_p2p_client_file(const char* filename, P2PClient* client) {
    return new P2PClientFile(filename, client);
}


P2PClient* new_p2pclient(const NodeID& id, RootSelector* root,
                         int ttl, int retry_time,
                         const char* domain, uint64_t connect_timeout,
                         uint64_t rpc_timeout, IFileSystem* checkedfs,
                         bool root_ssl) {
    return new P2PClientImpl(id, root, 10UL * 1000 * 1000, 60UL * 1000 * 1000,
                             ttl, retry_time, domain, connect_timeout,
                             rpc_timeout, checkedfs, root_ssl);
}

}  // namespace FileSystem
