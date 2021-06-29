#include "p2pfs.h"
#include <fcntl.h>
#include <stdlib.h>

#include <string>

#include "../../alog-stdstring.h"
#include "../../alog.h"
#include "../../expirecontainer.h"
#include "../cache/cache.h"
#include "../checkedfs/checkedfs.h"
#include "../forwardfs.h"
#include "../localfs.h"
#include "../path.h"
#include "../subfs.h"
#include "../virtual-file.h"
#include "client.h"

using namespace std;

namespace FileSystem {
class P2PFileSystem;

class P2PFile : public ForwardFile {
public:
    P2PFile(IFile *ref, const std::string &filename, P2PFileSystem *fs);
    ~P2PFile();
    int close() override;

protected:
    P2PFileSystem *m_fs = nullptr;
    std::string m_filename;
};

class P2PFileSystem : public IFileSystem {
public:
    bool m_client_ownership;
    P2PClient *m_client;
    IFileSystem *m_srcfs;

    ObjectCache<std::string, IFile *> *m_refs;

    UNIMPLEMENTED_POINTER(IFile *creat(const char *, mode_t) override);
    UNIMPLEMENTED(int mkdir(const char *, mode_t) override);
    UNIMPLEMENTED(int rmdir(const char *) override);
    UNIMPLEMENTED(int link(const char *, const char *) override);
    UNIMPLEMENTED(int symlink(const char *, const char *) override);
    UNIMPLEMENTED(ssize_t readlink(const char *, char *, size_t) override);
    UNIMPLEMENTED(int chmod(const char *, mode_t) override);
    UNIMPLEMENTED(int chown(const char *, uid_t, gid_t) override);
    UNIMPLEMENTED(int statfs(const char *path, struct statfs *buf) override);
    UNIMPLEMENTED(int statvfs(const char *path, struct statvfs *buf) override);
    UNIMPLEMENTED(int access(const char *pathname, int mode) override);
    UNIMPLEMENTED(int syncfs() override);
    UNIMPLEMENTED(int unlink(const char *filename) override);
    UNIMPLEMENTED(int lchown(const char *pathname, uid_t owner, gid_t group) override);
    UNIMPLEMENTED_POINTER(DIR *opendir(const char *) override);

    P2PFileSystem(P2PClient *client, bool client_ownership = false,
                  uint64_t refresh_timer = 10UL * 1000 * 1000)
        : m_client_ownership(client_ownership), m_client(client) {
        m_srcfs = client->getfs();
        m_refs = new ObjectCache<std::string, IFile *>(refresh_timer);
    }

    virtual ~P2PFileSystem() override {
        delete m_refs;
        delete m_srcfs;
        if (m_client_ownership)
            delete m_client;
    }

    void release(const std::string &filename) {
        m_refs->release(filename);
    }

    virtual IFile *open(const char *pathname, int flags) override {
        auto ctor = [&]() { return m_srcfs->open(pathname, flags); };
        IFile *ref = m_refs->acquire(pathname, ctor);
        if (ref == nullptr)
            LOG_ERROR_RETURN(0, nullptr, "Failed to acquire file with ", VALUE(pathname));
        return (IFile *)new P2PFile(ref, pathname, this);
    }

    virtual IFile *open(const char *filename, int flags, mode_t mode) override {
        return open(filename, flags); // mode will be ignored
    }

    int lstat(const char *path, struct stat *buf) override {
        return stat(path, buf);
    }

    virtual int stat(const char *path, struct stat *buf) override {
        auto ret = m_srcfs->stat(path, buf);
        if (ret < 0) {
            // if m_src is not like mefafile fs
            // the stat request may need trasfer via p2p upstream
            ret = m_client->fstat(path, buf);
        }
        return ret;
    }

    virtual int rename(const char *oldname, const char *newname) override {
        return m_client->rename(oldname, newname);
    }

    int truncate(const char *path, off_t length) override {
        return m_client->truncate(path, length);
    }
};

P2PFile::P2PFile(IFile *ref, const std::string &filename, P2PFileSystem *fs)
    : ForwardFile(ref), m_fs(fs), m_filename(filename) {
}

P2PFile::~P2PFile() {
    close();
}

int P2PFile::close() {
    if (m_fs) {
        auto fs = m_fs;
        m_fs = nullptr;
        fs->release(m_filename);
    }
    return 0;
}

static std::string transform(const char *path) {
    // since checked fs usually cannot keep folder structure
    // here is the Fn_trans_func, keep only basename
    // seperate parameters firstly
    std::string fn(path);
    fn = fn.substr(0, fn.find('?'));
    auto basename = strrchr(fn.data(), '/');
    if (basename) {
        if (strcmp(basename, "/data") == 0) {
            fn = fn.substr(0, basename - fn.data());
            // case is httpfs formed
            return "/sha256:" + fn.substr(fn.rfind('/') + 1);
        } else
            return basename;
    }
    return path;
}


IFileSystem* new_p2pfs(const NodeID& root, const NodeID& myid,
                       IFileSystem* metafs, ConnectionTracer* tracer,
                       bool balanced, int ttl, int retry_time,
                       const char* domain, uint64_t connect_timeout,
                       uint64_t rpc_timeout, bool root_ssl) {
    if (myid.addr == 0 && myid.port == 0) {
        if (!balanced) {
            IFileSystem* checkedfs = nullptr;
            if (metafs) checkedfs = new_checkedfs_adaptor_v1(nullptr, metafs, transform);
            P2PClient* p2pClient =
                new_p2pclient(myid, root, ttl, retry_time,
                              domain, connect_timeout, rpc_timeout, checkedfs, root_ssl);
            IFileSystem* p2pfs = new P2PFileSystem(p2pClient, true);
            return p2pfs;
        }
    }
    LOG_ERROR_RETURN(0, nullptr, "unsupported p2pfs params");

}

} // namespace FileSystem
