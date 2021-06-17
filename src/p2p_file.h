#pragma once
#include <string>
#include "overlaybd/fs/filesystem.h"
#include "overlaybd/fs/forwardfs.h"
#include "overlaybd/alog.h"

namespace FileSystem{

class P2pFileV2 : public ForwardFile
{
public:
    IFileSystem *m_underlayfs = nullptr;
    IFile *m_registryfile = nullptr;
    std::string m_pathname;

    P2pFileV2(IFileSystem *underlayfs, IFile* registryfile, const char *pathname)
        : ForwardFile(nullptr) ,
          m_underlayfs(underlayfs),
          m_registryfile(registryfile),
          m_pathname(pathname)
        {};

    virtual ~P2pFileV2()
    {
        safe_delete(m_registryfile);
    }

    virtual ssize_t pread(void *buf, size_t count, off_t offset) override
    {
        // return m_registryfile->pread(buf, count, offset);
        if (!m_file) {
            char buf[1024]{};
            m_registryfile->pread(buf, 1, 0);
            if (((FileSystem::RegistryFile*)m_registryfile)->getOssUrl(buf, 1024) != 0) {
                LOG_ERRNO_RETURN(EACCES, -1, "get signed oss_url failed: `", m_pathname);
            }
            LOG_INFO("signed url: `", buf);
            auto url = std::string(buf);
            auto p = url.find("data?");
            auto unescape_url = curl_unescape(url.substr(0, p).c_str(), p);
            std::string p2pfs_pathname = "/" + std::string(unescape_url) + url.substr(p);
            m_file = m_underlayfs->open(p2pfs_pathname.c_str(), O_RDONLY);
        }
        return ForwardFile::pread(buf, count, offset);
    }

    virtual int close() override
    {
        m_registryfile->close();
        return ForwardFile::close();
    }
};

class P2pfsV2 : public ForwardFS
{
public:
    IFileSystem *m_registryfs = nullptr;
    P2pfsV2(IFileSystem* fs) : ForwardFS(fs)
    {
    }
    // 打开一个blob链接，对其进行鉴权
    virtual IFile* open(const char *pathname, int flags) override
    {
        auto file = m_registryfs->open(pathname, flags);
        if (file == nullptr) {
            safe_delete(m_registryfs);
            LOG_ERRNO_RETURN(0, nullptr, "open registryfile failed: `", pathname);
        }
        return new P2pFileV2(m_fs, file, pathname);
    }

    virtual ~P2pfsV2()
    {
        safe_delete(m_fs);
    }
};

}


static std::string meta_name_trans_v2(const char *fn) {
    std::string ret(fn);
    std::string head_kw = "/sha256";
    std::string tail_kw = "/data?";
    auto p = ret.find(head_kw);
    auto q = ret.find(tail_kw);
    if (p == std::string::npos || q == std::string::npos) {
        return ret;
    }
    p += 11; // /sha256/xx/
    LOG_DEBUG("` -> sha256:`", fn, ret.substr(p, q - p));
    return "sha256:" + ret.substr(p, q - p);
}