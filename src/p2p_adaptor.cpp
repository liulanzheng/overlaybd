#include <string>
#include <curl/curl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "p2p_adaptor.h"
#include "overlaybd/fs/registryfs/registryfs.h"
#include "overlaybd/alog.h"
#include "overlaybd/alog-stdstring.h"

namespace FileSystem {

int P2pAdaptorFile::reauth() {
    char buf[1024]{};
    m_rfile->pread(buf, 1, 0);
    if (((FileSystem::RegistryFile *)m_rfile)->getUrl(buf, 1024) != 0) {
        LOG_ERRNO_RETURN(EACCES, -1, "get signed oss_url failed: `", m_pathname);
    }

    auto url = std::string(buf);
    auto p = url.find("data?");
    auto unescape_url = curl_unescape(url.substr(0, p).c_str(), p);
    DEFER({ curl_free(unescape_url); });
    std::string p2pfs_pathname = "/" + std::string(unescape_url) + url.substr(p);
    // LOG_INFO("p2p_path:`", p2pfs_pathname);
    m_file = m_underlayfs->open(p2pfs_pathname.c_str(), O_RDONLY);
    if (m_file == nullptr) {
        LOG_ERROR_RETURN(0, -1, "reload p2p auth for ` failed", m_pathname);
    }
    return 0;
}

ssize_t P2pAdaptorFile::pread(void *buf, size_t count, off_t offset) {
    auto ret = ForwardFile::pread(buf, count, offset);
    if (ret < 0) {
        if (errno == EPERM || errno == EACCES) {
            // reauth
            delete m_file;
            reauth();
            return ForwardFile::pread(buf, count, offset);
        }

        // downgrade
        auto backup_file = m_backupfs->open(m_pathname.c_str(), O_RDONLY);
        if (backup_file != nullptr) {
            auto bret = backup_file->pread(buf, count, offset);
            if (bret < 0) {
                // read failed
                delete backup_file;
                LOG_ERROR_RETURN(0, ret, "p2p read failed and read backup file failed for `",
                                 m_pathname);
            }
            // use backup file
            delete m_file;
            m_file = backup_file;
            LOG_INFO("downgrade to registryfs for `", m_pathname);
            return bret;
        } else {
            // open failed
            LOG_ERROR_RETURN(0, ret, "p2p read failed and create backup file failed for `",
                             m_pathname);
        }
    }
    return ret;
}

} // namespace FileSystem