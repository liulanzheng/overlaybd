#include "common_func.h"
#include <sys/fcntl.h>
#include <unistd.h>
#include "overlaybd/photon/thread.h"
#include "overlaybd/alog.h"
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/fs/filesystem.h"
#include "overlaybd/fs/localfs.h"

namespace CommonFunc {

std::string trim(const std::string &str) {
    std::string s = str;
    if (s.empty()) {
        return s;
    }

    s.erase(0, s.find_first_not_of(' '));
    s.erase(s.find_last_not_of('\n') + 1);
    s.erase(s.find_last_not_of(' ') + 1);
    return s;
}

std::string get_file_content(const std::string &file_path) {
    std::string ret;
    FileSystem::IFileSystem *lfs = nullptr;
    FileSystem::IFile *file = nullptr;
    char buf[4096] = {0};

    lfs = FileSystem::new_localfs_adaptor(nullptr, 2);
    if (!lfs) {
        LOG_ERROR_RETURN(0, "", "FileSystem::new_localfs_adaptor() return NULL");
    }
    DEFER(delete lfs);

    if (lfs->access(file_path.c_str(), F_OK) != 0) {
        LOG_ERROR_RETURN(0, "", "` doesn't exist!", file_path);
    }

    file = lfs->open(file_path.c_str(), O_RDONLY);
    if (!file) {
        LOG_ERROR_RETURN(0, "", "open(`, O_RDONLY). errno:`,` ", file_path, errno, strerror(errno));
    }
    DEFER(delete file);

    if (file->read(buf, sizeof(buf)) < 0) {
        LOG_ERROR_RETURN(0, "", "file_path:`, read error. errno:`,` ", file_path, errno,
                         strerror(errno));
    }
    ret = buf;
    ret = trim(ret);
    return ret;
}

} // namespace CommonFunc