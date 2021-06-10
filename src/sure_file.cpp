/*
   Copyright The Overlaybd Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#include <limits.h>
#include <unistd.h>
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/alog.h"
#include "overlaybd/fs/localfs.h"
#include "overlaybd/photon/thread.h"
#include "image_file.h"
#include "sure_file.h"

namespace FileSystem {
class SureFile : public ForwardFile_Ownership {
public:
    SureFile() = delete;
    SureFile(IFile *src_file, ImageFile *image_file, bool ownership)
        : ForwardFile_Ownership(src_file, ownership), m_ifile(image_file) {
    }

private:
    ImageFile *m_ifile = nullptr;

    void io_sleep(uint64_t &try_cnt) {
        if (try_cnt < 10)
            photon::thread_usleep(500); // 500us
        else
            photon::thread_usleep(2000); // 2ms

        if (try_cnt > 30000) // >1min
            photon::thread_sleep(1); // 1sec
    }


public:

    virtual ssize_t pread(void *buf, size_t count, off_t offset) override {
        uint64_t try_cnt = 0;
        size_t got_cnt = 0;
        auto time_st = photon::now;
        while (m_ifile && m_ifile->m_status >= 0 && photon::now - time_st < 31 * 1000 * 1000) {
            // exit on image in exit status, or timeout
            ssize_t ret = m_file->pread((char *)buf + got_cnt, count - got_cnt, offset + got_cnt);
            if (ret > 0)
                got_cnt += ret;
            if (got_cnt == count)
                return count;

            if ((ret < 0) && (!m_ifile->m_status < 1) && (errno == EPERM)) {
                // exit when booting. after boot, hang.
                m_ifile->set_auth_failed();
                LOG_ERROR_RETURN(0, -1, "authentication failed during image boot.");
            }

            if (got_cnt > count) {
                LOG_ERROR("pread(,`,`) return `. got_cnt:` > count:`, restart pread.", count,
                          offset, ret, got_cnt, count);
                got_cnt = 0;
            }

            io_sleep(try_cnt);
            try_cnt++;

            if (try_cnt % 300 == 0) {
                LOG_ERROR("pread read partial data. count:`, offset:`, ret:`, got_cnt:`, errno:`",
                          count, offset, ret, got_cnt, errno);
            }
        }
        return -1;
    }
};
} // namespace FileSystem

FileSystem::IFile *new_sure_file(FileSystem::IFile *src_file, ImageFile *image_file,
                                 bool ownership) {
    if (!src_file) {
        LOG_ERROR("failed to new_sure_file(null)");
        return nullptr;
    }
    return new FileSystem::SureFile(src_file, image_file, ownership);
}

FileSystem::IFile *new_sure_file_by_path(const char *file_path, int open_flags,
                                         ImageFile *image_file, bool ownership) {
    auto file = FileSystem::open_localfile_adaptor(file_path, open_flags, 0644, 0);
    return new_sure_file(file, image_file, ownership);
}