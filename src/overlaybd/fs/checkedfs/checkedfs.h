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
#pragma once
#include "../../object.h"
#include "../filesystem.h"
#include "../../photon/thread.h"
#include <string>
#include <vector>
#include "errno.h"

#define EVERIFY EUCLEAN

namespace FileSystem {

class FileMeta : public Object {
public:
    std::string meta_filename;
    std::string filename;
    ssize_t size;         // -1 mean unknown
    size_t segSize;       // segment size (checksum unit size), 0 means unknown
    uint8_t checksumType; // 0 means no checksum？
    std::vector<uint64_t> checksum;
    photon::rwlock checksum_lock;

    // verify segments checksum
    // maybe not aligned; do not verify for header and tail reminder
    virtual int verify(const struct iovec *iov, int iovcnt, off_t offset) const = 0;
    virtual int verify(const char *buf, size_t count, off_t offset) const = 0;

    // update segments checksum; must be aligned
    virtual int update_checksum(const char *buf, size_t count, off_t offset) = 0;
    virtual int update_checksum(const struct iovec *iov, int iovcnt, off_t offset) = 0;

    virtual int update_file_name(const char *fn) = 0;
    virtual int update_size(const ssize_t size) = 0;

    // close overlay file if needed
    virtual int close() = 0;

protected:
    FileMeta() {}
};

typedef std::string (*Fn_trans_func)(const char *);

// An FS liked Object for file meta
class IFileMetaFS : public Object {
public:
    IFileMetaFS(IFileSystem *fs, Fn_trans_func func);
    ~IFileMetaFS();
    // by default, use same name as meta file name, should use different fs
    Fn_trans_func file_name_trans = &same_name_trans;

    // input `filename` is the origin filename, will transfer to meta fn by get_meta_filename
    virtual FileMeta *open(const char *filename) = 0;
    virtual FileMeta *open_directly(const char *filename) = 0;
    virtual bool create(IFileSystem *fs, const char *fn, size_t segSize, uint8_t checksumType, bool fill) = 0;
    virtual int rename(const char *filename, const char *newfilename) = 0;
    virtual int unlink(const char *pathname) = 0;

protected:
    IFileSystem *m_fs;
    bool owner;

    static std::string same_name_trans(const char *filename) { return filename; }
};

// adaptor for checkedfs with an underlayfs `fs` and an fs for meta file `meta_fs`
// `fn_trans` is used to transfer a filename to its' meta filename, default use the same name
IFileSystem *new_checkedfs_adaptor_v1(IFileSystem *fs, IFileSystem *meta_fs,
                                      Fn_trans_func fn_trans = nullptr);

// `def_seg_size` is default segment size to split the file for calculating checksum
// calculate file checksum when create a meta, if `build_initial_checksum` is true. otherwise update checksums when
// writing to file. update or verify has to be aligned to segsize!!!
IFileSystem *new_checkedfs_adaptor_v2(IFileSystem *fs, IFileSystem *meta_fs,
                                      Fn_trans_func fn_trans = nullptr,
                                      size_t def_seg_size = 0,
                                      bool build_initial_checksum = false);

FileMeta* get_meta_v1(IFileSystem* checkedfs, const char* filename);

IFileMetaFS *new_filemetafs_adaptor_v1(IFileSystem *fs, Fn_trans_func trans = nullptr);

}; // namespace FileSystem