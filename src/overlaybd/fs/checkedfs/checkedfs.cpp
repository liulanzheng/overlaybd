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
#include "checkedfs.h"
#include "../../alog.h"
#include "../../alog-stdstring.h"
#include "../../iovector.h"
#include "../aligned-file.h"
#include "../forwardfs.h"
#include "../localfs.h"
#include "../range-split-vi.h"
#include "../range-split.h"
#include "tool/ChecksumType.h"
#include "tool/ChecksumUtil_v1.h"
#include "tool/ChecksumUtil_v2.h"
#include "tool/crc32c.h"
#include "../path.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#define DIR_MODE    S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH
#define FILE_MODE   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH

namespace FileSystem {

IFileMetaFS::IFileMetaFS(IFileSystem *fs, Fn_trans_func func) {
        owner = false;
        if (fs == nullptr) {
            owner = true;
            fs = new_localfs_adaptor();
        }
        m_fs = fs;
        if (func) {
            file_name_trans = func;
        }
    }

IFileMetaFS::~IFileMetaFS() {
    if (owner)
        delete m_fs;
}
class BaseFileMetaImpl : public FileMeta {
public:
    BaseFileMetaImpl() {}
    ~BaseFileMetaImpl() {}

    UNIMPLEMENTED(int update_checksum(const char *buf, size_t count, off_t offset));
    UNIMPLEMENTED(int update_checksum(const struct iovec *iov, int iovcnt, off_t offset));
    UNIMPLEMENTED(int update_file_name(const char *fn));
    UNIMPLEMENTED(int update_size(const ssize_t size));
    UNIMPLEMENTED(int close());

    uint64_t calculate_seg_checksum_and_extract(iovector &iovec) const {
        size_t index = 0, length = 0;
        uint64_t ret = 0;
        while (index < iovec.iovcnt() && length < segSize) {
            auto actural_len = std::min(iovec[index].iov_len, (size_t)(segSize - length));
            ret = checksum::ComputeChecksum((char *)(iovec[index].iov_base), actural_len, ret, checksumType);
            length += actural_len;
            index++;
        }
        iovec.extract_front(length);
        return ret;
    }

    virtual int verify(const char *buf, size_t count, off_t offset) const override {
        struct iovec iov {
            (void *)buf, count
        };
        return verify(&iov, 1, offset);
    };

    virtual int verify(const struct iovec *iov, int iovcnt, off_t offset) const override {
        if (checksumType == checksum::NONE)
            return 0;

        IOVector iovec(iov, iovcnt);
        auto split = range_split(offset, iovec.sum(), segSize);
        if (split.begin_remainder > 0) {
            iovec.extract_front(segSize - split.begin_remainder);
        }
        for (auto& part : split.aligned_parts()) {
            uint64_t cs = calculate_seg_checksum_and_extract(iovec);
            if (part.i < checksum.size() && cs != checksum[part.i]) {
                LOG_WARN("CRC error! part ` checksum `  expect `", part.i, cs, checksum[part.i]);
                return -1;
            }
        }

        return 0;
    }
};

// File Meta V1
// RO
class FileMetaV1Impl : public BaseFileMetaImpl {};

class FileMetaFSV1 : public IFileMetaFS {
public:
    FileMetaFSV1(IFileSystem *fs, Fn_trans_func trans = nullptr) : IFileMetaFS(fs, trans){};

    FileMeta *open(const char *fn) override {
        std::string meta_fn = file_name_trans(fn);
        FileMeta *meta = new FileMetaV1Impl();
        uint64_t filesize = 0;
        int segsize = 0;
        auto ret = parseChecksumFile(m_fs, meta_fn, &(meta->filename), &filesize, &segsize, &(meta->checksumType),
                                     &(meta->checksum));
        if (!ret) {
            delete meta;
            LOG_ERROR_RETURN(0, nullptr, "cannot parse checksum file");
        }
        meta->meta_filename = meta_fn;
        meta->size = filesize;
        meta->segSize = segsize;
        return meta;
    }

    FileMeta *open_directly(const char *fn) override {
        std::string meta_fn = fn;
        FileMeta *meta = new FileMetaV1Impl();
        uint64_t filesize = 0;
        int segsize = 0;
        auto ret = parseChecksumFile(m_fs, meta_fn, &(meta->filename), &filesize, &segsize, &(meta->checksumType),
                                     &(meta->checksum));
        if (!ret) {
            delete meta;
            LOG_ERROR_RETURN(0, nullptr, "cannot parse checksum file");
        }
        meta->meta_filename = meta_fn;
        meta->size = filesize;
        meta->segSize = segsize;
        return meta;
    }

    bool create(IFileSystem *ifs, const char *fn, uint64_t segSize, uint8_t checksumType, bool fill = true) override {
        return generateChecksumFile(ifs, fn, m_fs, file_name_trans(fn), segSize, checksumType);
    }

    UNIMPLEMENTED(int rename(const char *filename, const char *newfilename));
    UNIMPLEMENTED(int unlink(const char *pathname));
};

// File Meta V2
// RW
class FileMetaV2Impl : public BaseFileMetaImpl {
public:
    const static int HEADER_SIZE = 1024;
    const static int META_BODY_START = 1024;
    IFile *file;

    FileMetaV2Impl(IFile *underlay_file) { file = underlay_file; }

    virtual ~FileMetaV2Impl() override {
        if (file)
            delete file;
    }

    virtual int update_checksum(const char *buf, size_t count, off_t offset) {
        struct iovec iov {
            (void *)buf, count
        };
        return update_checksum(&iov, 1, offset);
    }

    virtual int update_checksum(const struct iovec *iov, int iovcnt, off_t offset) {
        if (checksumType == checksum::NONE)
            return 0;

        IOVector iovec(iov, iovcnt);
        auto split = range_split(offset, iovec.sum(), segSize);


        if (split.apend > checksum.size()) {
            photon::scoped_rwlock lock(checksum_lock, photon::WLOCK);
            checksum.resize(split.apend);
        }

        for (auto& part : split.aligned_parts()) {
            uint64_t cs = calculate_seg_checksum_and_extract(iovec);
            checksum[part.i] = cs;
        }

        auto w_size = (split.apend - split.apbegin) * sizeof(uint64_t);
        auto w_offset = META_BODY_START + split.apbegin * sizeof(uint64_t);

        {
            photon::scoped_rwlock lock(checksum_lock, photon::RLOCK);
            ssize_t num = file->pwrite((char *)(checksum.data() + split.apbegin), w_size, w_offset);
            if (num != (ssize_t)w_size) {
                LOG_ERRNO_RETURN(0, -1, "fail to write checksum to file");
            }
        }

        return 0;
    }

    virtual int update_file_name(const char *fn) override {
        filename.assign(fn);
        return flush_header();
    }

    virtual int update_size(const ssize_t sz) override {
        size = sz;
        return flush_header();
    }

    int flush_header() {
        std::string header = make_header_v2(this);
        return file->pwrite(header.data(), HEADER_SIZE, 0);
    }

    virtual int close() override { return file->close(); }
};

class FileMetaFSV2 : public IFileMetaFS {
public:
    FileMetaFSV2(IFileSystem *fs, Fn_trans_func func = nullptr) : IFileMetaFS(fs, func){};

    FileMeta *open(const char *fn) override {
        std::string meta_fn = file_name_trans(fn);
        IFile *meta_file = m_fs->open(meta_fn.c_str(), O_RDWR, 0644);
        if (meta_file == nullptr) {
            LOG_ERRNO_RETURN(0, nullptr, "failed to open meta file ", meta_fn.c_str());
        }

        // meta_file close and delete in FileMetaV2Impl
        FileMeta *meta = new FileMetaV2Impl(meta_file);
        meta->meta_filename = meta_fn;
        int ret = parse_meta_file_v2(meta, meta_file);

        if (!ret) {
            delete meta;
            LOG_ERROR_RETURN(0, nullptr, "cannot parse checksum file");
        }
        return meta;
    }

    FileMeta *open_directly(const char *fn) override {
        LOG_WARN("not implement");
        return nullptr;
    }

    bool create(IFileSystem *fs, const char *fn, size_t segSize, uint8_t checksumType, bool fill) override {
        uint64_t inputLen;
        std::vector<uint64_t> crcVec;
        assert(checksumType == checksum::CRC32C);

        IFile *fd = fs->open(fn, O_RDONLY);
        DEFER({ delete fd; });
        if (!generateChecksum(fd, segSize, checksumType, &inputLen, &crcVec, fill)) {
            return false;
        }

        std::string meta_fn = file_name_trans(fn);
        auto ofd = m_fs->open(meta_fn.c_str(), O_RDWR | O_CREAT, FILE_MODE);
        DEFER({ delete ofd; });
        if (ofd == nullptr) {
            if (errno == ENOENT) {
                // need to build dir
                auto dir_path = Path(meta_fn.data()).dirname();
                auto dir_ret = mkdir_recursive(dir_path, m_fs, DIR_MODE);
                if (dir_ret != 0) {
                    LOG_ERROR_RETURN(0, false, "failed to create dir for meta file: `", meta_fn.c_str());
                }
                ofd = m_fs->open(meta_fn.c_str(), O_RDWR | O_CREAT, FILE_MODE);
            }
            if (ofd == nullptr)
                LOG_ERROR_RETURN(0, false, "failed to open/create file: `", meta_fn.c_str());
        }

        FileMeta *fm = new FileMetaV2Impl(ofd);
        fm->filename = fn;
        fm->size = inputLen;
        fm->segSize = segSize;
        fm->checksumType = checksumType;

        // truncate the file to what's recorded in metadata
        if (ofd->ftruncate(0) < 0) {
            LOG_ERRNO_RETURN(0, false, "failed to truncate file");
        }

        std::string header = make_header_v2(fm);
        ofd->pwrite(header.data(), FileMetaV2Impl::HEADER_SIZE, 0);

        if (fill && crcVec.size() > 0) {
            auto w_size = crcVec.size() * sizeof(uint64_t);
            auto num = ofd->pwrite((char *)crcVec.data(), w_size, FileMetaV2Impl::META_BODY_START);

            if (num < (ssize_t)w_size) {
                LOG_ERROR("failed to write checksum data: `", num);
                return false;
            }
        }

        return true;
    }

    int rename(const char *filename, const char *newfilename) override {
        return m_fs->rename(file_name_trans(filename).c_str(), file_name_trans(newfilename).c_str());
    }

    int unlink(const char *pathname) override { return m_fs->unlink(file_name_trans(pathname).c_str()); }
};

class CheckedFile : public ForwardFile_Ownership {
public:
    FileMeta *m_meta;

    CheckedFile(IFile *file, FileMeta *meta, bool ownership) : ForwardFile_Ownership(file, ownership), m_meta(meta) {}

    ~CheckedFile() {
        if (m_ownership)
            delete m_meta;
    }

    ssize_t pread(void *buf, size_t count, off_t offset) override {
        struct iovec iov {
            buf, count
        };
        return preadv(&iov, 1, offset);
    }

    ssize_t preadv(const struct iovec *iov, int iovcnt, off_t offset) override {
        IOVector iovec(iov, iovcnt);
        return preadv_mutable(iovec.iovec(), iovec.iovcnt(), offset);
    }

    ssize_t preadv_mutable(struct iovec *iov, int iovcnt, off_t offset) override {
        auto ret = m_file->preadv(iov, iovcnt, offset);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "origin file preadv failed");
        }
        if (ret == 0)
            return ret;

        // shrink length of iov view  to read size!!
        iovector_view iovec(iov, iovcnt);
        iovec.shrink_to(ret);

        LOG_DEBUG("pread verify ` at offset ` with count `", m_meta->filename, offset, ret);
        auto v = m_meta->verify(iovec.iov, iovec.iovcnt, offset);
        if (v < 0)
            LOG_ERROR_RETURN(EVERIFY, -1, "verify failed");
        return ret;
    }

    ssize_t read(void *buf, size_t count) override {
        off_t offset = m_file->lseek(0, SEEK_CUR);
        ssize_t ret = pread(buf, count, offset);
        m_file->lseek(ret, SEEK_CUR);
        return ret;
    }

    ssize_t pwrite(const void *buf, size_t count, off_t offset) override {
        struct iovec iov {
            (void *)buf, count
        };
        return pwritev(&iov, 1, offset);
    }

    ssize_t pwritev(const struct iovec *iov, int iovcnt, off_t offset) override {
        iovector_view view((struct iovec *)iov, iovcnt);
        // write must aligned!!
        if (!(offset % m_meta->segSize == 0 && view.sum() % m_meta->segSize == 0)) {
            LOG_ERROR_RETURN(EPERM, -1, "write not aligned");
        }

        auto ret = m_file->pwritev(iov, iovcnt, offset);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "origin file pwritev failed");
        }
        if (ret == 0)
            return ret;

        auto v = m_meta->update_checksum(iov, iovcnt, offset);
        if (v < 0)
            LOG_ERROR_RETURN(EIO, -1, "checksum update failed");

        if (offset + ret > m_meta->size)
            m_meta->update_size(offset + ret);

        return ret;
    }

    virtual ssize_t pwritev_mutable(struct iovec *iov, int iovcnt, off_t offset) override {
        return pwritev(iov, iovcnt, offset);
    }

    ssize_t write(const void *buf, size_t count) override {
        off_t offset = m_file->lseek(0, SEEK_CUR);
        ssize_t ret = pwrite(buf, count, offset);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "write failed");
        }
        m_file->lseek(ret, SEEK_CUR);
        return ret;
    }

    int fstat(struct stat *stat) override {
        auto ret = m_file->fstat(stat);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "origin file fstat failed");
        }
        stat->st_size = m_meta->size;
        stat->st_blksize = m_meta->segSize;
        return 0;
    }

    virtual int ftruncate(off_t length) override {
        auto ret = m_file->ftruncate(length);
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "origin file ftruncate failed");
        }
        return m_meta->update_size(length);
    }

    int close() override {
        auto ret = m_file->close();
        if (ret < 0) {
            LOG_ERROR_RETURN(0, ret, "file close failed");
        }
        return m_meta->close();
    }
};

class CheckedFSAdaptor : public ForwardFS {
public:
    CheckedFSAdaptor(IFileSystem *fs, IFileMetaFS *mfs, size_t def_seg_size, bool build_initial_checksum)
        : ForwardFS(fs), m_metafs(mfs), m_def_seg_size(def_seg_size), m_build_initial_checksum(build_initial_checksum) {
    }

    ~CheckedFSAdaptor() { safe_delete(m_metafs); }

    // open with meta file, if meta not exists, create one if def_seg_size is set
    IFile *open(const char *pathname, int flags, mode_t mode) {
        IFile *file = m_fs->open(pathname, flags, mode);
        if (!file) {
            LOG_ERRNO_RETURN(0, nullptr, "file to open file `", pathname);
        }
        FileMeta *meta = open_or_create_meta(pathname);
        if (!meta) return nullptr;
        return new CheckedFile(file, meta, true);
    }

    IFile *open(const char *pathname, int flags) {
        IFile *file = m_fs->open(pathname, flags);
        if (!file) {
            LOG_ERRNO_RETURN(0, nullptr, "file to open file `", pathname);
        }
        FileMeta *meta = open_or_create_meta(pathname);
        if (!meta) return nullptr;
        return new CheckedFile(file, meta, true);
    }

    virtual int rename(const char *oldname, const char *newname) override {
        auto ret = m_fs->rename(oldname, newname);
        if (ret < 0)
            return ret;
        FileMeta *meta = m_metafs->open(oldname);
        DEFER({ delete meta; });
        if (meta == nullptr) {
            LOG_WARN("not a checkfs file, rename orgin file");
            return ret;
        }
        meta->update_file_name(m_metafs->file_name_trans(newname).c_str());
        meta->close();
        return m_metafs->rename(oldname, newname);
    }

    virtual int stat(const char *path, struct stat *buf) override {
        auto ret = m_fs->stat(path, buf);
        if (ret < 0)
            return ret;
        FileMeta *meta = open_or_create_meta(path);
        DEFER({ delete meta; });

        if (meta == nullptr) {
            LOG_WARN("not a checkfs file, return orgin file stat");
            return ret;
        }

        buf->st_size = meta->size;
        buf->st_blksize = meta->segSize;
        meta->close();
        return ret;
    }

    virtual int unlink(const char *pathname) override {
        auto ret = m_fs->unlink(pathname);
        if (ret < 0)
            return ret;
        auto meta_ret = m_metafs->unlink(pathname);
        if (meta_ret < 0) {
            if (errno == ENOENT) {
                LOG_WARN("not a checkfs file, unlink orgin file");
                return ret;
            }
            return meta_ret;
        }
        return ret;
    }

    FileMeta* get_meta(const char *pathname) {
        return open_or_create_meta(pathname);
    }

protected:
    IFileMetaFS *m_metafs;
    size_t m_def_seg_size;
    bool m_build_initial_checksum;

    FileMeta * open_or_create_meta(const char *pathname) {
        FileMeta *meta = m_metafs->open(pathname);
        if (!meta) {
            if (m_def_seg_size != 0) {
                // create meta if default seg size is set
                bool create_ret =
                    m_metafs->create(m_fs, pathname, m_def_seg_size, checksum::CRC32C, m_build_initial_checksum);
                if (!create_ret) {
                    LOG_ERRNO_RETURN(0, nullptr, "fail to build meta file for `", pathname);
                }
                meta = m_metafs->open(pathname);
            } else {
                // return err
                LOG_ERROR_RETURN(0, nullptr, "failed to open or build meta file for `", pathname);
            }
        }
        return meta;
    }
};

IFileSystem *new_checkedfs_adaptor_v1(IFileSystem *fs, IFileSystem *meta_fs, std::string (*fn_trans)(const char *)) {
    IFileMetaFS *mfs = new FileMetaFSV1(meta_fs, fn_trans);
    return new CheckedFSAdaptor(fs, mfs, 0, true);
}

IFileSystem *new_checkedfs_adaptor_v2(IFileSystem *fs, IFileSystem *meta_fs, std::string (*fn_trans)(const char *), size_t def_seg_size,
                                      bool build_initial_checksum) {
    IFileMetaFS *mfs = new FileMetaFSV2(meta_fs, fn_trans);
    return new CheckedFSAdaptor(fs, mfs, def_seg_size, build_initial_checksum);
}

FileMeta* get_meta_v1(IFileSystem* checkedfs, const char* filename) {
    if (checkedfs == nullptr) {
        LOG_ERROR_RETURN(0, nullptr, "Checked fs not exists");
    }
    auto fs = (CheckedFSAdaptor*)checkedfs;
    return fs->get_meta(filename);
}

IFileMetaFS *new_filemetafs_adaptor_v1(IFileSystem *fs, Fn_trans_func trans) {
    return new FileMetaFSV1(fs, trans);
}

} // namespace FileSystem