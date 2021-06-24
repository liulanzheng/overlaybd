/*
 *
 * Copyright (C) 2021 Alibaba Group.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING included with this distribution for more details.
 */

#pragma once
#include <string>
#include "overlaybd/fs/filesystem.h"
#include "overlaybd/fs/forwardfs.h"
#include "overlaybd/alog.h"

namespace FileSystem {

class P2pAdaptorFile : public ForwardFile_Ownership {
public:
    IFileSystem *m_underlayfs = nullptr;
    IFileSystem *m_backupfs = nullptr;
    IFile *m_rfile = nullptr;
    std::string m_pathname;

    P2pAdaptorFile(IFileSystem *underlayfs, IFile *rfile, const char *pathname,
                   IFileSystem *backupfs)
        : ForwardFile_Ownership(nullptr, true), m_underlayfs(underlayfs), m_rfile(rfile), m_pathname(pathname),
          m_backupfs(backupfs){};

    virtual ~P2pAdaptorFile() {
        safe_delete(m_rfile);
    }

    int reauth();
    virtual ssize_t pread(void *buf, size_t count, off_t offset) override;
};

class P2pAdaptorFS : public ForwardFS {
public:
    IFileSystem *m_dfs = nullptr; // down grade backup fs
    IFileSystem *m_rfs = nullptr; // registry fs
    P2pAdaptorFS(IFileSystem *fs, IFileSystem *rfs, IFileSystem *dfs)
        : ForwardFS(fs), m_rfs(rfs), m_dfs(dfs) {
    }

    virtual ~P2pAdaptorFS() {
        safe_delete(m_fs);
    }

    virtual IFile *open(const char *pathname, int flags) override {
        auto file = m_rfs->open(pathname, flags);
        if (file == nullptr) {
            LOG_ERRNO_RETURN(0, nullptr, "open remote file failed: `", pathname);
        }
        auto ret = new P2pAdaptorFile(m_fs, file, pathname, m_dfs);
        auto auth_ret = ret->reauth();
        if (auth_ret < 0) {
            delete ret;
            return nullptr;
        }
        return ret;
    }
};

} // namespace FileSystem
