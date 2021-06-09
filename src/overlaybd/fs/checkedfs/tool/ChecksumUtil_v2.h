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
#include "../../../alog.h"
#include "../checkedfs.h"
#include "../../filesystem.h"
#include "ChecksumType.h"
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace FileSystem {

std::string make_header_v2(FileSystem::FileMeta *meta) {
    std::string header(1024, 0);
    memcpy((void *)header.data(), meta->filename.data(), meta->filename.length());
    memcpy((void *)(header.data() + 512), &meta->size, 8);
    memcpy((void *)(header.data() + 512 + 8), &meta->segSize, 8);
    memcpy((void *)(header.data() + 512 + 16), &meta->checksumType, 1);
    return header;
}

void parse_header_v2(const char *buf, FileSystem::FileMeta *meta) {
    meta->filename.assign(buf);
    meta->size = *((ssize_t *)(buf + 512));
    meta->segSize = *((size_t *)(buf + 512 + 8));
    meta->checksumType = *((uint8_t *)(buf + 512 + 16));
}

bool parse_meta_file_v2(FileSystem::FileMeta *meta, FileSystem::IFile *file) {
    char buffer[2048];
    file->read(buffer, 1024);

    parse_header_v2(buffer, meta);

    if (meta->checksumType == checksum::CRC32C) {
        size_t checksumSize = ((meta->size + meta->segSize - 1) / meta->segSize);
        size_t checksumDataLen = checksumSize * sizeof(uint64_t);
        meta->checksum.resize(checksumSize);

        std::vector<char> checksumData(checksumDataLen);
        file->read(checksumData.data(), checksumDataLen);

        for (unsigned int i = 0; i < checksumSize; i++) {
            meta->checksum[i] = (*(reinterpret_cast<uint64_t *>(checksumData.data() + (i * sizeof(uint64_t)))));
        }
    } else {
        LOG_ERROR("unsupported checksum type `", meta->checksumType);
        return false;
    }
    return true;
}

} // namespace FileSystem