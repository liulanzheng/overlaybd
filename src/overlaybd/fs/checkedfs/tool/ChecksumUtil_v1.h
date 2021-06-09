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
#include "../../filesystem.h"
#include "ChecksumType.h"
#include "CommandLine.h"
#include "StringUtility.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include "../checkedfs.h"

namespace FileSystem {
// for version v1-1 & v1-2
static const char HEADERCOUNT[] = "header-count";
static const char INPUTFILE[] = "input-file-name";
static const char INPUTSIZE[] = "input-file-size";
static const char SEGSIZE[] = "segment-size";

// for version v1-2 only
static const char CHECKSUMMETHOD[] = "checksum-method";

using IFile = FileSystem::IFile;
using IFileSystem = FileSystem::IFileSystem;

static bool calculateSegCRC(IFile *fd, int64_t offset, int count, uint8_t chksumType, uint64_t *crcOut) {
    std::vector<char> s(64 * 1024);
    int num;
    uint64_t crc = 0;
    int readCount = 0;

    do {
        int size = ((count - readCount) < static_cast<int>(s.size())) ? (count - readCount)
                                                                      : static_cast<int>(s.size());
        num = fd->pread(s.data(), size, offset + readCount);
        if (num < 0) {
            LOG_ERRNO_RETURN(0, false, "pread: failed offset=` count=`", offset + readCount, size);
        }

        crc = checksum::ComputeChecksum(s.data(), num, crc, chksumType);

        readCount += num;
    } while ((unsigned int)num != 0 && (readCount != count));

    if (readCount != count) {
        return false;
    }

    *crcOut = crc;
    return true;
}

bool generateChecksum(IFile *file, int segSize, uint8_t chksumType, uint64_t *inputLenResult,
                      std::vector<uint64_t> *crcVec, bool fill) {
    crcVec->clear();

    struct stat s;
    if (file->fstat(&s) != 0) {
        LOG_ERROR_RETURN(0, false, "failed to get file stat: ");
    }

    if (segSize <= 0) {
        LOG_ERROR_RETURN(0, false, "invalid segSize: ", segSize);
    }

    int64_t inputLen = s.st_size;
    *inputLenResult = inputLen;

    if (fill) {
        assert(segSize > 0);
        int segNum = (inputLen % segSize == 0) ? (inputLen / segSize) : ((inputLen / segSize) + 1);

        for (int64_t i = 0; i < segNum; i++) {
            int count = segSize;
            if ((i == segNum - 1) && ((inputLen % segSize) != 0)) {
                count = inputLen % segSize;
            }

            uint64_t out;
            if (!calculateSegCRC(file, i * segSize, count, chksumType, &out)) {
                return false;
            }

            crcVec->push_back(out);
        }
    }

    return true;
}


static bool writeHeader(IFile *ofd, const std::string &input, int64_t inputLen, int segSize, uint8_t chksumType) {
    std::ostringstream ss;
    ss << HEADERCOUNT << ":4" << std::endl
       << INPUTFILE << ":" << input << std::endl
       << INPUTSIZE << ":" << inputLen << std::endl
       << SEGSIZE << ":" << segSize << std::endl
       << CHECKSUMMETHOD << ":" << checksum::ChecksumTypeToString(chksumType) << std::endl;

    int num = ofd->write(ss.str().data(), ss.str().size());

    if (num < static_cast<int>(ss.str().size())) {
        LOG_ERROR_RETURN(0, false, "failed to write headers: `", num);
    }

    return true;
}

bool generateChecksumFile(IFileSystem *infs, const std::string &input, IFileSystem *outfs, const std::string &output,
                          int segSize, uint8_t chksumType) {
    uint64_t inputLen;
    std::vector<uint64_t> crcVec;

    // only generate CRC32C checksum files
    // verification path recognizes the older checksum type in order to be backward compatible
    assert(chksumType == checksum::CRC32C);
    auto ifd = infs->open(input.c_str(), O_RDONLY);
    if (!generateChecksum(ifd, segSize, chksumType, &inputLen, &crcVec, true)) {
        return false;
    }

    mode_t mode = 0;
    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH; // 0644 & process ~umask

    auto ofd = outfs->open(output.c_str(), O_RDWR | O_CREAT, mode);
    DEFER({ delete ofd; });
    if (ofd == nullptr) {
        LOG_ERROR_RETURN(0, false, "failed to open file for write: `", output.c_str());
    }

    // truncate the file to what's recorded in metadata
    if (ofd->ftruncate(0) < 0) {
        LOG_ERRNO_RETURN(0, false, "truncate file");
    }

    if (!writeHeader(ofd, input, inputLen, segSize, chksumType)) {
        return false;
    }

    std::vector<char> outputData(crcVec.size() * sizeof(uint32_t));

    for (unsigned int i = 0; i < crcVec.size(); i++) {
        *(reinterpret_cast<uint32_t *>(outputData.data() + (i * sizeof(uint32_t)))) = htonl((uint32_t)crcVec[i]);
    }

    int num = ofd->write(outputData.data(), outputData.size());

    if (num < static_cast<int>(outputData.size())) {
        LOG_ERROR("failed to write checksum data: `", num);
        return false;
    }

    return true;
}

static bool getKV(const std::string &in, std::string *key, std::string *value) {
    std::size_t found = in.find(":");
    if (found == std::string::npos) {
        return false;
    }

    *key = in.substr(0, found);
    *value = in.substr(found + 1);

    return true;
}

void getline(IFile *f, std::string &line, ssize_t buffsize = 4096) {
    char buffer[buffsize];
    auto offset = f->lseek(0, SEEK_CUR);
    line = "";
    DEFER({ f->lseek(offset + line.length() + 1, SEEK_SET); });
    for (;;) {
        auto ret = f->read(buffer, buffsize);
        if (ret < 0)
            return;
        for (int i = 0; i < ret; i++) {
            if (buffer[i] == '\n' || buffer[i] == '\0') {
                line.append(buffer, i);
                return;
            }
        }
        line.append(buffer, ret);
        if (ret < buffsize)
            return;
    }
}

bool parseChecksumFile(IFileSystem *fs, const std::string &checksumFile, std::string *fileName, uint64_t *size,
                       int *segSizeOut, uint8_t *chksumType, std::vector<uint64_t> *crcVec) {
    crcVec->clear();
    auto ifs = fs->open(checksumFile.c_str(), O_RDONLY);
    DEFER({ delete ifs; });
    if (!ifs) {
        LOG_ERROR_RETURN(0, false, "failed to open file for read: `", checksumFile.c_str());
    }

    std::string line;
    getline(ifs, line);

    std::string key, value;

    if (!getKV(line, &key, &value)) {
        return false;
    }

    if (key != HEADERCOUNT) {
        return false;
    }

    int headerNum;

    if (!CommandLine::parseInt(value, &headerNum, "")) {
        return false;
    }

    uint64_t fileSize = -1;
    int32_t segSize = 0;
    uint8_t checksumType = checksum::CRC64_ECMA;

    for (int i = 0; i < headerNum; i++) {
        getline(ifs, line);
        if (!getKV(line, &key, &value)) {
            return false;
        }

        if (key == INPUTFILE) {
            *fileName = value;
        } else if (key == INPUTSIZE) {
            if (!CommandLine::parseULong(value, &fileSize, "")) {
                return false;
            }
        } else if (key == SEGSIZE) {
            if (!CommandLine::parseInt(value, &segSize, "")) {
                return false;
            }
        } else if (key == CHECKSUMMETHOD) {
            checksumType = checksum::ParseChecksumType(trim(value, " \t"));
        }
    }

    *size = fileSize;
    *segSizeOut = segSize;
    *chksumType = checksumType;

    if (checksumType == checksum::CRC64_ECMA) {
        // Checksum data is stored in text format
        while (1) {
            uint64_t crc;
            getline(ifs, line);
            if (line.size() == 0) {
                break;
            }

            if (!CommandLine::parseULong(line, &crc, "")) {
                return false;
            }

            crcVec->push_back(crc);
        }
    } else if (checksumType == checksum::CRC32C) {
        // Checksum data is stored in binary format
        int checksumDataLen = ((fileSize + segSize - 1) / segSize) * sizeof(uint32_t);
        std::vector<char> checksumData(checksumDataLen);
        ifs->read(checksumData.data(), checksumDataLen);

        if (!ifs) {
            return false;
        }

        for (unsigned int i = 0; i < checksumDataLen / sizeof(uint32_t); i++) {
            crcVec->push_back(ntohl(*(reinterpret_cast<uint32_t *>(checksumData.data() + (i * sizeof(uint32_t))))));
        }
    } else {
        LOG_ERROR("unsupported checksum type ", checksumType);
        return false;
    }

    return true;
}

} // namespace FileSystem