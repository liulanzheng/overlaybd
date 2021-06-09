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

#include <string>

#include "../../../alog.h"

#include "ChecksumType.h"
#include "crc32c.h"

namespace FileSystem {

namespace checksum {

uint8_t ParseChecksumType(const std::string &chksumTypeStr) {
    if (chksumTypeStr == "crc32c") {
        return CRC32C;
    } else if (chksumTypeStr == "crc64_ecma") {
        return CRC64_ECMA;
    } else if (chksumTypeStr == "md5") {
        LOG_ERROR("checksum type <md5> not supported!");
    } else if (chksumTypeStr == "crc64_iso") {
        LOG_ERROR("checksum type <crc64_iso> not supported!");
    } else if (!chksumTypeStr.empty()) {
        LOG_ERROR("unrecognized checksum type: `", chksumTypeStr.c_str());
    }
    return NONE;
}

std::string ChecksumTypeToString(uint8_t type) {
    switch (type) {
    case NONE:
        return "";
    case CRC32C:
        return "crc32c";
    case CRC64_ECMA:
        return "crc64_ecma";
    case CRC64_ISO:
    case MD5:
    default:
        return "unknown";
    }
}

uint64_t ComputeChecksum(const char *data, size_t len, uint64_t crc,
                         uint8_t chksumType) {
    switch (chksumType) {
    case CRC32C:
        return crc32::crc32c_extend(data, len, crc);
    default:
        return 0;
    }
}
} // namespace checksum
} // namespace FileSystem
