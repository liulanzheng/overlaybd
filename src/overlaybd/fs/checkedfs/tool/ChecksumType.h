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

#include <string>

namespace FileSystem {

namespace checksum {

enum ChecksumType {
    NONE = 0, // checksum is disabled
    CRC64_ECMA = 1,
    CRC64_ISO = 2,
    MD5 = 3,
    CRC32C = 4
};

uint8_t ParseChecksumType(const std::string &chksumTypeStr);

std::string ChecksumTypeToString(uint8_t chksumType);

uint64_t ComputeChecksum(const char *data, size_t len, uint64_t crc,
                         uint8_t chksumType);
} // namespace checksum
} // namespace FileSystem
