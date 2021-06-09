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

#include <sstream>

#include "StringUtility.h"

namespace FileSystem {

const std::vector<std::string> split(const std::string &input,
                                     const char delim) {
  std::vector<std::string> list;

  std::stringstream iss(input);
  std::string line;
  while (std::getline(iss, line, delim)) {
    list.emplace_back(line);
  }
  return list;
}

const std::pair<std::string, std::string> toKV(const std::string &input,
                                               const char delim) {
  std::pair<std::string, std::string> kv;

  auto pos = input.find_first_of(delim);
  if (pos != std::string::npos) {
    kv.first = input.substr(0, pos);
    kv.second = input.substr(pos + 1);
  }
  return kv;
}

const std::string trim(const std::string &input, const std::string &chars) {
  auto pos = input.find_first_not_of(chars);
  auto rpos = input.find_last_not_of(chars);
  return (pos != std::string::npos && pos <= rpos)
             ? input.substr(pos, rpos + 1 - pos)
             : "";
}

const std::string replace(const std::string &input,
                          const std::string &replaceStr,
                          const std::string &withStr, bool replaceAll) {
  std::stringstream ss;
  std::string::size_type prePos = 0;
  if (replaceStr.size() == 0) {
    return input;
  }
  while (prePos < input.size()) {
    std::string::size_type pos = input.find(replaceStr, prePos);
    if (pos == std::string::npos) {
      break;
    }
    ss << input.substr(prePos, pos - prePos) << withStr;
    prePos = pos + replaceStr.size();
    if (!replaceAll) {
      // replace only once, so break the loop.
      break;
    }
  }
  if (prePos < input.size()) {
    ss << input.substr(prePos);
  }
  return ss.str();
}

}  // namespace zfdistributor
