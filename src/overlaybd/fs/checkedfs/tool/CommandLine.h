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
#include <utility>
#include <vector>

namespace FileSystem {

// Utility class that hosts command line parsing related help functions
class CommandLine {
 public:
  static bool parseInt(const std::string& input, int* value, const std::string& context);
  static bool parseLong(const std::string& input, int64_t* value, const std::string& context);
  static bool parseULong(const std::string& input, uint64_t* value, const std::string& context);
  static std::vector<std::pair<std::string, int>> parseIpPortList(std::string input);
};

}
