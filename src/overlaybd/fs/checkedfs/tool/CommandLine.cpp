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
#include <iostream>

#include "CommandLine.h"

using namespace std;

namespace FileSystem {

namespace {

std::string trimSpace(const std::string& input) {
  std::string result;

  size_t curr = 0;

  while (curr < input.size()) {
    if (isspace(input[curr])) {
      curr++;
    } else {
      break;
    }
  }

  while (curr < input.size()) {
    if (isspace(input[curr])) {
      break;
    } else {
      result.push_back(input[curr]);
      curr++;
    }
  }

  return result;
}

}

bool CommandLine::parseInt(const std::string& input, int* value, const std::string& context) {
  try {
    *value = stoi(input);
    return true;
  } catch (std::exception& e) {
    std::cerr << "Failed to parse '" << input << "' to int, for " << context
              << ", due to " << e.what() << '\n';
    return false;
  }
}

bool CommandLine::parseLong(const std::string& input, int64_t* value, const std::string& context) {
  try {
    *value = stoll(input);
    return true;
  } catch (std::exception& e) {
    std::cerr << "Failed to parse '" << input << "' to int64_t, for " << context
              << ", due to " << e.what() << '\n';
    return false;
  }
}

bool CommandLine::parseULong(const std::string& input, uint64_t* value, const std::string& context) {
  try {
    *value = stoull(input);
    return true;
  } catch (std::exception& e) {
    std::cerr << "Failed to parse '" << input << "' to uint64_t, for " << context
              << ", due to " << e.what() << '\n';
    return false;
  }
}

std::vector<std::pair<std::string, int>> CommandLine::parseIpPortList(std::string input) {
  std::vector<std::pair<std::string, int>> result;

  std::string agent;

  //find next agent
  auto pos = input.find(';');

  while (pos!=string::npos) {
    // std::cout << " input :  " << input << std::endl;
    if (pos == 0) {
      std::cerr << " invalid option format " << input << std::endl;
      result.clear();
      return result;
    }
    agent = input.substr(0, pos);
    input = input.substr(pos+1, input.size() - pos - 1);

    //parse single agent
    auto pos2 = agent.find(':');
    if (pos2==string::npos || pos2==0 || pos2==agent.size() - 1) {
      std::cerr << " invalid option format " << input << std::endl;
      result.clear();
      return result;
    }

    auto ip = agent.substr(0, pos2);
    ip = trimSpace(ip);
    auto port = stoi(agent.substr(pos2+1, agent.size() - pos2 - 1));
    result.push_back(std::pair<std::string, int>(ip, port));

    //find next agent
    pos = input.find(';');
  }

  //last agent
  if (input.size() > 0) {
    agent = input;
    auto pos2 = agent.find(':');
    if (pos2==string::npos || pos2==0 || pos2==agent.size() - 1) {
      std::cerr << " invalid option format " << input << std::endl;
      result.clear();
      return result;
    }
    auto ip = agent.substr(0, pos2);
    ip = trimSpace(ip);
    auto port = stoi(agent.substr(pos2+1, agent.size() - pos2 - 1));
    result.push_back(std::pair<std::string, int>(ip, port));
  }

  return result;
};

}
