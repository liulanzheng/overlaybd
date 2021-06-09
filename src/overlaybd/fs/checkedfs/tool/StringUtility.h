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
#include <vector>
#include <utility>

namespace FileSystem {

/** @brief Split the input string at every occurrence
 *  of the given delimiter char.
 *
 *  Given an input string s and a delimiter character ch, return
 *  a vector of substrings which represent the split of s in a
 *  way such that every occurence of ch in s breaks the string
 *  into first half and second half (ch will be excluded). If
 *  no delimiter char ch can be found in s, this function returns
 *  a single element vector in which the original input s is the
 *  only member.
 *
 *  Example: split("a,b,c", ',') => [ "a", "b", "c" ]
 *
 *  @param input The string to be split.
 *  @param delim The delimiter character.
 *  @return the result of split in the form of vector<string>.
 */
const std::vector<std::string> split(const std::string &input,
                                     const char delim);

/** @brief Parse an input string as Key-Value pair.
 *
 *  Given an input string s and a delimiter character ch, return
 *  a pair of strings. The first element in the pair is the key,
 *  while the second element in the pair is the value. ch is the
 *  delimiter character that separates key and value in the input
 *  string s. If ch does not exist in the input string, an empty
 *  pair will be returned.
 *
 *  Example: toKV("key:value", ':') => <"key", "value">
 *
 *  @param input The string to be parsed.
 *  @param delim The delimiter character.
 *  @return the result key-value pair.
 */
const std::pair<std::string, std::string> toKV(const std::string &input,
                                                const char delim);

/** @brief trim characters out of the input string from both ends
 *
 *  Given an input string s and a group of characters chars, trim
 *  all the chars from the begin and end of the string until a character
 *  which is not in the group has been seen.
 *
 *  Example: trim("*_&abcd)_*", ['*', '_', '&']) => "abcd"
 *
 *  @param input The string to be trimmed.
 *  @param chars The characters group
 *  @return the result
 */
const std::string trim(const std::string &input, const std::string &chars);

/** @brief search input string and replace matched substring with new.
 *
 *  @param input The input string.
 *  @param replaceStr The string pattern searched for.
 *  @param withStr The new string to replace the matched string.
 *  @param replaceAll True if all occurrence of <replaceStr> must
 *  be replaced. False otherwise.
 */
const std::string replace(const std::string &input,
                          const std::string &replaceStr,
                          const std::string &withStr,
                          bool replaceAll = false);

}  // namespace zfdistributor
