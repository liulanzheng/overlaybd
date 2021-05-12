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

#include <atomic>
#include <memory>
#include <thread>

namespace Cache {

struct MetricAccumulateItem {
  MetricAccumulateItem();

  void Increase(uint64_t value);
  uint64_t FetchAndClean();
 private:
  std::atomic<uint64_t> accu_;
};

struct MetricAverageItem {
  MetricAverageItem();

  void Increase(uint64_t value);
  uint64_t FetchAndClean();
 private:
  //  40bit as latency, 24bit as counter
  std::atomic<uint64_t> composite_;
};

class Metrics {
 public:
  Metrics();

  MetricAccumulateItem readQps_;
  MetricAccumulateItem writeQps_;

  MetricAverageItem readLatency_;

  MetricAccumulateItem readHit_;

 private:
  void logging();

  std::unique_ptr<std::thread> thread_;
  FILE* pFile_ = nullptr;
};

extern Metrics* gMetric;

}  //  namespace Cache
