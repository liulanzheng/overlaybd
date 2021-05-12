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

#include "metric.h"

#include <gflags/gflags.h>

#include <chrono>

DEFINE_string(stress_metric_log, "/tmp/", "the log of metric");

namespace Cache {

const uint64_t kLogPeriodInSec = 5;
const uint32_t kCounterbits = 24;

Metrics* gMetric = nullptr;

MetricAccumulateItem::MetricAccumulateItem()
    : accu_(0) {
}

void MetricAccumulateItem::Increase(uint64_t value) {
  accu_.fetch_add(value, std::memory_order_relaxed);
}

uint64_t MetricAccumulateItem::FetchAndClean() {
  return accu_.exchange(0, std::memory_order_relaxed);
}

MetricAverageItem::MetricAverageItem()
    : composite_(0) {
}

void MetricAverageItem::Increase(uint64_t value) {
  uint64_t composite = (value << kCounterbits) | 1ull;
  composite_.fetch_add(composite, std::memory_order_relaxed);
}

uint64_t MetricAverageItem::FetchAndClean() {
  uint64_t composite = composite_.exchange(0, std::memory_order_relaxed);
  uint64_t sumOfLatency = composite >> kCounterbits;
  uint64_t count = composite & 0xffffffull;
  return count > 0 ? (sumOfLatency * 1.0 / count) : 0;
}

Metrics::Metrics() {
  pFile_ = fopen(std::string(FLAGS_stress_metric_log + "metric.log").c_str(), "w");
  if (!pFile_) {
    printf("fopen failed, error code : %d\n", errno);
    abort();
  }
  thread_ = std::make_unique<std::thread>([this](){
    while (true) {
      logging();
    }
  });
}

void Metrics::logging() {
  std::this_thread::sleep_for(std::chrono::seconds(kLogPeriodInSec));

  double value = 0;
  value = readLatency_.FetchAndClean();
  if (value) {
    fprintf(pFile_, "Read Latency us : %.1f\n", value / 10.0);
  }

  value = readQps_.FetchAndClean();
  if (value) {
    fprintf(pFile_, "Read Qps : %.1f\n", value / kLogPeriodInSec);
    double hit = readHit_.FetchAndClean();
    if (hit) {
      fprintf(pFile_, "Read Hit Ratio : %.2f\n", hit / value * 100);
    }
  }

  value = writeQps_.FetchAndClean();
  if (value) {
    fprintf(pFile_, "Write Qps : %.1f\n", value / kLogPeriodInSec);
  }

  fflush(pFile_);
}

}  //  namespace Cache
