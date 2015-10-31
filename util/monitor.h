// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_UTIL_MEASURE_H_
#define _INDEXFS_UTIL_MEASURE_H_

#include <time.h>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>

#include "common/logging.h"
#include "leveldb/env.h"
#include "leveldb/util/histogram.h"

namespace indexfs {

using leveldb::Env;
using leveldb::Histogram;

class Monitor {
 public:

  Monitor(const char* (*metrics),
          int num_metrics,
          int server_id = -1,
          time_t window_size = 10);

  ~Monitor();

  long TEST_GetCount(int metric_index) {
    return metric_cts_[metric_index];
  }
  long TEST_GetTotalCount() {
    return metric_cts_[num_metrics_ - 1];
  }
  double TEST_GetMaxLatency(int metric_index) {
    return metric_data_[metric_index].Max();
  }

  void AddMetric(int metric_index, double latency);
  void GetCurrentStatus(std::stringstream *report);

 private:
  void MaybeClearData(time_t now);
  // No copying allowed
  Monitor(const Monitor&);
  Monitor& operator=(const Monitor&);

  int server_id_;
  int num_metrics_;

  time_t window_size_;
  time_t window_start_;

  long* metric_cts_;
  Histogram* metric_data_;
  std::string* metric_names_;
};

inline void Monitor::MaybeClearData(time_t now) {
  if (now - window_start_ >= window_size_) {
    for (int i = 0; i < num_metrics_; ++i) {
      metric_data_[i].Clear();
    }
    window_start_ = now;
  }
}

inline void Monitor::AddMetric(int metric_index, double latency) {
  DLOG_ASSERT(metric_index >= 0);
  DLOG_ASSERT(metric_index < num_metrics_ - 1);

  metric_cts_[metric_index]++;
  metric_cts_[num_metrics_ - 1]++;

  metric_data_[metric_index].Add(latency);
  metric_data_[num_metrics_ - 1].Add(latency);
}

// -------------------------------------------------------------
// Monitor Helper
// -------------------------------------------------------------

class MonitorHelper {
  uint64_t start_;
  int metric_index_;
  Monitor* monitor_;

 public:
  MonitorHelper(int metric_index,
                Monitor* monitor) :
                metric_index_(metric_index),
                monitor_(monitor) {
    start_ = Env::Default()->NowMicros();
  }

  ~MonitorHelper() {
    uint64_t latency_ = Env::Default()->NowMicros() - start_;
    monitor_->AddMetric(metric_index_, static_cast<double>(latency_));
  }
};

} /* namespace indexfs */

#endif /* _INDEXFS_UTIL_MEASURE_H_ */
