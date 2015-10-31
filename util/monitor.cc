// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/monitor.h"

namespace indexfs {

Monitor::~Monitor() {
  delete [] metric_cts_;
  delete [] metric_data_;
  delete [] metric_names_;
}

Monitor::Monitor(const char* (*metrics),
        int num_metrics,
        int server_id,
        time_t window_size) :
        server_id_(server_id),
        num_metrics_(num_metrics + 1),
        window_size_(window_size),
        window_start_(0) {
  metric_names_ = new std::string[num_metrics_];
  for (int i = 0; i < num_metrics; ++i) {
    metric_names_[i] = metrics[i];
  }
  metric_names_[num_metrics_ - 1] = "total";
  metric_data_ = new Histogram[num_metrics_];
  metric_cts_ = new long[num_metrics_];
  memset(metric_cts_, 0, sizeof(long) * num_metrics_);
}

// Send to TSDB the current performance monitoring status,
// which includes the total accumulating counts for each metric (file system
// operation) measured so far,
// as well as the average and maximum latency observed during the current
// monitoring window.
// This method is designed to be called in a dedicated monitoring thread
// not responsible for handling file system requests.
//
void Monitor::GetCurrentStatus(std::stringstream *report) {
  time_t now = time(NULL);
  for (int i = 0; i < num_metrics_; ++i) {
    *report << metric_names_[i] << "_num" << ' '
            << now << ' '
            << metric_cts_[i] << ' '
            << "rank=" << server_id_  << '\n';
    *report << metric_names_[i] << "_max_lat" << ' '
            << now << ' '
            << metric_data_[i].Max() << ' '
            << "rank=" << server_id_  << '\n';
    *report << metric_names_[i] << "_avg_lat" << ' '
            << now << ' '
            << metric_data_[i].Average() << ' '
            << "rank=" << server_id_  << '\n';
  }
  // Resets all latency data if we reach the end of the current monitoring
  // window. For this to work nicely, the frequency of the monitoring
  // thread calling this method should be set to a value that can evenly
  // divide the monitoring window size.
  MaybeClearData(now);
}

} /* namespace */
