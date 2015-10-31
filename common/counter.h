// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_COUNTER_H_
#define _INDEXFS_COUNTER_H_

#include "common.h"

namespace indexfs {

struct RateCounter {
  uint64_t last_req_ts;
  uint64_t avg_gap;
  uint64_t count;

  RateCounter(uint64_t def_gap) :
    last_req_ts(0), avg_gap(def_gap), count(0) {}

  void AddRequest(uint64_t now) {
    count++;
    if (last_req_ts == 0) {
      last_req_ts = now;
    } else {
      avg_gap = avg_gap * 98 / 100 + (now - last_req_ts) / 50;
      last_req_ts = now;
    }
  }

  uint64_t GetInterval() const {
    return avg_gap;
  }

  uint64_t GetCount() const {
    return count;
  }
};

struct ClientCounter {
  uint64_t bucket_ts;
  uint64_t count;
  uint64_t avg_clients;

  void AddRequest(uint64_t now) {
    if (bucket_ts == 0) {
      bucket_ts = now;
    } else {
      uint64_t gap = now - bucket_ts;
      if (gap > 1000000) {
         for (size_t i = 0; i < gap / 1000000 - 1; ++i) {
            avg_clients = avg_clients * 98 / 100;
         }
         avg_clients = avg_clients * 98 / 100 + count / 50;
         bucket_ts += gap / 1000000 * 1000000;
         count = 0;
      }
      count ++;
    }
  }

  uint64_t GetAvgClients() const {
    return avg_clients;
  }
};

} // indexfs namespace

#endif /* COUNTER_H_ */
