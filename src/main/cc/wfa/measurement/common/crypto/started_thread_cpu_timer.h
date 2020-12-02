// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_STARTED_THREAD_CPU_TIMER_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_STARTED_THREAD_CPU_TIMER_H_

#include <time.h>

#include "absl/time/time.h"
#include "glog/logging.h"

namespace wfa::measurement::common::crypto {

namespace {

// Gets the cpu duration of current thread.
absl::Duration GetCurrentThreadCpuDuration() {
#ifdef __linux__
  struct timespec ts;
  CHECK(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0)
      << "Failed to get the thread cpu time.";
  return absl::DurationFromTimespec(ts);
#else
  return absl::ZeroDuration();
#endif
}

}  // namespace

class StartedThreadCpuTimer {
 public:
  StartedThreadCpuTimer() : start_(GetCurrentThreadCpuDuration()) {}
  absl::Duration Elapsed() const {
    return GetCurrentThreadCpuDuration() - start_;
  }
  int64_t ElapsedMillis() const { return absl::ToInt64Milliseconds(Elapsed()); }

 private:
  const absl::Duration start_;
};

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_STARTED_THREAD_CPU_TIMER_H_
