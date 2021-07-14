// Copyright 2020 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_TIME_STARTED_THREAD_CPU_TIMER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_TIME_STARTED_THREAD_CPU_TIMER_H_

#include "absl/time/time.h"

namespace wfa {

// Utility for determining the CPU duration of the current thread since the
// instance's construction.
//
// This is THREAD HOSTILE: it should not be used from multiple threads.
class StartedThreadCpuTimer {
 public:
  // Builds a `StartedThreadCpuTimer` and marks the time of construction as the
  // basis of comparison for called to `Elapsed` or `ElapsedMillis`.
  StartedThreadCpuTimer();

  // Outputs how much CPU time has elapsed on the current thread since
  // construction.
  absl::Duration Elapsed() const;

  // Outputs how much CPU time, in milliseconds, has elapsed on the current
  // thread since construction.
  int64_t ElapsedMillis() const;

 private:
  const absl::Duration start_;
};

}  // namespace wfa

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_TIME_STARTED_THREAD_CPU_TIMER_H_
