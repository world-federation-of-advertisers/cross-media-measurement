// Copyright 2021 The Cross-Media Measurement Authors
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

#include "wfa/measurement/common/time/started_thread_cpu_timer.h"

#include <ctime>

#include "absl/base/macros.h"
#include "absl/time/time.h"

namespace wfa {
namespace {
// Gets the cpu duration of current thread.
absl::Duration GetCurrentThreadCpuDuration() {
#ifdef __linux__
  struct timespec ts;
  ABSL_ASSERT(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0);
  return absl::DurationFromTimespec(ts);
#else
  return absl::ZeroDuration();
#endif
}
}  // namespace

StartedThreadCpuTimer::StartedThreadCpuTimer()
    : start_(GetCurrentThreadCpuDuration()) {}

absl::Duration StartedThreadCpuTimer::Elapsed() const {
  return GetCurrentThreadCpuDuration() - start_;
}

int64_t StartedThreadCpuTimer::ElapsedMillis() const {
  return absl::ToInt64Milliseconds(Elapsed());
}

}  // namespace wfa
