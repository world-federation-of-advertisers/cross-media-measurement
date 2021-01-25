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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_MATH_DISTRIBUTIONS_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_MATH_DISTRIBUTIONS_H_

#include "absl/random/random.h"
#include "absl/status/statusor.h"

namespace wfa::measurement::common::math {

struct DistributedGeometricRandomComponentOptions {
  // The number of contributors to the global random variable.
  int64_t num;
  // The p (success ratio) parameter of the polya distribution. 0<p<1.
  double p;
  // The threshold to truncate the polya random variables. A negative value
  // indicates no truncation.
  int64_t truncate_threshold = -1;
  // The offset added to the PolyaDiff. Usually greater than the
  // truncate_threshold such that the final result is positive.
  int64_t shift_offset = 0;
};

// Gets one component of a truncatedShiftedTwoSidedGeometricDistributed random
// variable using the decentralized mechanism, i.e., a truncated shifted
// PolyaDiff.
absl::StatusOr<int64_t> GetDistributedGeometricRandomComponent(
    DistributedGeometricRandomComponentOptions options);

}  // namespace wfa::measurement::common::math

#endif  // WFA_MEASUREMENT_COMMON_MATH_POLYA_DISTRIBUTION_H_
