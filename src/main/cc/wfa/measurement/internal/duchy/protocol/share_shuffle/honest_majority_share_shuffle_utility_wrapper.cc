// Copyright 2024 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility_wrapper.h"

#include <string>

#include "absl/status/statusor.h"
#include "common_cpp/jni/jni_wrap.h"
#include "common_cpp/macros/macros.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"
#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

absl::StatusOr<std::string> CompleteReachAndFrequencyShufflePhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteShufflePhaseRequest, CompleteShufflePhaseResponse>(
      serialized_request, CompleteReachAndFrequencyShufflePhase);
}

absl::StatusOr<std::string> CompleteReachOnlyShufflePhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteShufflePhaseRequest, CompleteShufflePhaseResponse>(
      serialized_request, CompleteReachOnlyShufflePhase);
}

absl::StatusOr<std::string> CompleteReachAndFrequencyAggregationPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteAggregationPhaseRequest,
                 CompleteAggregationPhaseResponse>(
      serialized_request, CompleteReachAndFrequencyAggregationPhase);
}

absl::StatusOr<std::string> CompleteReachOnlyAggregationPhase(
    const std::string& serialized_request) {
  return JniWrap<CompleteAggregationPhaseRequest,
                 CompleteAggregationPhaseResponse>(
      serialized_request, CompleteReachOnlyAggregationPhase);
}

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
