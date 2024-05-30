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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

using ::wfa::measurement::internal::duchy::protocol::
    CompleteShufflePhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteShufflePhaseResponse;

absl::StatusOr<CompleteShufflePhaseResponse>
CompleteReachAndFrequencyShufflePhase(
    const CompleteShufflePhaseRequest& request);

absl::StatusOr<CompleteShufflePhaseResponse> CompleteReachOnlyShufflePhase(
    const CompleteShufflePhaseRequest& request);

absl::StatusOr<CompleteAggregationPhaseResponse>
CompleteReachAndFrequencyAggregationPhase(
    const CompleteAggregationPhaseRequest& request);

absl::StatusOr<CompleteAggregationPhaseResponse>
CompleteReachOnlyAggregationPhase(
    const CompleteAggregationPhaseRequest& request);

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_H_
