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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_HELPER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_HELPER_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "math/distributed_noiser.h"
#include "wfa/frequency_count/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

absl::StatusOr<std::vector<uint32_t>> GenerateReachAndFrequencyNoiseRegisters(
    const ShareShuffleFrequencyVectorParams& frequency_vector_param,
    const math::DistributedNoiser& distributed_reach_noiser,
    const math::DistributedNoiser& distributed_frequency_noiser);

absl::StatusOr<std::vector<uint32_t>> GenerateReachOnlyNoiseRegisters(
    const ShareShuffleFrequencyVectorParams& frequency_vector_param,
    const math::DistributedNoiser& distributed_reach_noiser);

absl::StatusOr<frequency_count::PrngSeed> GetPrngSeedFromString(
    const std::string& seed_str);

absl::StatusOr<frequency_count::PrngSeed> GetPrngSeedFromCharVector(
    const std::vector<unsigned char>& seed_vec);

absl::StatusOr<std::vector<uint32_t>> GenerateShareFromSeed(
    const ShareShuffleFrequencyVectorParams& param,
    const frequency_count::PrngSeed& seed);

absl::StatusOr<std::vector<uint32_t>> GetShareVectorFromFrequencyVectorShare(
    const ShareShuffleFrequencyVectorParams& frequency_vector_params,
    const CompleteShufflePhaseRequest::FrequencyVectorShare&
        frequency_vector_share);

absl::StatusOr<std::vector<uint32_t>> CombineFrequencyVectorShares(
    const ShareShuffleFrequencyVectorParams& frequency_vector_params,
    const google::protobuf::RepeatedPtrField<
        CompleteAggregationPhaseRequest::ShareData>& frequency_vector_shares);

// Returns a vector result where result[i] = X[i] - Y[i] mod modulus.
absl::StatusOr<std::vector<uint32_t>> VectorSubMod(
    const std::vector<uint32_t>& vector_x,
    const std::vector<uint32_t>& vector_y, const uint32_t modulus);

// Returns a vector result where result[i] = X[i] + Y[i] mod modulus.
absl::StatusOr<std::vector<uint32_t>> VectorAddMod(
    absl::Span<const uint32_t> vector_x, absl::Span<const uint32_t> vector_y,
    const uint32_t modulus);

// Estimates reach from the number of non empty registers and the vid sampling
// width.
absl::StatusOr<int64_t> EstimateReach(int64_t non_empty_register_count,
                                      double vid_sampling_interval_width);

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_HELPER_H_
