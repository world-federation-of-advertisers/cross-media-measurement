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

#include "absl/status/statusor.h"
#include "math/distributed_noiser.h"
#include "wfa/any_sketch/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

absl::StatusOr<std::vector<uint32_t>> GenerateNoiseRegisters(
    const ShareShuffleSketchParams& sketch_param,
    const math::DistributedNoiser& distributed_noiser);

absl::StatusOr<any_sketch::PrngSeed> GetPrngSeedFromString(
    const std::string& seed_str);

absl::StatusOr<any_sketch::PrngSeed> GetPrngSeedFromCharVector(
    const std::vector<unsigned char>& seed_vec);

absl::StatusOr<std::vector<uint32_t>> GenerateShareFromSeed(
    const ShareShuffleSketchParams& param, const any_sketch::PrngSeed& seed);

absl::StatusOr<std::vector<uint32_t>> GetShareVectorFromSketchShare(
    const ShareShuffleSketchParams& sketch_params,
    const CompleteShufflePhaseRequest::SketchShare& sketch_share);

// Returns a vector result where result[i] = X[i] - Y[i] mod modulus.
absl::StatusOr<std::vector<uint32_t>> VectorSubMod(
    const std::vector<uint32_t>& X, const std::vector<uint32_t>& Y,
    uint32_t modulus);

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_INTERNAL_DUCHY_PROTOCOL_SHARE_SHUFFLE_HONEST_MAJORITY_SHARE_SHUFFLE_UTILITY_HELPER_H_
