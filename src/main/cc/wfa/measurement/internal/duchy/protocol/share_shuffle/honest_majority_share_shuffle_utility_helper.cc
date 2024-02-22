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

#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility_helper.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "any_sketch/crypto/shuffle.h"
#include "common_cpp/macros/macros.h"
#include "common_cpp/time/started_thread_cpu_timer.h"
#include "math/distributed_noiser.h"
#include "math/open_ssl_uniform_random_generator.h"
#include "wfa/any_sketch/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

using ::wfa::any_sketch::PrngSeed;
using ::wfa::math::CreatePrngFromSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;
using ::wfa::measurement::common::crypto::SecureShuffleWithSeed;

absl::StatusOr<std::vector<uint32_t>> GenerateNoiseRegisters(
    const ShareShuffleSketchParams& sketch_param,
    const math::DistributedNoiser& distributed_noiser) {
  if (sketch_param.ring_modulus() <=
      sketch_param.maximum_combined_frequency() + 1) {
    return absl::InvalidArgumentError(
        "Ring modulus must be greater than maximum combined frequency plus 1.");
  }

  int64_t total_noise_registers_count =
      distributed_noiser.options().shift_offset * 2 *
      (1 + sketch_param.maximum_combined_frequency());
  // Sets all noise registers to the sentinel value (q-1).
  std::vector<uint32_t> noise_registers(total_noise_registers_count,
                                        sketch_param.ring_modulus() - 1);

  int current_index = 0;
  for (int k = 0; k <= sketch_param.maximum_combined_frequency(); k++) {
    ASSIGN_OR_RETURN(int64_t noise_register_count_for_bucket_k,
                     distributed_noiser.GenerateNoiseComponent());
    for (int i = 0; i < noise_register_count_for_bucket_k; i++) {
      noise_registers[current_index] = k;
      current_index++;
    }
  }

  return noise_registers;
}

absl::StatusOr<PrngSeed> GetPrngSeedFromString(const std::string& seed_str) {
  if (seed_str.length() != (kBytesPerAes256Key + kBytesPerAes256Iv)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The seed string has length $0 bytes, however, $1 bytes are required.",
        seed_str.size(), (kBytesPerAes256Key + kBytesPerAes256Iv)));
  }
  PrngSeed seed;
  *seed.mutable_key() = seed_str.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = seed_str.substr(kBytesPerAes256Key, kBytesPerAes256Iv);
  return seed;
}

absl::StatusOr<PrngSeed> GetPrngSeedFromCharVector(
    const std::vector<unsigned char>& seed_vec) {
  if (seed_vec.size() != (kBytesPerAes256Key + kBytesPerAes256Iv)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The seed vector has length $0 bytes, however, $1 bytes are required.",
        seed_vec.size(), (kBytesPerAes256Key + kBytesPerAes256Iv)));
  }
  PrngSeed seed;
  *seed.mutable_key() =
      std::string(seed_vec.begin(), seed_vec.begin() + kBytesPerAes256Key);
  *seed.mutable_iv() =
      std::string(seed_vec.begin() + kBytesPerAes256Key,
                  seed_vec.begin() + kBytesPerAes256Key + kBytesPerAes256Iv);
  return seed;
}

absl::StatusOr<std::vector<uint32_t>> GenerateShareFromSeed(
    const ShareShuffleSketchParams& param, const any_sketch::PrngSeed& seed) {
  ASSIGN_OR_RETURN(std::unique_ptr<math::UniformPseudorandomGenerator> prng,
                   math::CreatePrngFromSeed(seed));
  ASSIGN_OR_RETURN(std::vector<uint32_t> share_from_seed,
                   prng->GenerateUniformRandomRange(param.register_count(),
                                                    param.ring_modulus()));
  return share_from_seed;
}

absl::StatusOr<std::vector<uint32_t>> GetShareVectorFromSketchShare(
    const ShareShuffleSketchParams& sketch_params,
    const CompleteShufflePhaseRequest::SketchShare& sketch_share) {
  std::vector<uint32_t> share_vector;
  switch (sketch_share.share_type_case()) {
    case CompleteShufflePhaseRequest::SketchShare::kData:
      share_vector = std::vector(sketch_share.data().values().begin(),
                                 sketch_share.data().values().end());
      break;
    case CompleteShufflePhaseRequest::SketchShare::kSeed: {
      ASSIGN_OR_RETURN(any_sketch::PrngSeed seed,
                       GetPrngSeedFromString(sketch_share.seed()));
      ASSIGN_OR_RETURN(share_vector,
                       GenerateShareFromSeed(sketch_params, seed));
      break;
    }
    case CompleteShufflePhaseRequest::SketchShare::SHARE_TYPE_NOT_SET:
      return absl::InvalidArgumentError("Share type is not defined.");
      break;
  }
  return share_vector;
}

absl::StatusOr<std::vector<uint32_t>> CombineSketchShares(
    const ShareShuffleSketchParams& sketch_params,
    const google::protobuf::RepeatedPtrField<
        CompleteAggregationPhaseRequest::ShareData>& sketch_shares) {
  if (sketch_shares.empty()) {
    return absl::InvalidArgumentError(
        "There must be at least one share vector.");
  }

  if (sketch_params.ring_modulus() < 2) {
    return absl::InvalidArgumentError("Ring modulus must be at least 2.");
  }

  std::vector<uint32_t> combined_share(
      sketch_shares.Get(0).share_vector().size(), 0);
  for (int i = 0; i < sketch_shares.size(); i++) {
    std::vector<uint32_t> temp =
        std::vector(sketch_shares.Get(i).share_vector().begin(),
                    sketch_shares.Get(i).share_vector().end());

    ASSIGN_OR_RETURN(
        combined_share,
        VectorAddMod(combined_share, temp, sketch_params.ring_modulus()));
  }

  return combined_share;
}

absl::StatusOr<std::vector<uint32_t>> VectorAddMod(
    absl::Span<const uint32_t> vector_x, absl::Span<const uint32_t> vector_y,
    const uint32_t modulus) {
  if (vector_x.size() != vector_y.size()) {
    return absl::InvalidArgumentError(
        "Input vectors must have the same length.");
  }
  for (int i = 0; i < vector_x.size(); i++) {
    if (vector_x[i] >= modulus && vector_y[i] >= modulus) {
      return absl::InvalidArgumentError(absl::Substitute(
          "Both vector_x[$0], which is $1, and vector_y[$0], which is $2, must "
          "be less than the modulus, which is $3.",
          i, vector_x[i], vector_y[i], modulus));
    } else if (vector_x[i] >= modulus) {
      return absl::InvalidArgumentError(
          absl::Substitute("vector_x[$0], which is $1, must be less than the "
                           "modulus, which is $2.",
                           i, vector_x[i], modulus));
    } else if (vector_y[i] >= modulus) {
      return absl::InvalidArgumentError(
          absl::Substitute("vector_y[$0], which is $1, must be less than the "
                           "modulus, which is $2.",
                           i, vector_y[i], modulus));
    }
  }
  std::vector<uint32_t> result(vector_x.size());
  for (int i = 0; i < vector_x.size(); i++) {
    result[i] = vector_x[i] + vector_y[i] -
                (vector_x[i] >= (modulus - vector_y[i])) * modulus;
  }
  return result;
}

absl::StatusOr<std::vector<uint32_t>> VectorSubMod(
    const std::vector<uint32_t>& vector_x,
    const std::vector<uint32_t>& vector_y, const uint32_t modulus) {
  if (vector_x.size() != vector_y.size()) {
    return absl::InvalidArgumentError(
        "Input vectors must have the same length.");
  }
  for (int i = 0; i < vector_x.size(); i++) {
    if (vector_x[i] >= modulus && vector_y[i] >= modulus) {
      return absl::InvalidArgumentError(absl::Substitute(
          "Both vector_x[$0], which is $1, and vector_y[$0], which is $2, must "
          "be less than the modulus, which is $3.",
          i, vector_x[i], vector_y[i], modulus));
    } else if (vector_x[i] >= modulus) {
      return absl::InvalidArgumentError(
          absl::Substitute("vector_x[$0], which is $1, must be less than the "
                           "modulus, which is $2.",
                           i, vector_x[i], modulus));
    } else if (vector_y[i] >= modulus) {
      return absl::InvalidArgumentError(
          absl::Substitute("vector_y[$0], which is $1, must be less than the "
                           "modulus, which is $2.",
                           i, vector_y[i], modulus));
    }
  }
  std::vector<uint32_t> result(vector_x.size());
  for (int i = 0; i < vector_x.size(); i++) {
    result[i] =
        vector_x[i] - vector_y[i] + (vector_x[i] < vector_y[i]) * modulus;
  }
  return result;
}

absl::StatusOr<int64_t> EstimateReach(int64_t non_empty_register_count,
                                      double vid_sampling_interval_width) {
  if (non_empty_register_count < 0) {
    return absl::InvalidArgumentError(
        "Non-empty register count must be a non-negative integer.");
  }
  if (vid_sampling_interval_width <= 0 || vid_sampling_interval_width > 1) {
    return absl::InvalidArgumentError(
        absl::Substitute("The vid sampling interval width must be greater than "
                         "0 and do not exceed 1, but $0 is provided.",
                         vid_sampling_interval_width));
  }
  return static_cast<int64_t>(non_empty_register_count /
                              vid_sampling_interval_width);
}

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
