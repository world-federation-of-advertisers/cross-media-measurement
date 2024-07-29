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
#include "common_cpp/macros/macros.h"
#include "common_cpp/time/started_thread_cpu_timer.h"
#include "crypto/shuffle.h"
#include "math/distributed_noiser.h"
#include "math/open_ssl_uniform_random_generator.h"
#include "wfa/frequency_count/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

using ::wfa::crypto::SecureShuffleWithSeed;
using ::wfa::frequency_count::PrngSeed;
using ::wfa::math::CreatePrngFromSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;

absl::StatusOr<std::vector<uint32_t>> GenerateReachAndFrequencyNoiseRegisters(
    const ShareShuffleFrequencyVectorParams& frequency_vector_param,
    const math::DistributedNoiser& distributed_reach_noiser,
    const math::DistributedNoiser& distributed_frequency_noiser) {
  if (frequency_vector_param.ring_modulus() <=
      frequency_vector_param.maximum_combined_frequency() + 1) {
    return absl::InvalidArgumentError(
        "Ring modulus must be greater than maximum combined frequency plus 1.");
  }

  int64_t total_noise_registers_count =
      distributed_reach_noiser.options().shift_offset * 2 +
      distributed_frequency_noiser.options().shift_offset * 2 *
          frequency_vector_param.maximum_combined_frequency();
  // Sets all noise registers to the sentinel value (q-1).
  std::vector<uint32_t> noise_registers(
      total_noise_registers_count, frequency_vector_param.ring_modulus() - 1);

  int current_index = 0;
  // Sample noise registers for reach.
  ASSIGN_OR_RETURN(int64_t noise_register_count_for_reach,
                   distributed_reach_noiser.GenerateNoiseComponent());
  for (int i = 0; i < noise_register_count_for_reach; i++) {
    noise_registers[current_index] = 0;
    current_index++;
  }

  // Sample noise registers for 1+ frequency.
  for (int k = 1; k <= frequency_vector_param.maximum_combined_frequency();
       k++) {
    ASSIGN_OR_RETURN(int64_t noise_register_count_for_bucket_k,
                     distributed_frequency_noiser.GenerateNoiseComponent());
    for (int i = 0; i < noise_register_count_for_bucket_k; i++) {
      noise_registers[current_index] = k;
      current_index++;
    }
  }

  return noise_registers;
}

absl::StatusOr<std::vector<uint32_t>> GenerateReachOnlyNoiseRegisters(
    const ShareShuffleFrequencyVectorParams& frequency_vector_param,
    const math::DistributedNoiser& distributed_noiser) {
  if (frequency_vector_param.ring_modulus() <=
      frequency_vector_param.maximum_combined_frequency() + 1) {
    return absl::InvalidArgumentError(
        "Ring modulus must be greater than maximum combined frequency plus 1.");
  }

  int64_t total_noise_registers_count =
      distributed_noiser.options().shift_offset * 2;

  ASSIGN_OR_RETURN(int64_t non_zero_register_noise_count,
                   distributed_noiser.GenerateNoiseComponent());

  std::vector<unsigned char> key(kBytesPerAes256Key);
  std::vector<unsigned char> iv(kBytesPerAes256Iv);
  RAND_bytes(key.data(), key.size());
  RAND_bytes(iv.data(), iv.size());

  ASSIGN_OR_RETURN(std::unique_ptr<math::UniformPseudorandomGenerator> prng,
                   math::OpenSslUniformPseudorandomGenerator::Create(key, iv));

  ASSIGN_OR_RETURN(std::vector<uint32_t> noise_registers,
                   prng->GenerateNonZeroUniformRandomRange(
                       non_zero_register_noise_count,
                       frequency_vector_param.ring_modulus()));

  noise_registers.resize(total_noise_registers_count, 0);

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
    const ShareShuffleFrequencyVectorParams& param,
    const frequency_count::PrngSeed& seed) {
  ASSIGN_OR_RETURN(std::unique_ptr<math::UniformPseudorandomGenerator> prng,
                   math::CreatePrngFromSeed(seed));
  ASSIGN_OR_RETURN(std::vector<uint32_t> share_from_seed,
                   prng->GenerateUniformRandomRange(param.register_count(),
                                                    param.ring_modulus()));
  return share_from_seed;
}

absl::StatusOr<std::vector<uint32_t>> GetShareVectorFromFrequencyVectorShare(
    const ShareShuffleFrequencyVectorParams& frequency_vector_params,
    const CompleteShufflePhaseRequest::FrequencyVectorShare&
        frequency_vector_share) {
  std::vector<uint32_t> share_vector;
  switch (frequency_vector_share.share_type_case()) {
    case CompleteShufflePhaseRequest::FrequencyVectorShare::kData:
      share_vector = std::vector(frequency_vector_share.data().values().begin(),
                                 frequency_vector_share.data().values().end());
      break;
    case CompleteShufflePhaseRequest::FrequencyVectorShare::kSeed: {
      ASSIGN_OR_RETURN(frequency_count::PrngSeed seed,
                       GetPrngSeedFromString(frequency_vector_share.seed()));
      ASSIGN_OR_RETURN(share_vector,
                       GenerateShareFromSeed(frequency_vector_params, seed));
      break;
    }
    case CompleteShufflePhaseRequest::FrequencyVectorShare::SHARE_TYPE_NOT_SET:
      return absl::InvalidArgumentError("Share type is not defined.");
      break;
  }
  return share_vector;
}

absl::StatusOr<std::vector<uint32_t>> CombineFrequencyVectorShares(
    const ShareShuffleFrequencyVectorParams& frequency_vector_params,
    const google::protobuf::RepeatedPtrField<
        CompleteAggregationPhaseRequest::ShareData>& frequency_vector_shares) {
  if (frequency_vector_shares.empty()) {
    return absl::InvalidArgumentError(
        "There must be at least one share vector.");
  }

  if (frequency_vector_params.ring_modulus() < 2) {
    return absl::InvalidArgumentError("Ring modulus must be at least 2.");
  }

  std::vector<uint32_t> combined_share(
      frequency_vector_shares.Get(0).share_vector().size(), 0);
  for (int i = 0; i < frequency_vector_shares.size(); i++) {
    std::vector<uint32_t> temp =
        std::vector(frequency_vector_shares.Get(i).share_vector().begin(),
                    frequency_vector_shares.Get(i).share_vector().end());

    ASSIGN_OR_RETURN(combined_share,
                     VectorAddMod(combined_share, temp,
                                  frequency_vector_params.ring_modulus()));
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
