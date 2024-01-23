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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/honest_majority_share_shuffle_utility.h"

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
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/noise_parameters_computation.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {

namespace {

using ::wfa::any_sketch::PrngSeed;
using ::wfa::math::CreatePrngFromSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;
using ::wfa::measurement::common::crypto::SecureShuffleWithSeed;

absl::StatusOr<std::vector<uint32_t>> GenerateNoiseRegisters(
    const ShareShuffleSketchParams& sketch_param,
    const math::DistributedNoiser& distributed_noiser) {
  if (sketch_param.ring_modulus() <= sketch_param.maximum_frequency() + 1) {
    return absl::InvalidArgumentError(
        "Ring modulus must be greater than maximum frequency plus 1.");
  }

  int64_t total_noise_registers_count =
      distributed_noiser.options().shift_offset * 2 *
      (1 + sketch_param.maximum_frequency());
  // Sets all noise registers to the sentinel value (q-1).
  std::vector<uint32_t> ret(total_noise_registers_count,
                            sketch_param.ring_modulus() - 1);

  int current_index = 0;
  for (int k = 0; k <= sketch_param.maximum_frequency(); k++) {
    ASSIGN_OR_RETURN(int64_t noise_register_count_for_bucket_k,
                     distributed_noiser.GenerateNoiseComponent());
    for (int i = 0; i < noise_register_count_for_bucket_k; i++) {
      ret[current_index] = k;
      current_index++;
    }
  }

  return ret;
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
    const ShareShuffleSketchParams& param, const PrngSeed& seed) {
  ASSIGN_OR_RETURN(std::unique_ptr<math::UniformPseudorandomGenerator> prng,
                   CreatePrngFromSeed(seed));
  ASSIGN_OR_RETURN(std::vector<uint32_t> share_from_seed,
                   prng->GenerateUniformRandomRange(param.register_count(),
                                                    param.ring_modulus()));
  return share_from_seed;
}

absl::Status VerifySketchParameters(const ShareShuffleSketchParams& params) {
  if (params.register_count() < 1) {
    return absl::InvalidArgumentError("The register count must be at least 1.");
  }
  if (params.ring_modulus() < 2) {
    return absl::InvalidArgumentError("The ring modulus must be at least 2.");
  }
  if (params.ring_modulus() <= params.maximum_frequency() + 1) {
    return absl::InvalidArgumentError(
        "The ring modulus must be greater than maximum frequency plus one.");
  }
  if (params.ring_modulus() > (1 << (8 * params.bytes_per_register()))) {
    return absl::InvalidArgumentError(
        "The bit length of the register is not enough to store the shares.");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<CompleteShufflePhaseResponse> CompleteShufflePhase(
    const CompleteShufflePhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteShufflePhaseResponse response;

  RETURN_IF_ERROR(VerifySketchParameters(request.sketch_params()));

  if (request.sketch_shares().empty()) {
    return absl::InvalidArgumentError("Sketch shares must not be empty.");
  }

  int sketch_size = request.sketch_params().register_count();

  // Combines the input shares.
  std::vector<uint32_t> combined_sketch(sketch_size, 0);
  for (int i = 0; i < request.sketch_shares().size(); i++) {
    switch (request.sketch_shares().Get(i).share_type_case()) {
      case CompleteShufflePhaseRequest::SketchShare::kData:
        if (request.sketch_shares().Get(i).data().values().size() !=
            request.sketch_params().register_count()) {
          return absl::InvalidArgumentError("Invalid input size.");
        }
        for (int j = 0; j < sketch_size; j++) {
          // It's guaranteed that (combined_sketch[j] +
          // request.sketch_shares().Get(i).data().values(j)) is not greater
          // than 2^{32}-1.
          combined_sketch[j] =
              (combined_sketch[j] +
               request.sketch_shares().Get(i).data().values(j)) %
              request.sketch_params().ring_modulus();
        }
        break;
      case CompleteShufflePhaseRequest::SketchShare::kSeed: {
        ASSIGN_OR_RETURN(
            PrngSeed seed,
            GetPrngSeedFromString(request.sketch_shares().Get(i).seed()));
        ASSIGN_OR_RETURN(std::vector<uint32_t> share_from_seed,
                         GenerateShareFromSeed(request.sketch_params(), seed));
        for (int j = 0; j < sketch_size; j++) {
          // It's guaranteed that (combined_sketch[j] + share_from_seed[j]) is
          // not greater than 2^{32}-1.
          combined_sketch[j] = (combined_sketch[j] + share_from_seed[j]) %
                               request.sketch_params().ring_modulus();
        }
        break;
      }
      case CompleteShufflePhaseRequest::SketchShare::SHARE_TYPE_NOT_SET:
        return absl::InvalidArgumentError("Share type is not defined.");
        break;
    }
  }

  ASSIGN_OR_RETURN(PrngSeed seed,
                   GetPrngSeedFromString(request.common_random_seed()));
  // Initializes the pseudo-random generator with the common random seed.
  // The PRNG will generate random shares for the noise registers (if needed)
  // and the seed that is used for shuffling.
  ASSIGN_OR_RETURN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                   CreatePrngFromSeed(seed));

  // Adds noise registers to the combined input share.
  if (request.has_dp_params()) {
    // Initializes the noiser, which will generate blind histogram noise to hide
    // the actual frequency histogram counts.
    auto noiser = GetBlindHistogramNoiser(request.dp_params(),
                                          /*contributors_count=*/2,
                                          request.noise_mechanism());

    // Generates local noise registers.
    ASSIGN_OR_RETURN(std::vector<uint32_t> noise_registers,
                     GenerateNoiseRegisters(request.sketch_params(), *noiser));

    std::vector<uint32_t> first_local_noise_share;
    std::vector<uint32_t> second_local_noise_share;

    // Generates local noise register shares using the common random seed.
    // Two workers generates shares of noise as below:
    // Worker 1: rand_vec_1 || rand_vec_2 <-- PRNG(seed).
    // Worker 2: rand_vec_2 || rand_vec_2 <-- PRNG(seed).
    // Worker 1 obtains shares: {first_local_noise_share ||
    // second_local_noise_share} = {(noise_registers_1 - rand_vec_1) ||
    // rand_vec_2}. Worker 2 obtains shares: {first_local_noise_share ||
    // second_local_noise_share} = {rand_vec_1 || (noise_registers_2 -
    // rand_vec_2)}.
    if (request.order() == CompleteShufflePhaseRequest::FIRST) {
      ASSIGN_OR_RETURN(
          std::vector<uint32_t> first_peer_noise_share,
          prng->GenerateUniformRandomRange(
              noise_registers.size(), request.sketch_params().ring_modulus()));
      first_local_noise_share.resize(noise_registers.size());
      for (int i = 0; i < noise_registers.size(); i++) {
        first_local_noise_share[i] =
            noise_registers[i] - first_peer_noise_share[i] +
            (noise_registers[i] < first_peer_noise_share[i]) *
                request.sketch_params().ring_modulus();
      }
      ASSIGN_OR_RETURN(
          second_local_noise_share,
          prng->GenerateUniformRandomRange(
              noise_registers.size(), request.sketch_params().ring_modulus()));

    } else if (request.order() == CompleteShufflePhaseRequest::SECOND) {
      ASSIGN_OR_RETURN(
          first_local_noise_share,
          prng->GenerateUniformRandomRange(
              noise_registers.size(), request.sketch_params().ring_modulus()));
      ASSIGN_OR_RETURN(
          std::vector<uint32_t> second_peer_noise_share,
          prng->GenerateUniformRandomRange(
              noise_registers.size(), request.sketch_params().ring_modulus()));
      second_local_noise_share.resize(noise_registers.size());
      for (int i = 0; i < noise_registers.size(); i++) {
        second_local_noise_share[i] =
            noise_registers[i] - second_peer_noise_share[i] +
            (noise_registers[i] < second_peer_noise_share[i]) *
                request.sketch_params().ring_modulus();
      }
    } else {
      return absl::InvalidArgumentError(
          "Non aggregator order must be specified.");
    }

    // Appends the first noise share to the combined sketch share.
    combined_sketch.insert(combined_sketch.end(),
                           first_local_noise_share.begin(),
                           first_local_noise_share.end());
    // Appends the second noise share to the combined sketch share.
    combined_sketch.insert(combined_sketch.end(),
                           second_local_noise_share.begin(),
                           second_local_noise_share.end());
  }

  // Generates shuffle seed from common random seed.
  ASSIGN_OR_RETURN(
      std::vector<unsigned char> shuffle_seed_vec,
      prng->GeneratePseudorandomBytes(kBytesPerAes256Key + kBytesPerAes256Iv));
  ASSIGN_OR_RETURN(PrngSeed shuffle_seed,
                   GetPrngSeedFromCharVector(shuffle_seed_vec));
  // Shuffle the shares.
  RETURN_IF_ERROR(SecureShuffleWithSeed(combined_sketch, shuffle_seed));

  response.mutable_combined_sketch()->Add(combined_sketch.begin(),
                                          combined_sketch.end());

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
