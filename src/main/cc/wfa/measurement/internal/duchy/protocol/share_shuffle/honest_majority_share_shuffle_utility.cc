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

#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility.h"

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
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/util/time_util.h"
#include "math/distributed_noiser.h"
#include "math/open_ssl_uniform_random_generator.h"
#include "wfa/frequency_count/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"
#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility_helper.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {

namespace {

using ::wfa::crypto::SecureShuffleWithSeed;
using ::wfa::frequency_count::PrngSeed;
using ::wfa::math::CreatePrngFromSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;

constexpr int kWorkerCount = 2;

absl::Status VerifySketchParameters(const ShareShuffleSketchParams& params) {
  if (params.register_count() < 1) {
    return absl::InvalidArgumentError("The register count must be at least 1.");
  }
  if (params.ring_modulus() < 2) {
    return absl::InvalidArgumentError("The ring modulus must be at least 2.");
  }
  if (params.ring_modulus() <= params.maximum_combined_frequency() + 1) {
    return absl::InvalidArgumentError(
        "The ring modulus must be greater than maximum combined frequency plus "
        "one.");
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

  const int sketch_size = request.sketch_params().register_count();

  // Combines the input shares.
  std::vector<uint32_t> combined_sketch(sketch_size, 0);
  for (int i = 0; i < request.sketch_shares().size(); i++) {
    ASSIGN_OR_RETURN(
        std::vector<uint32_t> share_vector,
        GetShareVectorFromSketchShare(request.sketch_params(),
                                      request.sketch_shares().Get(i)));
    if (share_vector.size() != sketch_size) {
      return absl::InvalidArgumentError(absl::Substitute(
          "The $0-th sketch share has invalid size. Expect $1 but the "
          "actual is $2.",
          i, sketch_size, share_vector.size()));
    }
    for (int j = 0; j < sketch_size; j++) {
      // It's guaranteed that (combined_sketch[j] + share_vector[j]) is
      // not greater than 2^{32}-1.
      combined_sketch[j] = (combined_sketch[j] + share_vector[j]) %
                           request.sketch_params().ring_modulus();
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

    // Both workers generate common random vectors from the common random seed.
    // rand_vec_1 || rand_vec_2 <-- PRNG(seed).
    ASSIGN_OR_RETURN(
        std::vector<uint32_t> rand_vec_1,
        prng->GenerateUniformRandomRange(
            noise_registers.size(), request.sketch_params().ring_modulus()));
    ASSIGN_OR_RETURN(
        std::vector<uint32_t> rand_vec_2,
        prng->GenerateUniformRandomRange(
            noise_registers.size(), request.sketch_params().ring_modulus()));

    // Generates local noise register shares using the common random vectors.
    // Worker 1 obtains shares:
    // {first_local_noise_share || second_local_noise_share}
    // = {(noise_registers_1 - rand_vec_1) || rand_vec_2}.
    // Worker 2 obtains shares:
    // {first_local_noise_share ||second_local_noise_share}
    // = {rand_vec_1 || (noise_registers_2 - rand_vec_2)}.
    std::vector<uint32_t> first_local_noise_share;
    std::vector<uint32_t> second_local_noise_share;
    if (request.order() == CompleteShufflePhaseRequest::FIRST) {
      ASSIGN_OR_RETURN(first_local_noise_share,
                       VectorSubMod(noise_registers, rand_vec_1,
                                    request.sketch_params().ring_modulus()));
      second_local_noise_share = std::move(rand_vec_2);
    } else if (request.order() == CompleteShufflePhaseRequest::SECOND) {
      first_local_noise_share = std::move(rand_vec_1);
      ASSIGN_OR_RETURN(second_local_noise_share,
                       VectorSubMod(noise_registers, rand_vec_2,
                                    request.sketch_params().ring_modulus()));
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
  *response.mutable_elapsed_cpu_duration() =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteAggregationPhaseResponse> CompleteAggregationPhase(
    const CompleteAggregationPhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteAggregationPhaseResponse response;

  if (request.sketch_shares().size() != kWorkerCount) {
    return absl::InvalidArgumentError(
        "The number of share vectors must be equal to the number of "
        "non-aggregators.");
  }
  ASSIGN_OR_RETURN(
      std::vector<uint32_t> combined_sketch,
      CombineSketchShares(request.sketch_params(), request.sketch_shares()));

  int maximum_frequency = request.maximum_frequency();
  if (maximum_frequency < 1) {
    return absl::InvalidArgumentError(
        "The maximum frequency should be greater than 0.");
  }

  // Validates the combined sketch and generates the frequency histogram.
  // frequency_histogram[i] = the number of times value i occurs where
  // i in {0, ..., maximum_frequency-1}.
  absl::flat_hash_map<int, int64_t> frequency_histogram;
  for (auto x : combined_sketch) {
    if (x > request.sketch_params().maximum_combined_frequency() &&
        x != (request.sketch_params().ring_modulus() - 1)) {
      return absl::InternalError(absl::Substitute(
          "The combined register value, which is $0, is not valid. It must be "
          "either the "
          "sentinel value, which is $1, or less that or equal to the combined "
          "maximum "
          "frequency, which is $2.",
          x, request.sketch_params().ring_modulus() - 1,
          request.sketch_params().maximum_combined_frequency()));
    }
    if (x < maximum_frequency) {
      frequency_histogram[x]++;
    }
  }

  int64_t register_count = request.sketch_params().register_count();

  // Computes the non empty register count by subtracting frequency zero from
  // the input sketch size. Another way to compute it is to add up all
  // frequencies from 1 to the maximum combined frequency. However, the latter
  // approach will have a lot more noise.
  int64_t non_empty_register_count = register_count - frequency_histogram[0];

  // Adjusts the frequency histogram and non empty register count according the
  // noise baseline for the frequencies from 0 to {maximum_frequency - 1}.
  if (request.has_dp_params()) {
    auto noiser = GetBlindHistogramNoiser(request.dp_params(), kWorkerCount,
                                          request.noise_mechanism());
    int64_t noise_baseline_per_bucket =
        noiser->options().shift_offset * kWorkerCount;
    // Removes the noise baseline from the frequency histogram.
    for (auto it = frequency_histogram.begin(); it != frequency_histogram.end();
         it++) {
      it->second = std::max(0L, it->second - noise_baseline_per_bucket);
    }
    // Adjusts the non empty register count.
    non_empty_register_count += noise_baseline_per_bucket;

    // Ensures that non_empty_register_count is at least 0.
    non_empty_register_count = std::max(0L, non_empty_register_count);

    // Ensures that non_empty_register_count is at most the input sketch size.
    non_empty_register_count =
        std::min(register_count, non_empty_register_count);
  }

  // Counts the number of registers that have frequency value in the range
  // [0, maximum_frequency - 1].
  int accumulated_count = 0;
  for (auto it = frequency_histogram.begin(); it != frequency_histogram.end();
       it++) {
    accumulated_count += it->second;
  }

  // Computes the value frequency_histogram[maximum_frequency+] from the current
  // frequency histogram, then stores the number of registers that have
  // frequency value in the range [1, maximum_frequency+] in adjusted_total, and
  // uses the adjusted total in the calculation of the frequency distribution.
  //
  // Let f[i] = frequency_histogram[i], MF = maximum_frequency. From the
  // previous step we have: accumulated_count = f[0] + ... + f[MF - 1].
  //
  // If the accumulated count exceeds the register count, f[MF+] will be
  // assigned zero value (which is very unlikely in practice). Then
  // adjusted_total = accumulated_count - f[0] = f[1] + ... + f[MF - 1] = f[1] +
  // ... + f[MF - 1] + f[MF+] (as f[MF+] = 0).
  //
  // Otherwise, f[MF+] is equal to {register_count - accumulated_count}. And
  // adjusted_total = register_count - f[0] = f[MF+] + accumulated_count - f[0]
  // = f[1] + ... + f[MF - 1] + f[MF+].
  int64_t adjusted_total = 0;
  if (accumulated_count > register_count) {
    frequency_histogram[maximum_frequency] = 0;
    adjusted_total = accumulated_count - frequency_histogram[0];
  } else {
    frequency_histogram[maximum_frequency] = register_count - accumulated_count;
    adjusted_total = register_count - frequency_histogram[0];
  }

  // Returns error message when the adjusted_total equals to zero. This happens
  // when frequency_histogram[0] = max(register_count, accumulated_count) which
  // suggests that the sketchs are empty.
  if (adjusted_total == 0) {
    return absl::InvalidArgumentError(
        "There is neither actual data nor effective noise in the request.");
  }
  // Estimates reach using the number of non-empty buckets and the vid sampling
  // interval width.
  ASSIGN_OR_RETURN(int64_t reach,
                   EstimateReach(non_empty_register_count,
                                 request.vid_sampling_interval_width()));

  response.set_reach(reach);

  google::protobuf::Map<int64_t, double>& distribution =
      *response.mutable_frequency_distribution();
  for (int i = 1; i <= maximum_frequency; ++i) {
    if (frequency_histogram[i] != 0) {
      distribution[i] =
          static_cast<double>(frequency_histogram[i]) / adjusted_total;
    }
  }
  *response.mutable_elapsed_cpu_duration() =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
