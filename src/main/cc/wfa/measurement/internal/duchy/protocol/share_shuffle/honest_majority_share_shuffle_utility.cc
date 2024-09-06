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
using ::wfa::math::DistributedNoiser;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;

constexpr int kWorkerCount = 2;

absl::Status VerifyFrequencyVectorParameters(
    const ShareShuffleFrequencyVectorParams& params) {
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
  return absl::OkStatus();
}

// Checks if val is a prime.
//
// This algorithm has the computation complexity of O(sqrt(val)).
bool IsPrime(int val) {
  if (val <= 1) {
    return false;
  }
  for (int i = 2; i * i <= val; i++) {
    if (val % i == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace

absl::StatusOr<CompleteShufflePhaseResponse>
CompleteReachAndFrequencyShufflePhase(
    const CompleteShufflePhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteShufflePhaseResponse response;

  RETURN_IF_ERROR(
      VerifyFrequencyVectorParameters(request.frequency_vector_params()));

  if (request.frequency_vector_shares().empty()) {
    return absl::InvalidArgumentError(
        "FrequencyVector shares must not be empty.");
  }

  const int frequency_vector_size =
      request.frequency_vector_params().register_count();

  // Combines the input shares.
  std::vector<uint32_t> combined_frequency_vector(frequency_vector_size, 0);
  for (int i = 0; i < request.frequency_vector_shares().size(); i++) {
    ASSIGN_OR_RETURN(std::vector<uint32_t> share_vector,
                     GetShareVectorFromFrequencyVectorShare(
                         request.frequency_vector_params(),
                         request.frequency_vector_shares().Get(i)));
    if (share_vector.size() != frequency_vector_size) {
      return absl::InvalidArgumentError(
          absl::Substitute("The $0-th frequency_vector share has invalid size. "
                           "Expect $1 but the "
                           "actual is $2.",
                           i, frequency_vector_size, share_vector.size()));
    }
    ASSIGN_OR_RETURN(
        combined_frequency_vector,
        VectorAddMod(combined_frequency_vector, share_vector,
                     request.frequency_vector_params().ring_modulus()));
  }

  ASSIGN_OR_RETURN(PrngSeed seed,
                   GetPrngSeedFromString(request.common_random_seed()));
  // Initializes the pseudo-random generator with the common random seed.
  // The PRNG will generate random shares for the noise registers (if needed)
  // and the seed that is used for shuffling.
  ASSIGN_OR_RETURN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                   CreatePrngFromSeed(seed));

  // Adds noise registers to the combined input share.
  if (request.has_reach_dp_params() && request.has_frequency_dp_params()) {
    // Initializes the reach noiser, which will generate reach noise.
    std::unique_ptr<DistributedNoiser> reach_noiser = GetBlindHistogramNoiser(
        request.reach_dp_params(), kWorkerCount, request.noise_mechanism());
    // Initializes the frequency noiser, which will generate noise to hide the
    // actual frequency histogram counts for frequency 1+.
    std::unique_ptr<DistributedNoiser> frequency_noiser =
        GetBlindHistogramNoiser(request.frequency_dp_params(), kWorkerCount,
                                request.noise_mechanism());
    // Generates local noise registers.
    ASSIGN_OR_RETURN(std::vector<uint32_t> noise_registers,
                     GenerateReachAndFrequencyNoiseRegisters(
                         request.frequency_vector_params(), *reach_noiser,
                         *frequency_noiser));

    // Both workers generate common random vectors from the common random seed.
    // rand_vec_1 || rand_vec_2 <-- PRNG(seed).
    ASSIGN_OR_RETURN(std::vector<uint32_t> rand_vec_1,
                     prng->GenerateUniformRandomRange(
                         noise_registers.size(),
                         request.frequency_vector_params().ring_modulus()));
    ASSIGN_OR_RETURN(std::vector<uint32_t> rand_vec_2,
                     prng->GenerateUniformRandomRange(
                         noise_registers.size(),
                         request.frequency_vector_params().ring_modulus()));

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
      ASSIGN_OR_RETURN(
          first_local_noise_share,
          VectorSubMod(noise_registers, rand_vec_1,
                       request.frequency_vector_params().ring_modulus()));
      second_local_noise_share = std::move(rand_vec_2);
    } else if (request.order() == CompleteShufflePhaseRequest::SECOND) {
      first_local_noise_share = std::move(rand_vec_1);
      ASSIGN_OR_RETURN(
          second_local_noise_share,
          VectorSubMod(noise_registers, rand_vec_2,
                       request.frequency_vector_params().ring_modulus()));
    } else {
      return absl::InvalidArgumentError(
          "Non aggregator order must be specified.");
    }

    // Appends the first noise share to the combined frequency_vector share.
    combined_frequency_vector.insert(combined_frequency_vector.end(),
                                     first_local_noise_share.begin(),
                                     first_local_noise_share.end());
    // Appends the second noise share to the combined frequency_vector share.
    combined_frequency_vector.insert(combined_frequency_vector.end(),
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
  RETURN_IF_ERROR(
      SecureShuffleWithSeed(combined_frequency_vector, shuffle_seed));

  response.mutable_combined_frequency_vector()->Add(
      combined_frequency_vector.begin(), combined_frequency_vector.end());
  *response.mutable_elapsed_cpu_duration() =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteShufflePhaseResponse> CompleteReachOnlyShufflePhase(
    const CompleteShufflePhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteShufflePhaseResponse response;

  RETURN_IF_ERROR(
      VerifyFrequencyVectorParameters(request.frequency_vector_params()));

  // Verify that the ring modulus is a prime.
  if (!IsPrime(request.frequency_vector_params().ring_modulus())) {
    return absl::InvalidArgumentError("The ring modulus must be a prime.");
  }

  if (request.frequency_vector_shares().empty()) {
    return absl::InvalidArgumentError(
        "FrequencyVector shares must not be empty.");
  }

  const int frequency_vector_size =
      request.frequency_vector_params().register_count();

  // Combines the input shares.
  std::vector<uint32_t> combined_frequency_vector(frequency_vector_size, 0);
  for (int i = 0; i < request.frequency_vector_shares().size(); i++) {
    ASSIGN_OR_RETURN(std::vector<uint32_t> share_vector,
                     GetShareVectorFromFrequencyVectorShare(
                         request.frequency_vector_params(),
                         request.frequency_vector_shares().Get(i)));
    if (share_vector.size() != frequency_vector_size) {
      return absl::InvalidArgumentError(
          absl::Substitute("The $0-th frequency_vector share has invalid size. "
                           "Expect $1 but the "
                           "actual is $2.",
                           i, frequency_vector_size, share_vector.size()));
    }
    ASSIGN_OR_RETURN(
        combined_frequency_vector,
        VectorAddMod(combined_frequency_vector, share_vector,
                     request.frequency_vector_params().ring_modulus()));
  }
  ASSIGN_OR_RETURN(PrngSeed seed,
                   GetPrngSeedFromString(request.common_random_seed()));
  // Initializes the pseudo-random generator with the common random seed.
  // The PRNG will generate random shares for the noise registers (if needed)
  // and the seed that is used for shuffling.
  ASSIGN_OR_RETURN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                   CreatePrngFromSeed(seed));

  // Sample a vector r of random values in [1, modulus).
  ASSIGN_OR_RETURN(std::vector<uint32_t> r,
                   prng->GenerateNonZeroUniformRandomRange(
                       frequency_vector_size,
                       request.frequency_vector_params().ring_modulus()));

  // Transform share of non-zero registers to share of a non-zero random value.
  for (int j = 0; j < frequency_vector_size; j++) {
    combined_frequency_vector[j] =
        uint64_t{combined_frequency_vector[j]} * uint64_t{r[j]} %
        request.frequency_vector_params().ring_modulus();
  }

  // Adds noise registers to the combined input share.
  if (request.has_reach_dp_params()) {
    // Initializes the reach noiser, which will generate reach noise.
    std::unique_ptr<DistributedNoiser> reach_noiser = GetBlindHistogramNoiser(
        request.reach_dp_params(), kWorkerCount, request.noise_mechanism());

    // Generates local noise registers.
    ASSIGN_OR_RETURN(std::vector<uint32_t> noise_registers,
                     GenerateReachOnlyNoiseRegisters(
                         request.frequency_vector_params(), *reach_noiser));

    // Both workers generate common random vectors from the common random seed.
    // rand_vec_1 || rand_vec_2 <-- PRNG(seed).
    ASSIGN_OR_RETURN(std::vector<uint32_t> rand_vec_1,
                     prng->GenerateUniformRandomRange(
                         noise_registers.size(),
                         request.frequency_vector_params().ring_modulus()));
    ASSIGN_OR_RETURN(std::vector<uint32_t> rand_vec_2,
                     prng->GenerateUniformRandomRange(
                         noise_registers.size(),
                         request.frequency_vector_params().ring_modulus()));

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
      ASSIGN_OR_RETURN(
          first_local_noise_share,
          VectorSubMod(noise_registers, rand_vec_1,
                       request.frequency_vector_params().ring_modulus()));
      second_local_noise_share = std::move(rand_vec_2);
    } else if (request.order() == CompleteShufflePhaseRequest::SECOND) {
      first_local_noise_share = std::move(rand_vec_1);
      ASSIGN_OR_RETURN(
          second_local_noise_share,
          VectorSubMod(noise_registers, rand_vec_2,
                       request.frequency_vector_params().ring_modulus()));
    } else {
      return absl::InvalidArgumentError(
          "Non aggregator order must be specified.");
    }

    // Appends the first noise share to the combined frequency_vector share.
    combined_frequency_vector.insert(combined_frequency_vector.end(),
                                     first_local_noise_share.begin(),
                                     first_local_noise_share.end());
    // Appends the second noise share to the combined frequency_vector share.
    combined_frequency_vector.insert(combined_frequency_vector.end(),
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
  RETURN_IF_ERROR(
      SecureShuffleWithSeed(combined_frequency_vector, shuffle_seed));

  response.mutable_combined_frequency_vector()->Add(
      combined_frequency_vector.begin(), combined_frequency_vector.end());
  *response.mutable_elapsed_cpu_duration() =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteAggregationPhaseResponse>
CompleteReachAndFrequencyAggregationPhase(
    const CompleteAggregationPhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteAggregationPhaseResponse response;

  RETURN_IF_ERROR(
      VerifyFrequencyVectorParameters(request.frequency_vector_params()));

  if (request.frequency_vector_shares().size() != kWorkerCount) {
    return absl::InvalidArgumentError(
        "The number of share vectors must be equal to the number of "
        "non-aggregators.");
  }
  ASSIGN_OR_RETURN(
      std::vector<uint32_t> combined_frequency_vector,
      CombineFrequencyVectorShares(request.frequency_vector_params(),
                                   request.frequency_vector_shares()));

  int maximum_frequency = request.maximum_frequency();
  if (maximum_frequency < 1) {
    return absl::InvalidArgumentError(
        "The maximum frequency should be greater than 0.");
  }

  // Validates the combined frequency_vector and generates the frequency
  // histogram. frequency_histogram[i] = the number of times value i occurs
  // where i in {0, ..., maximum_frequency-1}.
  absl::flat_hash_map<int, int64_t> frequency_histogram;
  for (const auto reg : combined_frequency_vector) {
    if (reg > request.frequency_vector_params().maximum_combined_frequency() &&
        reg != (request.frequency_vector_params().ring_modulus() - 1)) {
      return absl::InternalError(absl::Substitute(
          "The combined register value, which is $0, is not valid. It must be "
          "either the sentinel value, which is $1, or less that or equal to "
          "the combined maximum frequency, which is $2.",
          reg, request.frequency_vector_params().ring_modulus() - 1,
          request.frequency_vector_params().maximum_combined_frequency()));
    }
    if (reg < maximum_frequency) {
      frequency_histogram[reg]++;
    }
  }

  int64_t register_count = request.frequency_vector_params().register_count();

  // Computes the non empty register count by subtracting frequency zero from
  // the input frequency_vector size. Another way to compute it is to add up all
  // frequencies from 1 to the maximum combined frequency. However, the latter
  // approach will have a lot more noise.
  int64_t non_empty_register_count =
      (frequency_histogram.find(0) == frequency_histogram.end())
          ? register_count
          : register_count - frequency_histogram[0];

  // Adjusts the frequency histogram and non empty register count according the
  // noise baseline for the frequencies from 0 to {maximum_frequency - 1}.
  if (request.has_reach_dp_params() && request.has_frequency_dp_params()) {
    std::unique_ptr<DistributedNoiser> reach_noiser = GetBlindHistogramNoiser(
        request.reach_dp_params(), kWorkerCount, request.noise_mechanism());
    std::unique_ptr<DistributedNoiser> frequency_noiser =
        GetBlindHistogramNoiser(request.frequency_dp_params(), kWorkerCount,
                                request.noise_mechanism());
    int64_t reach_noise_baseline =
        reach_noiser->options().shift_offset * kWorkerCount;
    int64_t noise_baseline_per_frequency =
        frequency_noiser->options().shift_offset * kWorkerCount;
    // Removes the noise baseline from the frequency histogram.
    for (auto it = frequency_histogram.begin(); it != frequency_histogram.end();
         it++) {
      if (it->first == 0) {
        it->second = std::max(0L, it->second - reach_noise_baseline);
      } else {
        it->second = std::max(0L, it->second - noise_baseline_per_frequency);
      }
    }
    // Adjusts the non empty register count.
    non_empty_register_count += reach_noise_baseline;

    // Ensures that non_empty_register_count is at least 0.
    non_empty_register_count = std::max(0L, non_empty_register_count);

    // Ensures that non_empty_register_count is at most the input
    // frequency_vector size.
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
  // suggests that the frequency_vectors are empty.
  if (adjusted_total == 0) {
    return absl::InvalidArgumentError(
        "There is neither actual data nor effective noise in the request.");
  }
  // Estimates reach using the number of non-empty buckets and the VID sampling
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

absl::StatusOr<CompleteAggregationPhaseResponse>
CompleteReachOnlyAggregationPhase(
    const CompleteAggregationPhaseRequest& request) {
  StartedThreadCpuTimer timer;
  CompleteAggregationPhaseResponse response;
  RETURN_IF_ERROR(
      VerifyFrequencyVectorParameters(request.frequency_vector_params()));
  if (request.frequency_vector_shares().size() != kWorkerCount) {
    return absl::InvalidArgumentError(
        "The number of share vectors must be equal to the number of "
        "non-aggregators.");
  }
  ASSIGN_OR_RETURN(
      std::vector<uint32_t> combined_frequency_vector,
      CombineFrequencyVectorShares(request.frequency_vector_params(),
                                   request.frequency_vector_shares()));

  // Count the non-zero registers.
  int64_t non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if (reg != 0) {
      non_empty_register_count++;
    }
  }

  // Adjusts the non empty register count according the noise baseline.
  if (request.has_reach_dp_params()) {
    std::unique_ptr<DistributedNoiser> reach_noiser = GetBlindHistogramNoiser(
        request.reach_dp_params(), kWorkerCount, request.noise_mechanism());
    int64_t reach_noise_baseline =
        reach_noiser->options().shift_offset * kWorkerCount;

    // Removes the noise baseline from the non empty register count.
    non_empty_register_count -= reach_noise_baseline;

    // Ensures that non_empty_register_count is at least 0.
    non_empty_register_count = std::max(0L, non_empty_register_count);

    // Ensures that non_empty_register_count is at most the input
    // frequency_vector size.
    non_empty_register_count =
        std::min(request.frequency_vector_params().register_count(),
                 non_empty_register_count);
  }

  // Estimates reach using the number of non-empty buckets and the VID sampling
  // interval width.
  ASSIGN_OR_RETURN(int64_t reach,
                   EstimateReach(non_empty_register_count,
                                 request.vid_sampling_interval_width()));

  response.set_reach(reach);
  *response.mutable_elapsed_cpu_duration() =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
