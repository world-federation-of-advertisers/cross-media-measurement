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
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "frequency_count/generate_secret_shares.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "math/distributed_noiser.h"
#include "math/open_ssl_uniform_random_generator.h"
#include "openssl/obj_mac.h"
#include "wfa/frequency_count/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/noise_mechanism.pb.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {
namespace {

using ::testing::SizeIs;
using ::wfa::frequency_count::PrngSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;

constexpr int kEdpCount = 5;
constexpr int kRegisterCount = 10;
constexpr int kMaxFrequencyPerEdp = 10;
constexpr int kMaxCombinedFrequency = 1 + kEdpCount * kMaxFrequencyPerEdp;
constexpr int kRingModulus = 128;
constexpr double kEpsilon = 1.0;
constexpr double kDelta = 0.000001;

TEST(GenerateReachAndFrequencyNoiseRegisters,
     InvalidFrequencyVectorParamsFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kMaxCombinedFrequency + 1);
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto reach_noiser = GetBlindHistogramNoiser(
      dp_params,
      /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);
  auto frequency_noiser = GetBlindHistogramNoiser(
      dp_params,
      /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);
  EXPECT_THAT(
      GenerateReachAndFrequencyNoiseRegisters(frequency_vector_params,
                                              *reach_noiser, *frequency_noiser)
          .status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "must be greater than maximum combined frequency plus 1"));
}

TEST(GenerateReachAndFrequencyNoiseRegisters,
     ValidFrequencyVectorParamsSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);
  DifferentialPrivacyParams reach_dp_params;
  reach_dp_params.set_epsilon(kEpsilon / 10.0);
  reach_dp_params.set_delta(kDelta);
  DifferentialPrivacyParams frequency_dp_params;
  frequency_dp_params.set_epsilon(kEpsilon);
  frequency_dp_params.set_delta(kDelta);
  auto reach_noiser = GetBlindHistogramNoiser(
      reach_dp_params,
      /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);
  auto frequency_noiser = GetBlindHistogramNoiser(
      frequency_dp_params,
      /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);
  int total_noise_for_reach = reach_noiser->options().shift_offset * 2;
  int total_noise_per_frequency = frequency_noiser->options().shift_offset * 2;
  int total_noise = total_noise_for_reach +
                    frequency_vector_params.maximum_combined_frequency() *
                        total_noise_per_frequency;
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> noise_registers,
      GenerateReachAndFrequencyNoiseRegisters(
          frequency_vector_params, *reach_noiser, *frequency_noiser));
  std::unordered_map<int, int> noise_frequency;
  for (auto x : noise_registers) {
    noise_frequency[x]++;
  }
  EXPECT_THAT(noise_registers, SizeIs(total_noise));
  EXPECT_LE(noise_frequency[0], total_noise_for_reach);
  for (int i = 1; i <= frequency_vector_params.maximum_combined_frequency();
       i++) {
    EXPECT_LE(noise_frequency[i], total_noise_per_frequency);
  }
  int sentinel_count = total_noise;
  for (int i = 0; i <= frequency_vector_params.maximum_combined_frequency();
       i++) {
    sentinel_count -= noise_frequency[i];
  }
  EXPECT_EQ(noise_frequency[frequency_vector_params.ring_modulus() - 1],
            sentinel_count);
}

TEST(GenerateReachOnlyNoiseRegisters, InvalidFrequencyVectorParamsFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kMaxCombinedFrequency + 1);
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  EXPECT_THAT(
      GenerateReachOnlyNoiseRegisters(frequency_vector_params, *noiser)
          .status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "must be greater than maximum combined frequency plus 1"));
}

TEST(GenerateReachOnlyNoiseRegisters, ValidFrequencyVectorParamsSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  int total_noise = noiser->options().shift_offset * 2;
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> noise_registers,
      GenerateReachOnlyNoiseRegisters(frequency_vector_params, *noiser));

  EXPECT_THAT(noise_registers, SizeIs(total_noise));
}

TEST(GetShareVectorFromFrequencyVectorShare,
     FrequencyVectorShareTypeNotSetFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_register_count(kRegisterCount);
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::FrequencyVectorShare frequency_vector_share;
  EXPECT_THAT(GetShareVectorFromFrequencyVectorShare(frequency_vector_params,
                                                     frequency_vector_share)
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Share type"));
}

TEST(GetShareVectorFromFrequencyVectorShare, ShareFromShareDataSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_register_count(kRegisterCount);
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::FrequencyVectorShare frequency_vector_share;
  std::vector<uint32_t> share_data(0, frequency_vector_params.register_count());
  frequency_vector_share.mutable_data()->mutable_values()->Add(
      share_data.begin(), share_data.end());
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> share_vector,
                       GetShareVectorFromFrequencyVectorShare(
                           frequency_vector_params, frequency_vector_share));
  EXPECT_EQ(share_vector, share_data);
}

TEST(GetShareVectorFromFrequencyVectorShare, ShareFromShareSeedSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_register_count(kRegisterCount);
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::FrequencyVectorShare frequency_vector_share;
  std::vector<uint32_t> share_data(0, frequency_vector_params.register_count());
  *frequency_vector_share.mutable_seed() =
      std::string(kBytesPerAes256Key + kBytesPerAes256Iv, 'a');
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> share_vector,
                       GetShareVectorFromFrequencyVectorShare(
                           frequency_vector_params, frequency_vector_share));
  EXPECT_THAT(share_vector, SizeIs(frequency_vector_params.register_count()));
}

TEST(GetPrngSeedFromString, InvalidStringLengthFails) {
  std::string seed_str(kBytesPerAes256Key + kBytesPerAes256Iv - 1, 'a');
  EXPECT_THAT(GetPrngSeedFromString(seed_str).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "length"));
}

TEST(GetPrngSeedFromString, ValidStringLengthSucceeds) {
  std::string seed_str(kBytesPerAes256Key + kBytesPerAes256Iv, 'a');
  ASSERT_OK_AND_ASSIGN(PrngSeed seed, GetPrngSeedFromString(seed_str));
}

TEST(GetPrngSeedFromCharVector, InvalidVectorLengthFails) {
  std::vector<unsigned char> seed_vec(
      kBytesPerAes256Key + kBytesPerAes256Iv - 1, 'a');
  EXPECT_THAT(GetPrngSeedFromCharVector(seed_vec).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "length"));
}

TEST(GetPrngSeedFromCharVector, ValidVectorLengthSucceeds) {
  std::vector<unsigned char> seed_vec(kBytesPerAes256Key + kBytesPerAes256Iv,
                                      'a');
  ASSERT_OK_AND_ASSIGN(PrngSeed seed, GetPrngSeedFromCharVector(seed_vec));
}

TEST(GenerateShareFromSeed, InvalidSeedFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_register_count(kRegisterCount);
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);
  PrngSeed seed;
  *seed.mutable_key() = std::string(kBytesPerAes256Key - 1, 'a');
  *seed.mutable_iv() = std::string(kBytesPerAes256Iv, 'b');
  EXPECT_THAT(GenerateShareFromSeed(frequency_vector_params, seed).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(GenerateShareFromSeed, ValidSeedSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_register_count(kRegisterCount);
  frequency_vector_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  frequency_vector_params.set_ring_modulus(kRingModulus);
  PrngSeed seed;
  *seed.mutable_key() = std::string(kBytesPerAes256Key, 'a');
  *seed.mutable_iv() = std::string(kBytesPerAes256Iv, 'b');
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> share_vector_from_seed,
                       GenerateShareFromSeed(frequency_vector_params, seed));
  ASSERT_EQ(share_vector_from_seed.size(),
            frequency_vector_params.register_count());
  for (int i = 0; i < share_vector_from_seed.size(); i++) {
    EXPECT_LE(share_vector_from_seed[i],
              frequency_vector_params.ring_modulus());
  }
}

TEST(VectorSubMod, InputVectorsHaveDifferentSizeFails) {
  std::vector<uint32_t> X(5, 1);
  std::vector<uint32_t> Y(6, 0);
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "same length"));
}

TEST(VectorSubMode, FirstVectorHasElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, kRingModulus};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, 0};
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "vector_x"));
}

TEST(VectorSubMode, SecondVectorHasElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, 4};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, kRingModulus};
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "vector_y"));
}

TEST(VectorSubMode, BothVectorsHaveElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, kRingModulus};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, kRingModulus};
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Both"));
}

TEST(VectorSubMod, InputVectorsHaveSameSizeSucceeds) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1};
  int kModulus = 8;
  std::vector<uint32_t> expected_result = {2, 4, 6, 0, 2, 4, 6};
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> result,
                       VectorSubMod(X, Y, kModulus));
  EXPECT_EQ(result, expected_result);
}

TEST(VectorAddMod, InputVectorsHaveDifferentSizeFails) {
  std::vector<uint32_t> X(5, 1);
  std::vector<uint32_t> Y(6, 0);
  EXPECT_THAT(VectorAddMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "same length"));
}

TEST(VectorAddMode, FirstVectorHasElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, kRingModulus};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, 0};
  EXPECT_THAT(VectorAddMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "vector_x"));
}

TEST(VectorAddMode, SecondVectorHasElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, 4};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, kRingModulus};
  EXPECT_THAT(VectorAddMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "vector_y"));
}

TEST(VectorAddMode, BothVectorsHaveElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, kRingModulus};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, kRingModulus};
  EXPECT_THAT(VectorAddMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Both"));
}

TEST(VectorAddMod, InputVectorsHaveSameSizeSucceeds) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7};
  std::vector<uint32_t> Y = {7, 1, 2, 4, 5, 3, 6};
  int kModulus = 8;
  std::vector<uint32_t> expected_result = {0, 3, 5, 0, 2, 1, 5};
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> result,
                       VectorAddMod(X, Y, kModulus));
  EXPECT_EQ(result, expected_result);
}

TEST(EstimateReach, InvalidNonEmptyRegisterCountFails) {
  EXPECT_THAT(EstimateReach(-1, 0.5).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "non-negative"));
}

TEST(EstimateReach, ZeroVidSamplingIntervalWidthFails) {
  EXPECT_THAT(EstimateReach(10, 0).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "interval width"));
}

TEST(EstimateReach, NegativeVidSamplingIntervalWidthFails) {
  EXPECT_THAT(EstimateReach(10, -0.5).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "interval width"));
}

TEST(EstimateReach, VidSamplingIntervalWidthGreaterThanOneFails) {
  EXPECT_THAT(EstimateReach(10, 1.1).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "interval width"));
}

TEST(EstimateReach, ValidInputSucceeds) {
  ASSERT_OK_AND_ASSIGN(int64_t reach_1, EstimateReach(10, 0.5));
  EXPECT_EQ(reach_1, 20);
  ASSERT_OK_AND_ASSIGN(int64_t reach_2, EstimateReach(10, 1.0));
  EXPECT_EQ(reach_2, 10);
}

TEST(CombineFrequencyVectorShares, EmptyFrequencyVectorSharesFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_ring_modulus(kRingModulus);
  CompleteAggregationPhaseRequest request;
  EXPECT_THAT(CombineFrequencyVectorShares(frequency_vector_params,
                                           request.frequency_vector_shares())
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least one"));
}

TEST(CombineFrequencyVectorShares, InvalidRingModulusFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_ring_modulus(1);
  CompleteAggregationPhaseRequest request;
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(1);
  EXPECT_THAT(CombineFrequencyVectorShares(frequency_vector_params,
                                           request.frequency_vector_shares())
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "modulus"));
}

TEST(CombineFrequencyVectorShares, InvalidInputShareFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_ring_modulus(kRingModulus);
  CompleteAggregationPhaseRequest request;
  std::vector<uint32_t> share_vector_1 = {1, 0, 1, 0, 1};
  std::vector<uint32_t> share_vector_2 = {kRingModulus, 1, 0, 1, 0};
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_1.begin(), share_vector_1.end());
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_2.begin(), share_vector_2.end());
  EXPECT_THAT(CombineFrequencyVectorShares(frequency_vector_params,
                                           request.frequency_vector_shares())
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "must be less than the modulus"));
}

TEST(CombineFrequencyVectorShares, InputSharesHaveDifferentLengthFails) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_ring_modulus(kRingModulus);
  CompleteAggregationPhaseRequest request;
  std::vector<uint32_t> share_vector_1 = {0, 1, 0, 1, 3};
  std::vector<uint32_t> share_vector_2 = {2, 1, 0, 1, 0, 5};
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_1.begin(), share_vector_1.end());
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_2.begin(), share_vector_2.end());
  EXPECT_THAT(CombineFrequencyVectorShares(frequency_vector_params,
                                           request.frequency_vector_shares())
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "length"));
}

TEST(CombineFrequencyVectorShares, ValidInputSharesAndParamsSucceeds) {
  ShareShuffleFrequencyVectorParams frequency_vector_params;
  frequency_vector_params.set_ring_modulus(10);
  CompleteAggregationPhaseRequest request;
  std::vector<uint32_t> share_vector_1 = {1, 0, 1, 0, 1};
  std::vector<uint32_t> share_vector_2 = {5, 1, 0, 1, 0};
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_1.begin(), share_vector_1.end());
  request.add_frequency_vector_shares()->mutable_share_vector()->Add(
      share_vector_2.begin(), share_vector_2.end());
  ASSERT_OK_AND_ASSIGN(
      auto combined_share,
      CombineFrequencyVectorShares(frequency_vector_params,
                                   request.frequency_vector_shares()));
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
