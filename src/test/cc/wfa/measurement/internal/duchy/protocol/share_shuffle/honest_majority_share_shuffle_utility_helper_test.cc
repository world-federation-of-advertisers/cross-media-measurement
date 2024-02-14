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
#include "any_sketch/crypto/secret_share_generator.h"
#include "common_cpp/macros/macros.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "math/distributed_noiser.h"
#include "math/open_ssl_uniform_random_generator.h"
#include "openssl/obj_mac.h"
#include "wfa/any_sketch/secret_share.pb.h"
#include "wfa/measurement/internal/duchy/noise_mechanism.pb.h"
#include "wfa/measurement/internal/duchy/protocol/common/noise_parameters_computation.h"
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {
namespace {

using ::testing::SizeIs;
using ::wfa::any_sketch::PrngSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;

constexpr int kEdpCount = 5;
constexpr int kRegisterCount = 10;
constexpr int kBytesPerRegister = 1;
constexpr int kMaxFrequencyPerEdp = 10;
constexpr int kMaxCombinedFrequency = 1 + kEdpCount * kMaxFrequencyPerEdp;
constexpr int kRingModulus = 128;
constexpr double kEpsilon = 1.0;
constexpr double kDelta = 0.000001;

TEST(GenerateNoiseRegisters, InvalidSketchParamsFails) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kMaxCombinedFrequency + 1);
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  EXPECT_THAT(
      GenerateNoiseRegisters(sketch_params, *noiser).status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "must be greater than maximum combined frequency plus 1"));
}

TEST(GenerateNoiseRegisters, ValidSketchParamsSucceeds) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  int total_noise_per_frequency = noiser->options().shift_offset * 2;
  int total_noise = (1 + sketch_params.maximum_combined_frequency()) *
                    total_noise_per_frequency;
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> noise_registers,
                       GenerateNoiseRegisters(sketch_params, *noiser));
  std::unordered_map<int, int> noise_frequency;
  for (auto x : noise_registers) {
    noise_frequency[x]++;
  }
  EXPECT_THAT(noise_registers, SizeIs(total_noise));
  for (int i = 0; i <= sketch_params.maximum_combined_frequency(); i++) {
    EXPECT_LE(noise_frequency[i], total_noise_per_frequency);
  }
  int sentinel_count = total_noise;
  for (int i = 0; i <= sketch_params.maximum_combined_frequency(); i++) {
    sentinel_count -= noise_frequency[i];
  }
  EXPECT_EQ(noise_frequency[sketch_params.ring_modulus() - 1], sentinel_count);
}

TEST(GetShareVectorFromSketchShare, SketchShareTypeNotSetFails) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_register_count(kRegisterCount);
  sketch_params.set_bytes_per_register(kBytesPerRegister);
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::SketchShare sketch_share;
  EXPECT_THAT(
      GetShareVectorFromSketchShare(sketch_params, sketch_share).status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "Share type"));
}

TEST(GetShareVectorFromSketchShare, ShareFromShareDataSucceeds) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_register_count(kRegisterCount);
  sketch_params.set_bytes_per_register(kBytesPerRegister);
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::SketchShare sketch_share;
  std::vector<uint32_t> share_data(0, sketch_params.register_count());
  sketch_share.mutable_data()->mutable_values()->Add(share_data.begin(),
                                                     share_data.end());
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      GetShareVectorFromSketchShare(sketch_params, sketch_share));
  EXPECT_EQ(share_vector, share_data);
}

TEST(GetShareVectorFromSketchShare, ShareFromShareSeedSucceeds) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_register_count(kRegisterCount);
  sketch_params.set_bytes_per_register(kBytesPerRegister);
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);

  CompleteShufflePhaseRequest::SketchShare sketch_share;
  std::vector<uint32_t> share_data(0, sketch_params.register_count());
  *sketch_share.mutable_seed() =
      std::string(kBytesPerAes256Key + kBytesPerAes256Iv, 'a');
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      GetShareVectorFromSketchShare(sketch_params, sketch_share));
  EXPECT_THAT(share_vector, SizeIs(sketch_params.register_count()));
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
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_register_count(kRegisterCount);
  sketch_params.set_bytes_per_register(kBytesPerRegister);
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);
  PrngSeed seed;
  *seed.mutable_key() = std::string(kBytesPerAes256Key - 1, 'a');
  *seed.mutable_iv() = std::string(kBytesPerAes256Iv, 'b');
  EXPECT_THAT(GenerateShareFromSeed(sketch_params, seed).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(GenerateShareFromSeed, ValidSeedSucceeds) {
  ShareShuffleSketchParams sketch_params;
  sketch_params.set_register_count(kRegisterCount);
  sketch_params.set_bytes_per_register(kBytesPerRegister);
  sketch_params.set_maximum_combined_frequency(kMaxCombinedFrequency);
  sketch_params.set_ring_modulus(kRingModulus);
  PrngSeed seed;
  *seed.mutable_key() = std::string(kBytesPerAes256Key, 'a');
  *seed.mutable_iv() = std::string(kBytesPerAes256Iv, 'b');
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> share_vector_from_seed,
                       GenerateShareFromSeed(sketch_params, seed));
  ASSERT_EQ(share_vector_from_seed.size(), sketch_params.register_count());
  for (int i = 0; i < share_vector_from_seed.size(); i++) {
    EXPECT_LE(share_vector_from_seed[i], sketch_params.ring_modulus());
  }
}

TEST(VectorSubMod, InputVectorsHaveDifferentSizeFails) {
  std::vector<uint32_t> X(5, 1);
  std::vector<uint32_t> Y(6, 0);
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "same length"));
}

TEST(VectorSubMode, InputVectorHasElementGreaterThanOrEqualToModulusFails) {
  std::vector<uint32_t> X = {1, 2, 3, 4, 5, 6, 7, kRingModulus};
  std::vector<uint32_t> Y = {7, 6, 5, 4, 3, 2, 1, 0};
  EXPECT_THAT(VectorSubMod(X, Y, kRingModulus).status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "modulus"));
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

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
