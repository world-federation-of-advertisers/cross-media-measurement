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
#include "wfa/measurement/internal/duchy/protocol/honest_majority_share_shuffle_methods.pb.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/noise_parameters_computation.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {
namespace {

using ::testing::IsSupersetOf;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;
using ::wfa::StatusIs;
using ::wfa::any_sketch::PrngSeed;
using ::wfa::any_sketch::SecretShare;
using ::wfa::any_sketch::SecretShareParameter;
using ::wfa::any_sketch::crypto::GenerateSecretShares;
using ::wfa::math::CreatePrngFromSeed;
using ::wfa::math::kBytesPerAes256Iv;
using ::wfa::math::kBytesPerAes256Key;
using ::wfa::math::UniformPseudorandomGenerator;
using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::NoiseMechanism;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteShufflePhaseRequest;
using ::wfa::measurement::internal::duchy::protocol::
    CompleteShufflePhaseResponse;

constexpr int kEdpCount = 5;
constexpr int kRegisterCount = 10;
constexpr int kBytesPerRegister = 1;
constexpr int kMaxFrequencyPerEdp = 10;
constexpr int kMaxCombinedFrequency = 1 + kEdpCount * kMaxFrequencyPerEdp;
constexpr int kRingModulus = 128;
constexpr double kEpsilon = 1.0;
constexpr double kDelta = 0.000001;

class ShufflePhaseTestData {
 public:
  ShufflePhaseTestData() {
    *request_.mutable_common_random_seed() =
        std::string(kBytesPerAes256Key + kBytesPerAes256Iv, 'a');
    request_.set_noise_mechanism(NoiseMechanism::DISCRETE_GAUSSIAN);
    PrngSeed seed = GenerateRandomSeed();
    auto prng = CreatePrngFromSeed(seed);
    if (!prng.ok()) {
      LOG(FATAL) << "Cannot create Prng from seed.\n";
    }
    prng_ = std::move(prng.value());
  }

  void SetNonAggregatorOrder(
      const CompleteShufflePhaseRequest::NonAggregatorOrder& order) {
    request_.set_order(order);
  }

  void SetSketchParams(int register_count, int bytes_per_register,
                       int maximum_combined_frequency, int ring_modulus) {
    request_.mutable_sketch_params()->set_register_count(register_count);
    request_.mutable_sketch_params()->set_bytes_per_register(
        bytes_per_register);
    request_.mutable_sketch_params()->set_maximum_combined_frequency(
        maximum_combined_frequency);
    request_.mutable_sketch_params()->set_ring_modulus(ring_modulus);
  }

  void SetDifferentialPrivacyParams(double eps, double delta) {
    request_.mutable_dp_params()->set_epsilon(eps);
    request_.mutable_dp_params()->set_delta(delta);
  }

  void AddSeedToSketchShares(const std::string& seed) {
    *request_.add_sketch_shares()->mutable_seed() = seed;
  }

  void AddShareToSketchShares(const std::vector<uint32_t>& data) {
    request_.add_sketch_shares()->mutable_data()->mutable_values()->Add(
        data.begin(), data.end());
  }

  void AddSeedToSketchShares(const SecretShare& secret_share) {
    *request_.add_sketch_shares()->mutable_seed() =
        (secret_share.share_seed().key() + secret_share.share_seed().iv());
  }

  void AddShareToSketchShares(const SecretShare& secret_share) {
    request_.add_sketch_shares()->mutable_data()->mutable_values()->Add(
        secret_share.share_vector().begin(), secret_share.share_vector().end());
  }

  PrngSeed GenerateRandomSeed() {
    std::string key;
    std::string iv;
    key.resize(kBytesPerAes256Key);
    iv.resize(kBytesPerAes256Iv);

    RAND_bytes(reinterpret_cast<uint8_t*>(key.data()), key.size());
    RAND_bytes(reinterpret_cast<uint8_t*>(iv.data()), iv.size());

    PrngSeed seed;
    *seed.mutable_key() = key;
    *seed.mutable_iv() = iv;

    return seed;
  }

  absl::StatusOr<std::vector<uint32_t>> GenerateUniformRandomRange(
      int64_t size, uint32_t modulus) {
    ASSIGN_OR_RETURN(std::vector<uint32_t> ret,
                     prng_->GenerateUniformRandomRange(size, modulus));
    return ret;
  }

  absl::StatusOr<CompleteShufflePhaseResponse> RunShufflePhase() {
    ASSIGN_OR_RETURN(auto response, CompleteShufflePhase(request_));
    return response;
  }

 protected:
  std::vector<uint32_t> input_;
  std::unique_ptr<UniformPseudorandomGenerator> prng_;
  CompleteShufflePhaseRequest request_;
};

TEST(ShufflePhaseAtNonAggregator, InvalidRegisterCountFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(/*register_count=*/0, /*bytes_per_registers=*/1,
                            /*maximum_combined_frequency=*/10,
                            /*ring_modulus=*/128);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "register count"));
}

TEST(ShufflePhaseAtNonAggregator, InvalidRingModulusFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(/*register_count=*/100, /*bytes_per_registers=*/1,
                            /*maximum_combined_frequency=*/10,
                            /*ring_modulus=*/1);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least 2"));
}

TEST(ShufflePhaseAtNonAggregator, InvalidRingModulusAndMaxFrequencyPairFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(/*register_count=*/100, /*bytes_per_registers=*/1,
                            /*maximum_combined_frequency=*/4,
                            /*ring_modulus=*/5);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "plus one"));
}

TEST(ShufflePhaseAtNonAggregator, RingModulusDoesNotFitTheRegisterFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(/*register_count=*/100, /*bytes_per_registers=*/1,
                            /*maximum_combined_frequency=*/127,
                            /*ring_modulus=*/257);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "bit length"));
}

TEST(ShufflePhaseAtNonAggregator, InputSizeDoesNotMatchTheConfigFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::vector<uint32_t> share_data(kRegisterCount - 1, 1);
  test_data.AddShareToSketchShares(share_data);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "invalid size"));
}

TEST(ShufflePhaseAtNonAggregator, EmptySketchSharesFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));
}

TEST(ShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsNotSpecifiedSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToSketchShares(share_data);
  EXPECT_EQ(test_data.RunShufflePhase().status(), absl::OkStatus());
}

TEST(ShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsSpecifiedFails) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToSketchShares(share_data);
  EXPECT_THAT(test_data.RunShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "order"));
}

TEST(ShufflePhaseAtNonAggregator,
     SketchSharesContainOnlyShareVectorAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddShareToSketchShares(share_vector);
  }

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunShufflePhase());

  std::vector<uint32_t> combined_sketch(ret.combined_sketch().begin(),
                                        ret.combined_sketch().end());
  ASSERT_THAT(combined_sketch, SizeIs(kRegisterCount));

  std::vector<uint32_t> expected_combined_sketch(kRegisterCount);
  for (int i = 0; i < expected_combined_sketch.size(); i++) {
    expected_combined_sketch[i] = kEdpCount * share_vector[i] % kRingModulus;
  }

  EXPECT_THAT(combined_sketch,
              testing::UnorderedElementsAreArray(expected_combined_sketch));
}

TEST(ShufflePhaseAtNonAggregator,
     SketchSharesContainOnlySeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddSeedToSketchShares(share_seed);
  }
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunShufflePhase());
  std::vector<uint32_t> combined_sketch(ret.combined_sketch().begin(),
                                        ret.combined_sketch().end());
  ASSERT_THAT(combined_sketch, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_single_share_vector,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_sketch(kRegisterCount);
  for (int i = 0; i < expected_combined_sketch.size(); i++) {
    expected_combined_sketch[i] =
        kEdpCount * expected_single_share_vector[i] % kRingModulus;
  }

  EXPECT_THAT(combined_sketch,
              testing::UnorderedElementsAreArray(expected_combined_sketch));
}

TEST(ShufflePhaseAtNonAggregator,
     SketchSharesContainShareVectorAndSeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToSketchShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToSketchShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunShufflePhase());
  std::vector<uint32_t> combined_sketch(ret.combined_sketch().begin(),
                                        ret.combined_sketch().end());
  ASSERT_THAT(combined_sketch, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_sketch(kRegisterCount);
  for (int i = 0; i < expected_combined_sketch.size(); i++) {
    expected_combined_sketch[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  EXPECT_THAT(combined_sketch,
              testing::UnorderedElementsAreArray(expected_combined_sketch));
}

TEST(ShufflePhaseAtNonAggregator, ShufflePhaseWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetSketchParams(kRegisterCount, kBytesPerRegister,
                            kMaxCombinedFrequency, kRingModulus);
  test_data.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToSketchShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToSketchShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunShufflePhase());
  std::vector<uint32_t> combined_sketch(ret.combined_sketch().begin(),
                                        ret.combined_sketch().end());
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  int64_t total_noise_registers_count_per_duchy =
      noiser->options().shift_offset * 2 * (1 + kMaxCombinedFrequency);

  ASSERT_THAT(
      combined_sketch,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_sketch_without_noise(kRegisterCount);
  for (int i = 0; i < expected_combined_sketch_without_noise.size(); i++) {
    expected_combined_sketch_without_noise[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  EXPECT_THAT(combined_sketch,
              testing::IsSupersetOf(expected_combined_sketch_without_noise));
}

TEST(ShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithoutDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.SetSketchParams(kRegisterCount, kBytesPerRegister,
                              kMaxCombinedFrequency, kRingModulus);
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ShufflePhaseTestData test_data_2;
  test_data_2.SetSketchParams(kRegisterCount, kBytesPerRegister,
                              kMaxCombinedFrequency, kRingModulus);
  test_data_2.SetNonAggregatorOrder(CompleteShufflePhaseRequest::SECOND);

  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> input_a,
                       test_data_1.GenerateUniformRandomRange(
                           kRegisterCount, kMaxFrequencyPerEdp));
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> input_b,
                       test_data_2.GenerateUniformRandomRange(
                           kRegisterCount, kMaxFrequencyPerEdp));

  SecretShareParameter ss_params;
  ss_params.set_modulus(kRingModulus);

  ASSERT_OK_AND_ASSIGN(SecretShare secret_share_a,
                       GenerateSecretShares(ss_params, input_a));

  ASSERT_OK_AND_ASSIGN(SecretShare secret_share_b,
                       GenerateSecretShares(ss_params, input_b));
  test_data_1.AddShareToSketchShares(secret_share_a);
  test_data_2.AddSeedToSketchShares(secret_share_a);

  test_data_1.AddShareToSketchShares(secret_share_b);
  test_data_2.AddSeedToSketchShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunShufflePhase());

  std::vector<uint32_t> combined_sketch_share_1(ret_1.combined_sketch().begin(),
                                                ret_1.combined_sketch().end());
  std::vector<uint32_t> combined_sketch_share_2(ret_2.combined_sketch().begin(),
                                                ret_2.combined_sketch().end());

  std::vector<uint32_t> combined_sketch(combined_sketch_share_1.size());
  for (int i = 0; i < combined_sketch.size(); i++) {
    combined_sketch[i] =
        ((combined_sketch_share_1[i] + combined_sketch_share_2[i]) %
         kRingModulus);
  }

  std::vector<uint32_t> combined_input(kRegisterCount);
  for (int i = 0; i < kRegisterCount; i++) {
    combined_input[i] = (input_a[i] + input_b[i] % kRingModulus);
  }

  EXPECT_THAT(combined_sketch,
              testing::UnorderedElementsAreArray(combined_input));
}

TEST(ShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.SetSketchParams(kRegisterCount, kBytesPerRegister,
                              kMaxCombinedFrequency, kRingModulus);
  test_data_1.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ShufflePhaseTestData test_data_2;
  test_data_2.SetSketchParams(kRegisterCount, kBytesPerRegister,
                              kMaxCombinedFrequency, kRingModulus);
  test_data_2.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  test_data_2.SetNonAggregatorOrder(CompleteShufflePhaseRequest::SECOND);

  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> input_a,
                       test_data_1.GenerateUniformRandomRange(
                           kRegisterCount, kMaxFrequencyPerEdp));
  ASSERT_OK_AND_ASSIGN(std::vector<uint32_t> input_b,
                       test_data_2.GenerateUniformRandomRange(
                           kRegisterCount, kMaxFrequencyPerEdp));

  SecretShareParameter ss_params;
  ss_params.set_modulus(kRingModulus);

  ASSERT_OK_AND_ASSIGN(SecretShare secret_share_a,
                       GenerateSecretShares(ss_params, input_a));

  ASSERT_OK_AND_ASSIGN(SecretShare secret_share_b,
                       GenerateSecretShares(ss_params, input_b));
  test_data_1.AddShareToSketchShares(secret_share_a);
  test_data_2.AddSeedToSketchShares(secret_share_a);

  test_data_1.AddShareToSketchShares(secret_share_b);
  test_data_2.AddSeedToSketchShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunShufflePhase());

  std::vector<uint32_t> combined_sketch_share_1(ret_1.combined_sketch().begin(),
                                                ret_1.combined_sketch().end());
  std::vector<uint32_t> combined_sketch_share_2(ret_2.combined_sketch().begin(),
                                                ret_2.combined_sketch().end());

  std::vector<uint32_t> combined_sketch(combined_sketch_share_1.size());
  for (int i = 0; i < combined_sketch.size(); i++) {
    combined_sketch[i] =
        ((combined_sketch_share_1[i] + combined_sketch_share_2[i]) %
         kRingModulus);
  }

  std::vector<uint32_t> combined_input(kRegisterCount);
  for (int i = 0; i < kRegisterCount; i++) {
    combined_input[i] = (input_a[i] + input_b[i] % kRingModulus);
  }

  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  int64_t total_noise_registers_count_per_duchy =
      noiser->options().shift_offset * 2 * (1 + kMaxCombinedFrequency);

  ASSERT_THAT(
      combined_sketch,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  EXPECT_THAT(combined_sketch, testing::IsSupersetOf(combined_input));

  std::unordered_map<int, int> combined_input_frequency;
  for (auto x : combined_input) {
    combined_input_frequency[x]++;
  }
  std::unordered_map<int, int> noisy_frequency;
  for (auto x : combined_sketch) {
    noisy_frequency[x]++;
  }

  int total_noise_added = 2 * total_noise_registers_count_per_duchy;

  // Verifies that all noises are within the bound.
  for (int i = 0; i <= kMaxCombinedFrequency; i++) {
    EXPECT_LE(combined_input_frequency[i], noisy_frequency[i]);
    EXPECT_LE(noisy_frequency[i] - combined_input_frequency[i],
              total_noise_added);
  }

  // The noisy frequency map [f_0, ..., f_{kMaxCombinedFrequency},
  // f_{kRingModulus-1}] have exactly (2 + kMaxCombinedFrequency) elements.
  EXPECT_EQ(noisy_frequency.size(), 2 + kMaxCombinedFrequency);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
