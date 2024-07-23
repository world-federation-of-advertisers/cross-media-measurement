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
#include "wfa/measurement/internal/duchy/protocol/share_shuffle/honest_majority_share_shuffle_utility_helper.h"

namespace wfa::measurement::internal::duchy::protocol::share_shuffle {
namespace {

using ::testing::IsSupersetOf;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;
using ::wfa::StatusIs;
using ::wfa::frequency_count::GenerateSecretShares;
using ::wfa::frequency_count::PrngSeed;
using ::wfa::frequency_count::SecretShare;
using ::wfa::frequency_count::SecretShareParameter;
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
using ::wfa::measurement::internal::duchy::protocol::common::
    GetBlindHistogramNoiser;

constexpr int kEdpCount = 5;
constexpr int kRegisterCount = 100;
constexpr int kMaxFrequencyPerEdp = 2;
constexpr int kMaxCombinedFrequency = 1 + kEdpCount * kMaxFrequencyPerEdp;
constexpr int kRingModulus = 127;
constexpr double kEpsilon = 1.0;
constexpr double kDelta = 0.1;
constexpr double kVidSamplingIntervalWidth = 0.5;

struct MpcResult {
  int64_t reach;
  absl::flat_hash_map<int64_t, double> frequency_distribution;
};

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

class ShufflePhaseTestData {
 public:
  ShufflePhaseTestData() {
    *request_.mutable_common_random_seed() =
        std::string(kBytesPerAes256Key + kBytesPerAes256Iv, 'a');
    request_.set_noise_mechanism(NoiseMechanism::DISCRETE_GAUSSIAN);
    SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                             kRingModulus);
    SetDifferentialPrivacyParams(kEpsilon, kDelta);

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

  void SetFrequencyVectorParams(int register_count,
                                int maximum_combined_frequency,
                                int ring_modulus) {
    request_.mutable_frequency_vector_params()->set_register_count(
        register_count);
    request_.mutable_frequency_vector_params()->set_maximum_combined_frequency(
        maximum_combined_frequency);
    request_.mutable_frequency_vector_params()->set_ring_modulus(ring_modulus);
  }

  void SetReachOnlyDifferentialPrivacyParams(double eps, double delta) {
    request_.mutable_reach_dp_params()->set_epsilon(eps);
    request_.mutable_reach_dp_params()->set_delta(delta);
  }

  void SetDifferentialPrivacyParams(double eps, double delta) {
    request_.mutable_reach_dp_params()->set_epsilon(eps / 10.0);
    request_.mutable_reach_dp_params()->set_delta(delta);
    request_.mutable_frequency_dp_params()->set_epsilon(eps);
    request_.mutable_frequency_dp_params()->set_delta(delta);
  }

  void ClearDifferentialPrivacyParams() {
    request_.clear_reach_dp_params();
    request_.clear_frequency_dp_params();
  }

  void AddSeedToFrequencyVectorShares(const std::string& seed) {
    *request_.add_frequency_vector_shares()->mutable_seed() = seed;
  }

  void AddShareToFrequencyVectorShares(const std::vector<uint32_t>& data) {
    request_.add_frequency_vector_shares()
        ->mutable_data()
        ->mutable_values()
        ->Add(data.begin(), data.end());
  }

  void AddSeedToFrequencyVectorShares(const SecretShare& secret_share) {
    *request_.add_frequency_vector_shares()->mutable_seed() =
        (secret_share.share_seed().key() + secret_share.share_seed().iv());
  }

  void AddShareToFrequencyVectorShares(const SecretShare& secret_share) {
    request_.add_frequency_vector_shares()
        ->mutable_data()
        ->mutable_values()
        ->Add(secret_share.share_vector().begin(),
              secret_share.share_vector().end());
  }

  absl::StatusOr<std::vector<uint32_t>> GenerateUniformRandomRange(
      int64_t size, uint32_t modulus) {
    ASSIGN_OR_RETURN(std::vector<uint32_t> ret,
                     prng_->GenerateUniformRandomRange(size, modulus));
    return ret;
  }

  absl::StatusOr<CompleteShufflePhaseResponse>
  RunReachAndFrequencyShufflePhase() {
    ASSIGN_OR_RETURN(auto response,
                     CompleteReachAndFrequencyShufflePhase(request_));
    return response;
  }

  absl::StatusOr<CompleteShufflePhaseResponse> RunReachOnlyShufflePhase() {
    ASSIGN_OR_RETURN(auto response, CompleteReachOnlyShufflePhase(request_));
    return response;
  }

 protected:
  std::vector<uint32_t> input_;
  std::unique_ptr<UniformPseudorandomGenerator> prng_;
  CompleteShufflePhaseRequest request_;
};

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator, InvalidRegisterCountFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/0,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/128);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "register count"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator, InvalidRingModulusFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/1);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least 2"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     InvalidRingModulusAndMaxFrequencyPairFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/4,
                                     /*ring_modulus=*/5);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "plus one"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     InputSizeDoesNotMatchTheConfigFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::vector<uint32_t> share_data(kRegisterCount - 1, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "invalid size"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     EmptyFrequencyVectorSharesFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsNotSpecifiedSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_EQ(test_data.RunReachAndFrequencyShufflePhase().status(),
            absl::OkStatus());
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsSpecifiedFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_THAT(test_data.RunReachAndFrequencyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "order"));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainOnlyShareVectorAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddShareToFrequencyVectorShares(share_vector);
  }

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachAndFrequencyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        kEdpCount * share_vector[i] % kRingModulus;
  }

  EXPECT_THAT(
      combined_frequency_vector,
      testing::UnorderedElementsAreArray(expected_combined_frequency_vector));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainOnlySeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddSeedToFrequencyVectorShares(share_seed);
  }
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachAndFrequencyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_single_share_vector,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        kEdpCount * expected_single_share_vector[i] % kRingModulus;
  }

  EXPECT_THAT(
      combined_frequency_vector,
      testing::UnorderedElementsAreArray(expected_combined_frequency_vector));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainShareVectorAndSeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToFrequencyVectorShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachAndFrequencyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  EXPECT_THAT(
      combined_frequency_vector,
      testing::UnorderedElementsAreArray(expected_combined_frequency_vector));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     ShufflePhaseWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetDifferentialPrivacyParams(kEpsilon, kDelta);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToFrequencyVectorShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachAndFrequencyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
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
  int64_t total_noise_registers_count_per_duchy =
      reach_noiser->options().shift_offset * 2 +
      kMaxCombinedFrequency * frequency_noiser->options().shift_offset * 2;

  ASSERT_THAT(
      combined_frequency_vector,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector_without_noise(
      kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector_without_noise.size();
       i++) {
    expected_combined_frequency_vector_without_noise[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  EXPECT_THAT(
      combined_frequency_vector,
      testing::IsSupersetOf(expected_combined_frequency_vector_without_noise));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithoutDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.ClearDifferentialPrivacyParams();
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ShufflePhaseTestData test_data_2;
  test_data_2.ClearDifferentialPrivacyParams();
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
  test_data_1.AddShareToFrequencyVectorShares(secret_share_a);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_a);

  test_data_1.AddShareToFrequencyVectorShares(secret_share_b);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunReachAndFrequencyShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunReachAndFrequencyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector_share_1(
      ret_1.combined_frequency_vector().begin(),
      ret_1.combined_frequency_vector().end());
  std::vector<uint32_t> combined_frequency_vector_share_2(
      ret_2.combined_frequency_vector().begin(),
      ret_2.combined_frequency_vector().end());

  std::vector<uint32_t> combined_frequency_vector(
      combined_frequency_vector_share_1.size());
  for (int i = 0; i < combined_frequency_vector.size(); i++) {
    combined_frequency_vector[i] = ((combined_frequency_vector_share_1[i] +
                                     combined_frequency_vector_share_2[i]) %
                                    kRingModulus);
  }

  std::vector<uint32_t> combined_input(kRegisterCount);
  for (int i = 0; i < kRegisterCount; i++) {
    combined_input[i] = (input_a[i] + input_b[i] % kRingModulus);
  }

  EXPECT_THAT(combined_frequency_vector,
              testing::UnorderedElementsAreArray(combined_input));
}

TEST(ReachAndFrequencyShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ShufflePhaseTestData test_data_2;
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
  test_data_1.AddShareToFrequencyVectorShares(secret_share_a);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_a);

  test_data_1.AddShareToFrequencyVectorShares(secret_share_b);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunReachAndFrequencyShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunReachAndFrequencyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector_share_1(
      ret_1.combined_frequency_vector().begin(),
      ret_1.combined_frequency_vector().end());
  std::vector<uint32_t> combined_frequency_vector_share_2(
      ret_2.combined_frequency_vector().begin(),
      ret_2.combined_frequency_vector().end());

  std::vector<uint32_t> combined_frequency_vector(
      combined_frequency_vector_share_1.size());
  for (int i = 0; i < combined_frequency_vector.size(); i++) {
    combined_frequency_vector[i] = ((combined_frequency_vector_share_1[i] +
                                     combined_frequency_vector_share_2[i]) %
                                    kRingModulus);
  }

  std::vector<uint32_t> combined_input(kRegisterCount);
  for (int i = 0; i < kRegisterCount; i++) {
    combined_input[i] = (input_a[i] + input_b[i] % kRingModulus);
  }

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
  int64_t total_noise_registers_count_per_duchy =
      reach_noiser->options().shift_offset * 2 +
      kMaxCombinedFrequency * frequency_noiser->options().shift_offset * 2;

  ASSERT_THAT(
      combined_frequency_vector,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  EXPECT_THAT(combined_frequency_vector, testing::IsSupersetOf(combined_input));

  absl::flat_hash_map<int, int> combined_input_frequency;
  for (const auto reg : combined_input) {
    combined_input_frequency[reg]++;
  }
  absl::flat_hash_map<int, int> noisy_frequency;
  for (const auto reg : combined_frequency_vector) {
    noisy_frequency[reg]++;
  }

  int total_reach_noise_per_duchy = reach_noiser->options().shift_offset * 2;
  int total_frequency_noise_per_bucket_per_duchy =
      frequency_noiser->options().shift_offset * 2;

  // Verifies that all noises are within the bound.
  EXPECT_LE(combined_input_frequency[0], noisy_frequency[0]);
  EXPECT_LE(noisy_frequency[0] - combined_input_frequency[0],
            2 * total_reach_noise_per_duchy);
  for (int i = 1; i <= kMaxCombinedFrequency; i++) {
    EXPECT_LE(combined_input_frequency[i], noisy_frequency[i]);
    EXPECT_LE(noisy_frequency[i] - combined_input_frequency[i],
              2 * total_frequency_noise_per_bucket_per_duchy);
  }

  // The noisy frequency map [f_0, ..., f_{kMaxCombinedFrequency},
  // f_{kRingModulus-1}] have exactly (2 + kMaxCombinedFrequency) elements.
  EXPECT_EQ(noisy_frequency.size(), 2 + kMaxCombinedFrequency);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator, InvalidRegisterCountFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/0,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/128);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "register count"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator, InvalidRingModulusFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/1);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least 2"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     InvalidRingModulusAndMaxFrequencyPairFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/4,
                                     /*ring_modulus=*/5);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "plus one"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator, NonPrimeRingModulusFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/127,
                                     /*ring_modulus=*/130);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "prime"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     InputSizeDoesNotMatchTheConfigFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::vector<uint32_t> share_data(kRegisterCount - 1, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "invalid size"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator, EmptyFrequencyVectorSharesFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsNotSpecifiedSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_EQ(test_data.RunReachOnlyShufflePhase().status(), absl::OkStatus());
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     NonAggregatorOrderNotSpecifiedAndDpParamsSpecifiedFails) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetReachOnlyDifferentialPrivacyParams(kEpsilon, kDelta);
  std::vector<uint32_t> share_data(kRegisterCount, 1);
  test_data.AddShareToFrequencyVectorShares(share_data);
  EXPECT_THAT(test_data.RunReachOnlyShufflePhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "order"));
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainOnlyShareVectorAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddShareToFrequencyVectorShares(share_vector);
  }

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachOnlyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        kEdpCount * share_vector[i] % kRingModulus;
  }
  int expected_non_empty_register_count = 0;
  for (const auto reg : expected_combined_frequency_vector) {
    if (reg != 0) {
      expected_non_empty_register_count++;
    }
  }

  int non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if ((reg % kRingModulus) != 0) {
      non_empty_register_count++;
    }
  }

  EXPECT_EQ(non_empty_register_count, expected_non_empty_register_count);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainOnlySeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  for (int i = 0; i < kEdpCount; i++) {
    test_data.AddSeedToFrequencyVectorShares(share_seed);
  }
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachOnlyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_single_share_vector,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        kEdpCount * expected_single_share_vector[i] % kRingModulus;
  }

  int expected_non_empty_register_count = 0;
  for (const auto reg : expected_combined_frequency_vector) {
    if (reg != 0) {
      expected_non_empty_register_count++;
    }
  }

  int non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if ((reg % kRingModulus) != 0) {
      non_empty_register_count++;
    }
  }

  EXPECT_EQ(non_empty_register_count, expected_non_empty_register_count);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     FrequencyVectorSharesContainShareVectorAndSeedsAndNoDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.ClearDifferentialPrivacyParams();
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToFrequencyVectorShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachOnlyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  ASSERT_THAT(combined_frequency_vector, SizeIs(kRegisterCount));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector(kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector.size(); i++) {
    expected_combined_frequency_vector[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  int expected_non_empty_register_count = 0;
  for (const auto reg : expected_combined_frequency_vector) {
    if (reg != 0) {
      expected_non_empty_register_count++;
    }
  }

  int non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if ((reg % kRingModulus) != 0) {
      non_empty_register_count++;
    }
  }

  EXPECT_EQ(non_empty_register_count, expected_non_empty_register_count);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator, ShufflePhaseWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data;
  test_data.SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                                     kRingModulus);
  test_data.SetReachOnlyDifferentialPrivacyParams(kEpsilon, kDelta);
  test_data.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector,
      test_data.GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector);

  std::string share_seed(kBytesPerAes256Key + kBytesPerAes256Iv, 'c');
  test_data.AddSeedToFrequencyVectorShares(share_seed);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret,
                       test_data.RunReachOnlyShufflePhase());
  std::vector<uint32_t> combined_frequency_vector(
      ret.combined_frequency_vector().begin(),
      ret.combined_frequency_vector().end());
  DifferentialPrivacyParams dp_params;
  dp_params.set_epsilon(kEpsilon);
  dp_params.set_delta(kDelta);
  auto noiser = GetBlindHistogramNoiser(dp_params,
                                        /*contributors_count=*/2,
                                        NoiseMechanism::DISCRETE_GAUSSIAN);
  int64_t total_noise_registers_count_per_duchy =
      noiser->options().shift_offset * 2;

  ASSERT_THAT(
      combined_frequency_vector,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  PrngSeed seed;
  *seed.mutable_key() = share_seed.substr(0, kBytesPerAes256Key);
  *seed.mutable_iv() = share_seed.substr(kBytesPerAes256Key, kBytesPerAes256Iv);

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<UniformPseudorandomGenerator> prng,
                       CreatePrngFromSeed(seed));

  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> expected_share_vector_from_seed,
      prng->GenerateUniformRandomRange(kRegisterCount, kRingModulus));
  std::vector<uint32_t> expected_combined_frequency_vector_without_noise(
      kRegisterCount);
  for (int i = 0; i < expected_combined_frequency_vector_without_noise.size();
       i++) {
    expected_combined_frequency_vector_without_noise[i] =
        (expected_share_vector_from_seed[i] + share_vector[i]) % kRingModulus;
  }

  int expected_non_empty_register_count = 0;
  for (const auto reg : expected_combined_frequency_vector_without_noise) {
    if (reg != 0) {
      expected_non_empty_register_count++;
    }
  }

  int non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if ((reg % kRingModulus) != 0) {
      non_empty_register_count++;
    }
  }

  EXPECT_GE(non_empty_register_count, expected_non_empty_register_count);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithoutDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.ClearDifferentialPrivacyParams();
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);

  ShufflePhaseTestData test_data_2;
  test_data_2.ClearDifferentialPrivacyParams();
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
  test_data_1.AddShareToFrequencyVectorShares(secret_share_a);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_a);

  test_data_1.AddShareToFrequencyVectorShares(secret_share_b);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunReachOnlyShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunReachOnlyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector_share_1(
      ret_1.combined_frequency_vector().begin(),
      ret_1.combined_frequency_vector().end());
  std::vector<uint32_t> combined_frequency_vector_share_2(
      ret_2.combined_frequency_vector().begin(),
      ret_2.combined_frequency_vector().end());

  std::vector<uint32_t> combined_frequency_vector(
      combined_frequency_vector_share_1.size());
  for (int i = 0; i < combined_frequency_vector.size(); i++) {
    combined_frequency_vector[i] = ((combined_frequency_vector_share_1[i] +
                                     combined_frequency_vector_share_2[i]) %
                                    kRingModulus);
  }

  std::vector<uint32_t> combined_input(kRegisterCount);
  for (int i = 0; i < kRegisterCount; i++) {
    combined_input[i] = (input_a[i] + input_b[i] % kRingModulus);
  }

  int expected_non_empty_register_count = 0;
  for (const auto reg : combined_input) {
    if (reg != 0) {
      expected_non_empty_register_count++;
    }
  }

  int non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if ((reg % kRingModulus) != 0) {
      non_empty_register_count++;
    }
  }

  EXPECT_EQ(non_empty_register_count, expected_non_empty_register_count);
}

TEST(ReachOnlyShufflePhaseAtNonAggregator,
     ShufflePhaseSimulationForTwoDuchiesWithDpNoiseSucceeds) {
  ShufflePhaseTestData test_data_1;
  test_data_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
  test_data_1.SetReachOnlyDifferentialPrivacyParams(kEpsilon, kDelta);

  ShufflePhaseTestData test_data_2;
  test_data_2.SetNonAggregatorOrder(CompleteShufflePhaseRequest::SECOND);
  test_data_2.SetReachOnlyDifferentialPrivacyParams(kEpsilon, kDelta);

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
  test_data_1.AddShareToFrequencyVectorShares(secret_share_a);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_a);

  test_data_1.AddShareToFrequencyVectorShares(secret_share_b);
  test_data_2.AddSeedToFrequencyVectorShares(secret_share_b);

  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_1,
                       test_data_1.RunReachOnlyShufflePhase());
  ASSERT_OK_AND_ASSIGN(CompleteShufflePhaseResponse ret_2,
                       test_data_2.RunReachOnlyShufflePhase());

  std::vector<uint32_t> combined_frequency_vector_share_1(
      ret_1.combined_frequency_vector().begin(),
      ret_1.combined_frequency_vector().end());
  std::vector<uint32_t> combined_frequency_vector_share_2(
      ret_2.combined_frequency_vector().begin(),
      ret_2.combined_frequency_vector().end());

  std::vector<uint32_t> combined_frequency_vector(
      combined_frequency_vector_share_1.size());
  for (int i = 0; i < combined_frequency_vector.size(); i++) {
    combined_frequency_vector[i] = ((combined_frequency_vector_share_1[i] +
                                     combined_frequency_vector_share_2[i]) %
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
      noiser->options().shift_offset * 2;

  ASSERT_THAT(
      combined_frequency_vector,
      SizeIs(kRegisterCount + 2 * total_noise_registers_count_per_duchy));

  int non_empty_register_count = 0;
  for (const auto reg : combined_input) {
    if (reg != 0) {
      non_empty_register_count++;
    }
  }

  int noisy_non_empty_register_count = 0;
  for (const auto reg : combined_frequency_vector) {
    if (reg != 0) {
      noisy_non_empty_register_count++;
    }
  }
  int total_noise_added = 2 * total_noise_registers_count_per_duchy;

  // Verifies that all noises are within the bound.
  EXPECT_LE(non_empty_register_count, noisy_non_empty_register_count);
  EXPECT_LE(noisy_non_empty_register_count - non_empty_register_count,
            total_noise_added);
}

class AggregationPhaseTestData {
 public:
  AggregationPhaseTestData() {
    PrngSeed seed = GenerateRandomSeed();
    auto prng = CreatePrngFromSeed(seed);
    if (!prng.ok()) {
      LOG(FATAL) << "Cannot create Prng from seed.\n";
    }
    prng_ = std::move(prng.value());
    request_.set_vid_sampling_interval_width(kVidSamplingIntervalWidth);
    request_.set_noise_mechanism(NoiseMechanism::DISCRETE_GAUSSIAN);
    SetDifferentialPrivacyParams(kEpsilon, kDelta);
    SetFrequencyVectorParams(kRegisterCount, kMaxCombinedFrequency,
                             kRingModulus);
    SetMaximumFrequency(kMaxFrequencyPerEdp);
  }

  void SetMaximumFrequency(int maximum_frequency) {
    request_.set_maximum_frequency(maximum_frequency);
  }

  void SetFrequencyVectorParams(int register_count,
                                int maximum_combined_frequency,
                                int ring_modulus) {
    request_.mutable_frequency_vector_params()->set_register_count(
        register_count);
    request_.mutable_frequency_vector_params()->set_maximum_combined_frequency(
        maximum_combined_frequency);
    request_.mutable_frequency_vector_params()->set_ring_modulus(ring_modulus);
  }

  void ClearDifferentialPrivacyParams() {
    request_.clear_reach_dp_params();
    request_.clear_frequency_dp_params();
  }

  void SetReachOnlyDifferentialPrivacyParams(double eps, double delta) {
    request_.mutable_reach_dp_params()->set_epsilon(eps);
    request_.mutable_reach_dp_params()->set_delta(delta);
  }

  void SetDifferentialPrivacyParams(double eps, double delta) {
    request_.mutable_reach_dp_params()->set_epsilon(eps / 10.0);
    request_.mutable_reach_dp_params()->set_delta(delta);
    request_.mutable_frequency_dp_params()->set_epsilon(eps);
    request_.mutable_frequency_dp_params()->set_delta(delta);
  }

  void AddShareToFrequencyVectorShares(absl::Span<const uint32_t> data) {
    request_.add_frequency_vector_shares()->mutable_share_vector()->Add(
        data.begin(), data.end());
  }

  absl::StatusOr<MpcResult> RunReachAndFrequencyAggregationPhase() {
    MpcResult result;
    ASSIGN_OR_RETURN(auto response,
                     CompleteReachAndFrequencyAggregationPhase(request_));
    result.reach = response.reach();
    for (auto pair : response.frequency_distribution()) {
      result.frequency_distribution[pair.first] = pair.second;
    }
    return result;
  }

  absl::StatusOr<MpcResult> RunReachOnlyAggregationPhase() {
    MpcResult result;
    ASSIGN_OR_RETURN(auto response,
                     CompleteReachOnlyAggregationPhase(request_));
    result.reach = response.reach();
    return result;
  }

  absl::StatusOr<std::vector<uint32_t>> GenerateRandomShare(int size,
                                                            uint32_t modulus) {
    ASSIGN_OR_RETURN(std::vector<uint32_t> share_vector,
                     prng_->GenerateUniformRandomRange(size, modulus));
    return share_vector;
  }

 protected:
  std::unique_ptr<UniformPseudorandomGenerator> prng_;
  CompleteAggregationPhaseRequest request_;
};

TEST(ReachAndFrequencyAggregationPhase, InvalidRegisterCountFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/0,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/128);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "register count"));
}

TEST(ReachAndFrequencyAggregationPhase, InvalidRingModulusFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/1);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least 2"));
}

TEST(ReachAndFrequencyAggregationPhase,
     InvalidRingModulusAndMaxFrequencyPairFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/4,
                                     /*ring_modulus=*/5);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "plus one"));
}

TEST(ReachAndFrequencyAggregationPhase,
     InvalidNumberOfFrequencyVectorSharesFails) {
  AggregationPhaseTestData test_data;
  std::vector<uint32_t> share_vector(10, 0);
  test_data.AddShareToFrequencyVectorShares(share_vector);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "non-aggregators"));
}

TEST(ReachAndFrequencyAggregationPhase, InvalidMaximumFrequencyFails) {
  AggregationPhaseTestData test_data;
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_1,
      test_data.GenerateRandomShare(kRegisterCount, kRingModulus));
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_2,
      test_data.GenerateRandomShare(kRegisterCount, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);
  test_data.SetMaximumFrequency(0);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "maximum"));
}

TEST(ReachAndFrequencyAggregationPhase,
     FrequencyVectorShareVectorsHaveDifferentSizeFails) {
  AggregationPhaseTestData test_data;
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_1,
      test_data.GenerateRandomShare(kRegisterCount, kRingModulus));
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_2,
      test_data.GenerateRandomShare(kRegisterCount + 1, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);
  test_data.SetMaximumFrequency(0);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "same length"));
}

TEST(ReachAndFrequencyAggregationPhase,
     CombinedFrequencyVectorElementExceedsMaxCombinedFrequencyFails) {
  AggregationPhaseTestData test_data;
  int register_count = 1;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/8);
  test_data.SetMaximumFrequency(2);
  test_data.ClearDifferentialPrivacyParams();

  std::vector<uint32_t> share_vector_1 = {0};
  std::vector<uint32_t> share_vector_2 = {5};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);
  EXPECT_THAT(test_data.RunReachAndFrequencyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInternal, "5"));
}

TEST(ReachAndFrequencyAggregationPhase,
     AggregationPhaseWithoutDPNoiseSucceeds) {
  AggregationPhaseTestData test_data;
  int register_count = 8;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/8);
  test_data.SetMaximumFrequency(2);
  test_data.ClearDifferentialPrivacyParams();

  // The combined frequency_vector is {0, 0, 0, 0, 1, 2, 3, 4}. The frequency
  // histogram after removing offset is {f[0] = 4, f[1] = f[2] = f[3] = f[4] =
  // 1}.
  std::vector<uint32_t> share_vector_1 = {3, 4, 3, 6, 1, 7, 2, 0};
  std::vector<uint32_t> share_vector_2 = {5, 4, 5, 2, 0, 3, 1, 4};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.RunReachAndFrequencyAggregationPhase());
  int64_t expected_reach = 4.0 / kVidSamplingIntervalWidth;
  EXPECT_EQ(result.reach, expected_reach);
  ASSERT_THAT(result.frequency_distribution, SizeIs(2));
  EXPECT_EQ(result.frequency_distribution[1], 0.25);
  EXPECT_EQ(result.frequency_distribution[2], 0.75);
}

TEST(ReachAndFrequencyAggregationPhase, AggregationPhaseWithDPNoiseSucceeds) {
  AggregationPhaseTestData test_data;
  int register_count = 8;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/8);
  test_data.SetMaximumFrequency(2);
  // Frequency offset = 2 and reach offset = 6.
  test_data.SetDifferentialPrivacyParams(10.0, 1.0);
  // The combined frequency_vector is {0, 0, 0, 0, 1, 2, 3, 4, 0, 0, 1, 1, 2, 2,
  // 3, 3, 4, 4}. The frequency histogram after removing offset is {f[0] = 0,
  // f[1] = f[2] = f[3] = f[4] = 1}.
  std::vector<uint32_t> share_vector_1 = {3, 4, 3, 6, 1, 7, 4, 2, 0,
                                          0, 0, 0, 0, 0, 0, 0, 0, 0};
  std::vector<uint32_t> share_vector_2 = {5, 4, 5, 2, 0, 3, 7, 2, 0,
                                          0, 1, 1, 2, 2, 3, 3, 4, 4};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.RunReachAndFrequencyAggregationPhase());
  int64_t expected_reach = 8.0 / kVidSamplingIntervalWidth;
  EXPECT_EQ(result.reach, expected_reach);
  ASSERT_THAT(result.frequency_distribution, SizeIs(2));
  EXPECT_EQ(result.frequency_distribution[1], 0.125);
  EXPECT_EQ(result.frequency_distribution[2], 0.875);
}

TEST(ReachAndFrequencyAggregationPhase,
     AggregationPhaseNoDataNorEffectiveNoiseFails) {
  AggregationPhaseTestData test_data;
  int register_count = 4;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/8);
  test_data.SetMaximumFrequency(2);
  // Frequency offset = 2 and reach offset = 6.
  test_data.SetDifferentialPrivacyParams(10.0, 1.0);
  // The combined frequency_vector is {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2}.
  // The frequency histogram after removing offset is {f[0] = 4, f[2] = 1}.
  std::vector<uint32_t> share_vector_1 = {1, 2, 3, 4, 0, 3, 4,
                                          3, 6, 1, 7, 4, 3};
  std::vector<uint32_t> share_vector_2 = {7, 6, 5, 4, 0, 5, 4,
                                          5, 2, 7, 3, 6, 7};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);

  EXPECT_THAT(
      test_data.RunReachAndFrequencyAggregationPhase().status(),
      StatusIs(absl::StatusCode::kInvalidArgument, "data nor effective noise"));
}

TEST(ReachOnlyAggregationPhase, InvalidRegisterCountFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/0,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/128);
  EXPECT_THAT(test_data.RunReachOnlyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "register count"));
}

TEST(ReachOnlyAggregationPhase, InvalidRingModulusFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/10,
                                     /*ring_modulus=*/1);
  EXPECT_THAT(test_data.RunReachOnlyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "at least 2"));
}

TEST(ReachOnlyAggregationPhase, InvalidRingModulusAndMaxFrequencyPairFails) {
  AggregationPhaseTestData test_data;
  test_data.SetFrequencyVectorParams(/*register_count=*/100,
                                     /*maximum_combined_frequency=*/4,
                                     /*ring_modulus=*/5);
  EXPECT_THAT(test_data.RunReachOnlyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "plus one"));
}

TEST(ReachOnlyAggregationPhase, InvalidNumberOfFrequencyVectorSharesFails) {
  AggregationPhaseTestData test_data;
  std::vector<uint32_t> share_vector(10, 0);
  test_data.AddShareToFrequencyVectorShares(share_vector);
  EXPECT_THAT(test_data.RunReachOnlyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "non-aggregators"));
}

TEST(ReachOnlyAggregationPhase,
     FrequencyVectorShareVectorsHaveDifferentSizeFails) {
  AggregationPhaseTestData test_data;
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_1,
      test_data.GenerateRandomShare(kRegisterCount, kRingModulus));
  ASSERT_OK_AND_ASSIGN(
      std::vector<uint32_t> share_vector_2,
      test_data.GenerateRandomShare(kRegisterCount + 1, kRingModulus));
  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);
  EXPECT_THAT(test_data.RunReachOnlyAggregationPhase().status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "same length"));
}

TEST(ReachOnlyAggregationPhase, AggregationPhaseWithoutDPNoiseSucceeds) {
  AggregationPhaseTestData test_data;
  int register_count = 8;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/7);
  test_data.SetMaximumFrequency(2);
  test_data.ClearDifferentialPrivacyParams();

  // The combined frequency_vector is {0, 0, 0, 0, 1, 2, 3, 4}.
  std::vector<uint32_t> share_vector_1 = {2, 3, 2, 5, 1, 6, 2, 0};
  std::vector<uint32_t> share_vector_2 = {5, 4, 5, 2, 0, 3, 1, 4};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.RunReachOnlyAggregationPhase());
  int64_t expected_reach = 4.0 / kVidSamplingIntervalWidth;
  EXPECT_EQ(result.reach, expected_reach);
}

TEST(ReachOnlyAggregationPhase, AggregationPhaseWithDPNoiseSucceeds) {
  AggregationPhaseTestData test_data;
  int register_count = 8;
  test_data.SetFrequencyVectorParams(register_count,
                                     /*max_combined_frequency=*/4,
                                     /*ring_modulus=*/7);
  test_data.SetMaximumFrequency(2);
  // Computed offset = 2.
  test_data.SetReachOnlyDifferentialPrivacyParams(10.0, 1.0);
  // The combined frequency_vector is {0, 0, 0, 0, 1, 2, 3, 4, 0, 0, 1, 1, 2, 2,
  // 3, 3, 4, 4}.
  std::vector<uint32_t> share_vector_1 = {2, 3, 2, 5, 1, 6, 2, 0, 0,
                                          0, 0, 0, 0, 0, 0, 0, 0, 0};
  std::vector<uint32_t> share_vector_2 = {5, 4, 5, 2, 0, 3, 1, 4, 0,
                                          0, 1, 1, 2, 2, 3, 3, 4, 4};

  test_data.AddShareToFrequencyVectorShares(share_vector_1);
  test_data.AddShareToFrequencyVectorShares(share_vector_2);

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.RunReachOnlyAggregationPhase());
  int64_t expected_reach = 8.0 / kVidSamplingIntervalWidth;
  EXPECT_EQ(result.reach, expected_reach);
}

class EndToEndHmssTest {
 public:
  EndToEndHmssTest() {}
  absl::Status GoThroughEntireMpcProtocol(int edp_count, int register_count,
                                          int maximum_combined_frequency,
                                          int ring_modulus,
                                          int maximum_frequency, double epsilon,
                                          double delta, bool has_dp_noise) {
    ShufflePhaseTestData worker_1;
    ShufflePhaseTestData worker_2;
    AggregationPhaseTestData aggregator;

    worker_1.SetFrequencyVectorParams(register_count,
                                      maximum_combined_frequency, ring_modulus);
    worker_2.SetFrequencyVectorParams(register_count,
                                      maximum_combined_frequency, ring_modulus);
    aggregator.SetFrequencyVectorParams(
        register_count, maximum_combined_frequency, ring_modulus);
    worker_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
    worker_2.SetNonAggregatorOrder(CompleteShufflePhaseRequest::SECOND);

    SecretShareParameter secret_share_params;
    secret_share_params.set_modulus(ring_modulus);

    int64_t reach_shift_offset = 0;
    int64_t frequency_shift_offset = 0;
    int64_t total_noise_registers_count_per_duchy = 0;

    if (has_dp_noise) {
      DifferentialPrivacyParams reach_dp_params;
      reach_dp_params.set_epsilon(epsilon / 10.0);
      reach_dp_params.set_delta(delta);

      DifferentialPrivacyParams frequency_dp_params;
      frequency_dp_params.set_epsilon(epsilon);
      frequency_dp_params.set_delta(delta);

      auto reach_noiser = GetBlindHistogramNoiser(
          reach_dp_params,
          /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);
      auto frequency_noiser = GetBlindHistogramNoiser(
          frequency_dp_params,
          /*contributors_count=*/2, NoiseMechanism::DISCRETE_GAUSSIAN);

      reach_shift_offset = reach_noiser->options().shift_offset;
      frequency_shift_offset = frequency_noiser->options().shift_offset;

      total_noise_registers_count_per_duchy =
          reach_noiser->options().shift_offset * 2 +
          frequency_noiser->options().shift_offset * 2 *
              maximum_combined_frequency;
    } else {
      worker_1.ClearDifferentialPrivacyParams();
      worker_2.ClearDifferentialPrivacyParams();
      aggregator.ClearDifferentialPrivacyParams();
    }

    PrngSeed seed = GenerateRandomSeed();
    ASSIGN_OR_RETURN(auto prng, CreatePrngFromSeed(seed));

    std::vector<std::vector<uint32_t>> data_from_edps(edp_count);
    std::vector<uint32_t> combined_frequency_vector(register_count, 0);
    std::vector<SecretShare> secret_shares(edp_count);

    // Emulates the EDPs.
    // Generates shares from input and distributes them to workers.
    for (int i = 0; i < edp_count; i++) {
      // Generates random frequency_vector for the Edp.
      ASSIGN_OR_RETURN(data_from_edps[i],
                       prng->GenerateUniformRandomRange(register_count,
                                                        1 + maximum_frequency));
      // Adds the frequency_vector to the combined frequency_vector.
      ASSIGN_OR_RETURN(combined_frequency_vector,
                       VectorAddMod(combined_frequency_vector,
                                    data_from_edps[i], ring_modulus));
      // Generates secret share from the frequency_vector.
      ASSIGN_OR_RETURN(
          secret_shares[i],
          GenerateSecretShares(secret_share_params, data_from_edps[i]));
      // Assigns the frequency_vector shares to the workers.
      if (i % 2 == 0) {
        worker_1.AddShareToFrequencyVectorShares(secret_shares[i]);
        worker_2.AddSeedToFrequencyVectorShares(secret_shares[i]);
      } else {
        worker_1.AddSeedToFrequencyVectorShares(secret_shares[i]);
        worker_2.AddShareToFrequencyVectorShares(secret_shares[i]);
      }
    }

    // Worker 1 runs the shuffle phase.
    ASSIGN_OR_RETURN(
        CompleteShufflePhaseResponse complete_shuffle_phase_response_1,
        worker_1.RunReachAndFrequencyShufflePhase());

    // Worker 2 runs the shuffle phase.
    ASSIGN_OR_RETURN(
        CompleteShufflePhaseResponse complete_shuffle_phase_response_2,
        worker_2.RunReachAndFrequencyShufflePhase());

    std::vector<uint32_t> share_from_worker_1(
        complete_shuffle_phase_response_1.combined_frequency_vector().begin(),
        complete_shuffle_phase_response_1.combined_frequency_vector().end());
    std::vector<uint32_t> share_from_worker_2(
        complete_shuffle_phase_response_2.combined_frequency_vector().begin(),
        complete_shuffle_phase_response_2.combined_frequency_vector().end());

    EXPECT_THAT(
        share_from_worker_1,
        SizeIs(register_count + 2 * total_noise_registers_count_per_duchy));
    EXPECT_THAT(
        share_from_worker_2,
        SizeIs(register_count + 2 * total_noise_registers_count_per_duchy));

    // Emulates the aggregator.
    aggregator.AddShareToFrequencyVectorShares(share_from_worker_1);
    aggregator.AddShareToFrequencyVectorShares(share_from_worker_2);

    std::unordered_map<int, int64_t> frequency_histogram;
    for (const auto reg : combined_frequency_vector) {
      frequency_histogram[reg]++;
    }

    int64_t expected_reach =
        (register_count - frequency_histogram[0]) / kVidSamplingIntervalWidth;

    // When there is no differential noise, the shift offset is 0.
    int64_t max_reach_error =
        2 * reach_shift_offset / kVidSamplingIntervalWidth;

    ASSIGN_OR_RETURN(MpcResult result,
                     aggregator.RunReachAndFrequencyAggregationPhase());

    EXPECT_LE(std::abs(expected_reach - result.reach), max_reach_error);

    int min_reach = std::max(
        1L, register_count - frequency_histogram[0] - 2 * reach_shift_offset);
    int max_reach =
        register_count - frequency_histogram[0] + 2 * reach_shift_offset;

    for (int i = 1; i < kMaxFrequencyPerEdp; i++) {
      if (frequency_histogram.count(i)) {
        double upper_bound =
            0.00001 + (frequency_histogram[i] + 2 * frequency_shift_offset) /
                          static_cast<double>(min_reach);
        double lower_bound =
            -0.00001 + (frequency_histogram[i] - 2 * frequency_shift_offset) /
                           static_cast<double>(max_reach);
        EXPECT_LE(result.frequency_distribution[i], upper_bound);
        EXPECT_GE(result.frequency_distribution[i], lower_bound);
      }
    }

    return absl::OkStatus();
  }
};

TEST(EndToEndHmss, HmssWithoutDPNoiseSucceeds) {
  EndToEndHmssTest mpc;
  auto mpc_result = mpc.GoThroughEntireMpcProtocol(
      /*edp_count=*/kEdpCount, /*register_count=*/kRegisterCount,
      /*maximum_combined_frequency=*/kMaxCombinedFrequency,
      /*ring_modulus=*/kRingModulus,
      /*maximum_frequency=*/kMaxFrequencyPerEdp, /*epsilon=*/kEpsilon,
      /*delta=*/kDelta, /*has_dp_noise=*/false);
  EXPECT_THAT(mpc_result, IsOk());
}

TEST(EndToEndHmss, HmssWithDPNoiseSucceeds) {
  EndToEndHmssTest mpc;
  auto mpc_result = mpc.GoThroughEntireMpcProtocol(
      /*edp_count=*/kEdpCount, /*register_count=*/kRegisterCount,
      /*maximum_combined_frequency=*/kMaxCombinedFrequency,
      /*ring_modulus=*/kRingModulus,
      /*maximum_frequency=*/kMaxFrequencyPerEdp, /*epsilon=*/kEpsilon,
      /*delta=*/kDelta, /*has_dp_noise=*/true);
  EXPECT_THAT(mpc_result, IsOk());
}

class EndToEndReachOnlyHmssTest {
 public:
  EndToEndReachOnlyHmssTest() {}
  absl::Status GoThroughEntireMpcProtocol(int edp_count, int register_count,
                                          int ring_modulus, double epsilon,
                                          double delta, bool has_dp_noise) {
    ShufflePhaseTestData worker_1;
    ShufflePhaseTestData worker_2;
    AggregationPhaseTestData aggregator;

    worker_1.SetFrequencyVectorParams(register_count, edp_count, ring_modulus);
    worker_1.SetReachOnlyDifferentialPrivacyParams(epsilon, delta);
    worker_2.SetFrequencyVectorParams(register_count, edp_count, ring_modulus);
    worker_2.SetReachOnlyDifferentialPrivacyParams(epsilon, delta);
    aggregator.SetFrequencyVectorParams(register_count, edp_count,
                                        ring_modulus);
    aggregator.SetReachOnlyDifferentialPrivacyParams(epsilon, delta);
    worker_1.SetNonAggregatorOrder(CompleteShufflePhaseRequest::FIRST);
    worker_2.SetNonAggregatorOrder(CompleteShufflePhaseRequest::SECOND);

    SecretShareParameter secret_share_params;
    secret_share_params.set_modulus(ring_modulus);

    int64_t shift_offset = 0;
    int64_t total_noise_registers_count_per_duchy = 0;

    if (has_dp_noise) {
      DifferentialPrivacyParams reach_dp_params;
      reach_dp_params.set_epsilon(epsilon);
      reach_dp_params.set_delta(delta);
      auto noiser = GetBlindHistogramNoiser(reach_dp_params,
                                            /*contributors_count=*/2,
                                            NoiseMechanism::DISCRETE_GAUSSIAN);
      shift_offset = noiser->options().shift_offset;
      total_noise_registers_count_per_duchy =
          noiser->options().shift_offset * 2;
    } else {
      worker_1.ClearDifferentialPrivacyParams();
      worker_2.ClearDifferentialPrivacyParams();
      aggregator.ClearDifferentialPrivacyParams();
    }

    PrngSeed seed = GenerateRandomSeed();
    ASSIGN_OR_RETURN(auto prng, CreatePrngFromSeed(seed));

    std::vector<std::vector<uint32_t>> data_from_edps(edp_count);
    std::vector<uint32_t> combined_frequency_vector(register_count, 0);
    std::vector<SecretShare> secret_shares(edp_count);

    // Emulates the EDPs.
    // Generates shares from input and distributes them to workers.
    for (int i = 0; i < edp_count; i++) {
      // Generates random frequency_vector for the Edp.
      ASSIGN_OR_RETURN(data_from_edps[i],
                       prng->GenerateUniformRandomRange(register_count, 2));
      // Adds the frequency_vector to the combined frequency_vector.
      ASSIGN_OR_RETURN(combined_frequency_vector,
                       VectorAddMod(combined_frequency_vector,
                                    data_from_edps[i], ring_modulus));
      // Generates secret share from the frequency_vector.
      ASSIGN_OR_RETURN(
          secret_shares[i],
          GenerateSecretShares(secret_share_params, data_from_edps[i]));
      // Assigns the frequency_vector shares to the workers.
      if (i % 2 == 0) {
        worker_1.AddShareToFrequencyVectorShares(secret_shares[i]);
        worker_2.AddSeedToFrequencyVectorShares(secret_shares[i]);
      } else {
        worker_1.AddSeedToFrequencyVectorShares(secret_shares[i]);
        worker_2.AddShareToFrequencyVectorShares(secret_shares[i]);
      }
    }

    // Worker 1 runs the shuffle phase.
    ASSIGN_OR_RETURN(
        CompleteShufflePhaseResponse complete_shuffle_phase_response_1,
        worker_1.RunReachOnlyShufflePhase());

    // Worker 2 runs the shuffle phase.
    ASSIGN_OR_RETURN(
        CompleteShufflePhaseResponse complete_shuffle_phase_response_2,
        worker_2.RunReachOnlyShufflePhase());

    std::vector<uint32_t> share_from_worker_1(
        complete_shuffle_phase_response_1.combined_frequency_vector().begin(),
        complete_shuffle_phase_response_1.combined_frequency_vector().end());
    std::vector<uint32_t> share_from_worker_2(
        complete_shuffle_phase_response_2.combined_frequency_vector().begin(),
        complete_shuffle_phase_response_2.combined_frequency_vector().end());

    EXPECT_THAT(
        share_from_worker_1,
        SizeIs(register_count + 2 * total_noise_registers_count_per_duchy));
    EXPECT_THAT(
        share_from_worker_2,
        SizeIs(register_count + 2 * total_noise_registers_count_per_duchy));

    // Emulates the aggregator.
    aggregator.AddShareToFrequencyVectorShares(share_from_worker_1);
    aggregator.AddShareToFrequencyVectorShares(share_from_worker_2);

    int expected_non_empty_register_count = 0;
    for (const auto reg : combined_frequency_vector) {
      if (reg != 0) {
        expected_non_empty_register_count++;
      }
    }

    int64_t expected_reach =
        expected_non_empty_register_count / kVidSamplingIntervalWidth;

    // When there is no differential noise, the shift offset is 0.
    int64_t max_reach_error = 2 * shift_offset / kVidSamplingIntervalWidth;

    ASSIGN_OR_RETURN(MpcResult result,
                     aggregator.RunReachOnlyAggregationPhase());

    EXPECT_LE(std::abs(expected_reach - result.reach), max_reach_error);

    return absl::OkStatus();
  }
};

TEST(EndToEndReachOnlyHmss, HmssWithoutDPNoiseSucceeds) {
  EndToEndReachOnlyHmssTest mpc;
  auto mpc_result = mpc.GoThroughEntireMpcProtocol(
      /*edp_count=*/kEdpCount, /*register_count=*/kRegisterCount,
      /*ring_modulus=*/kRingModulus, /*epsilon=*/kEpsilon,
      /*delta=*/kDelta, /*has_dp_noise=*/false);
  EXPECT_THAT(mpc_result, IsOk());
}

TEST(EndToEndReachOnlyHmss, HmssWithDPNoiseSucceeds) {
  EndToEndReachOnlyHmssTest mpc;
  auto mpc_result = mpc.GoThroughEntireMpcProtocol(
      /*edp_count=*/kEdpCount, /*register_count=*/kRegisterCount,
      /*ring_modulus=*/kRingModulus, /*epsilon=*/kEpsilon,
      /*delta=*/kDelta, /*has_dp_noise=*/true);
  EXPECT_THAT(mpc_result, IsOk());
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::share_shuffle
