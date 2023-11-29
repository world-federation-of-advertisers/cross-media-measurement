// Copyright 2023 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/reach_only_liquid_legions_v2_encryption_utility.h"

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "any_sketch/crypto/sketch_encrypter.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "estimation/estimators.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "openssl/obj_mac.h"
#include "private_join_and_compute/crypto/commutative_elgamal.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "wfa/any_sketch/sketch.pb.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/internal/duchy/noise_mechanism.pb.h"
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/testing/liquid_legions_v2_encryption_utility_helper.h"

namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2 {
namespace {

using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::testing::DoubleNear;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;
using ::wfa::any_sketch::Sketch;
using ::wfa::any_sketch::SketchConfig;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::ExtractElGamalCiphertextFromString;
using ::wfa::measurement::common::crypto::GetCountValuesPlaintext;
using ::wfa::measurement::common::crypto::kBlindedHistogramNoiseRegisterKey;
using ::wfa::measurement::common::crypto::kBytesPerCipherText;
using ::wfa::measurement::common::crypto::kDestroyedRegisterKey;
using ::wfa::measurement::common::crypto::kPaddingNoiseRegisterId;
using ::wfa::measurement::common::crypto::kPublisherNoiseRegisterId;
using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::ElGamalKeyPair;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;
using ::wfa::measurement::internal::duchy::NoiseMechanism;

constexpr int kWorkerCount = 3;
constexpr int kPublisherCount = 3;
constexpr int kMaxFrequency = 10;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
constexpr int kParallelism = 3;
constexpr int kBytesPerEcPoint = 33;
constexpr int kBytesCipherText = kBytesPerEcPoint * 2;
constexpr int kDecayRate = 12;
constexpr int kLiquidLegionsSize = 100 * 1000;
constexpr float kVidSamplingIntervalWidth = 0.5;

struct ReachOnlyMpcResult {
  int64_t reach;
};

void AddRegister(Sketch* sketch, const int index) {
  auto register_ptr = sketch->add_registers();
  register_ptr->set_index(index);
}

MATCHER_P(IsBlockSorted, block_size, "") {
  if (arg.length() % block_size != 0) {
    return false;
  }
  for (size_t i = block_size; i < arg.length(); i += block_size) {
    if (arg.substr(i, block_size) < arg.substr(i - block_size, block_size)) {
      return false;
    }
  }
  return true;
}

// The ReachOnlyTest generates cipher keys for 3 duchies, and the combined
// public key for the data providers. The duchy 1 and 2 are non-aggregator
// workers, while the duchy 3 is the aggregator.
class ReachOnlyTest {
 public:
  ElGamalKeyPair duchy_1_el_gamal_key_pair_;
  std::string duchy_1_p_h_key_;
  ElGamalKeyPair duchy_2_el_gamal_key_pair_;
  std::string duchy_2_p_h_key_;
  ElGamalKeyPair duchy_3_el_gamal_key_pair_;
  std::string duchy_3_p_h_key_;
  ElGamalPublicKey client_el_gamal_public_key_;  // combined from 3 duchy keys;
  ElGamalPublicKey duchy_2_3_composite_public_key_;  // combined from duchy 2
                                                     // and duchy_3 public keys;
  std::unique_ptr<any_sketch::crypto::SketchEncrypter> sketch_encrypter_;

  ReachOnlyTest() {
    CompleteReachOnlyInitializationPhaseRequest
        complete_reach_only_initialization_phase_request;
    complete_reach_only_initialization_phase_request.set_curve_id(kTestCurveId);

    duchy_1_el_gamal_key_pair_ =
        CompleteReachOnlyInitializationPhase(
            complete_reach_only_initialization_phase_request)
            ->el_gamal_key_pair();
    duchy_2_el_gamal_key_pair_ =
        CompleteReachOnlyInitializationPhase(
            complete_reach_only_initialization_phase_request)
            ->el_gamal_key_pair();
    duchy_3_el_gamal_key_pair_ =
        CompleteReachOnlyInitializationPhase(
            complete_reach_only_initialization_phase_request)
            ->el_gamal_key_pair();

    // Combine the el_gamal keys from all duchies to generate the data provider
    // el_gamal key.
    client_el_gamal_public_key_ = ToDuchyInternalElGamalKey(
        any_sketch::crypto::CombineElGamalPublicKeys(
            kTestCurveId,
            {ToAnySketchElGamalKey(duchy_1_el_gamal_key_pair_.public_key()),
             ToAnySketchElGamalKey(duchy_2_el_gamal_key_pair_.public_key()),
             ToAnySketchElGamalKey(duchy_3_el_gamal_key_pair_.public_key())})
            .value());
    duchy_2_3_composite_public_key_ = ToDuchyInternalElGamalKey(
        any_sketch::crypto::CombineElGamalPublicKeys(
            kTestCurveId,
            {ToAnySketchElGamalKey(duchy_2_el_gamal_key_pair_.public_key()),
             ToAnySketchElGamalKey(duchy_3_el_gamal_key_pair_.public_key())})
            .value());

    any_sketch::crypto::CiphertextString client_public_key = {
        .u = client_el_gamal_public_key_.generator(),
        .e = client_el_gamal_public_key_.element(),
    };

    // Create a sketch_encrypter for encrypting plaintext any_sketch data.
    sketch_encrypter_ = any_sketch::crypto::CreateWithPublicKey(
                            kTestCurveId, kMaxFrequency, client_public_key)
                            .value();
  }

  absl::StatusOr<std::string> EncryptWithFlaggedKey(const Sketch& sketch) {
    return sketch_encrypter_->Encrypt(
        sketch, any_sketch::crypto::EncryptSketchRequest::FLAGGED_KEY);
  }

  // Helper function to go through the entire MPC protocol using the input data.
  // The final ReachOnlyMpcResult are returned.
  absl::StatusOr<ReachOnlyMpcResult> GoThroughEntireMpcProtocol(
      const std::string& encrypted_sketch,
      RegisterNoiseGenerationParameters* reach_noise_parameters,
      NoiseMechanism noise_mechanism) {
    // Setup phase at Duchy 1.
    // We assume all test data comes from duchy 1 in the test.
    CompleteReachOnlySetupPhaseRequest
        complete_reach_only_setup_phase_request_1;
    complete_reach_only_setup_phase_request_1.set_combined_register_vector(
        encrypted_sketch);

    if (reach_noise_parameters != nullptr) {
      *complete_reach_only_setup_phase_request_1.mutable_noise_parameters() =
          *reach_noise_parameters;
    }
    complete_reach_only_setup_phase_request_1.set_noise_mechanism(
        noise_mechanism);
    complete_reach_only_setup_phase_request_1.set_curve_id(kTestCurveId);
    *complete_reach_only_setup_phase_request_1
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_reach_only_setup_phase_request_1.set_parallelism(kParallelism);

    ASSIGN_OR_RETURN(
        CompleteReachOnlySetupPhaseResponse
            complete_reach_only_setup_phase_response_1,
        CompleteReachOnlySetupPhase(complete_reach_only_setup_phase_request_1));
    EXPECT_THAT(
        complete_reach_only_setup_phase_response_1.combined_register_vector(),
        IsBlockSorted(kBytesCipherText));
    EXPECT_THAT(complete_reach_only_setup_phase_response_1
                    .serialized_excessive_noise_ciphertext(),
                SizeIs(kBytesCipherText));

    // Setup phase at Duchy 2.
    // We assume all test data comes from duchy 1 in the test, so there is only
    // noise from duchy 2 (if configured)
    CompleteReachOnlySetupPhaseRequest
        complete_reach_only_setup_phase_request_2;
    if (reach_noise_parameters != nullptr) {
      *complete_reach_only_setup_phase_request_2.mutable_noise_parameters() =
          *reach_noise_parameters;
    }
    complete_reach_only_setup_phase_request_2.set_noise_mechanism(
        noise_mechanism);
    complete_reach_only_setup_phase_request_2.set_curve_id(kTestCurveId);
    *complete_reach_only_setup_phase_request_2
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_reach_only_setup_phase_request_2.set_parallelism(kParallelism);

    ASSIGN_OR_RETURN(
        CompleteReachOnlySetupPhaseResponse
            complete_reach_only_setup_phase_response_2,
        CompleteReachOnlySetupPhase(complete_reach_only_setup_phase_request_2));
    EXPECT_THAT(
        complete_reach_only_setup_phase_response_2.combined_register_vector(),
        IsBlockSorted(kBytesCipherText));
    EXPECT_THAT(complete_reach_only_setup_phase_response_2
                    .serialized_excessive_noise_ciphertext(),
                SizeIs(kBytesCipherText));

    // Setup phase at Duchy 3.
    // We assume all test data comes from duchy 1 in the test, so there is only
    // noise from duchy 3 (if configured)
    CompleteReachOnlySetupPhaseRequest
        complete_reach_only_setup_phase_request_3;
    if (reach_noise_parameters != nullptr) {
      *complete_reach_only_setup_phase_request_3.mutable_noise_parameters() =
          *reach_noise_parameters;
    }
    complete_reach_only_setup_phase_request_3.set_curve_id(kTestCurveId);
    complete_reach_only_setup_phase_request_3.set_noise_mechanism(
        noise_mechanism);
    *complete_reach_only_setup_phase_request_3
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_reach_only_setup_phase_request_3.set_parallelism(kParallelism);

    std::string serialized_excessive_noise_ciphertext =
        absl::StrCat(complete_reach_only_setup_phase_response_1
                         .serialized_excessive_noise_ciphertext(),
                     complete_reach_only_setup_phase_response_2
                         .serialized_excessive_noise_ciphertext());
    complete_reach_only_setup_phase_request_3
        .set_serialized_excessive_noise_ciphertext(
            serialized_excessive_noise_ciphertext);

    // Combine all CRVs from the workers.
    std::string combine_data = absl::StrCat(
        complete_reach_only_setup_phase_response_1.combined_register_vector(),
        complete_reach_only_setup_phase_response_2.combined_register_vector());
    complete_reach_only_setup_phase_request_3.set_combined_register_vector(
        combine_data);

    ASSIGN_OR_RETURN(CompleteReachOnlySetupPhaseResponse
                         complete_reach_only_setup_phase_response_3,
                     CompleteReachOnlySetupPhaseAtAggregator(
                         complete_reach_only_setup_phase_request_3));
    EXPECT_THAT(
        complete_reach_only_setup_phase_response_3.combined_register_vector(),
        IsBlockSorted(kBytesCipherText));
    EXPECT_THAT(complete_reach_only_setup_phase_response_3
                    .serialized_excessive_noise_ciphertext(),
                SizeIs(kBytesCipherText));

    // Execution phase at duchy 1 (non-aggregator).
    CompleteReachOnlyExecutionPhaseRequest
        complete_reach_only_execution_phase_request_1;
    *complete_reach_only_execution_phase_request_1
         .mutable_local_el_gamal_key_pair() = duchy_1_el_gamal_key_pair_;
    complete_reach_only_execution_phase_request_1.set_curve_id(kTestCurveId);
    *complete_reach_only_execution_phase_request_1
         .mutable_serialized_excessive_noise_ciphertext() =
        complete_reach_only_setup_phase_response_3
            .serialized_excessive_noise_ciphertext();
    complete_reach_only_execution_phase_request_1.set_parallelism(kParallelism);
    complete_reach_only_execution_phase_request_1.set_combined_register_vector(
        complete_reach_only_setup_phase_response_3.combined_register_vector());
    complete_reach_only_execution_phase_request_1
        .set_serialized_excessive_noise_ciphertext(
            complete_reach_only_setup_phase_response_3
                .serialized_excessive_noise_ciphertext());
    ASSIGN_OR_RETURN(CompleteReachOnlyExecutionPhaseResponse
                         complete_reach_only_execution_phase_response_1,
                     CompleteReachOnlyExecutionPhase(
                         complete_reach_only_execution_phase_request_1));
    EXPECT_THAT(complete_reach_only_execution_phase_response_1
                    .combined_register_vector(),
                IsBlockSorted(kBytesCipherText));

    // Execution phase at duchy 2 (non-aggregator).
    CompleteReachOnlyExecutionPhaseRequest
        complete_reach_only_execution_phase_request_2;
    *complete_reach_only_execution_phase_request_2
         .mutable_local_el_gamal_key_pair() = duchy_2_el_gamal_key_pair_;
    complete_reach_only_execution_phase_request_2.set_curve_id(kTestCurveId);
    *complete_reach_only_execution_phase_request_2
         .mutable_serialized_excessive_noise_ciphertext() =
        complete_reach_only_execution_phase_response_1
            .serialized_excessive_noise_ciphertext();
    complete_reach_only_execution_phase_request_2.set_parallelism(kParallelism);
    complete_reach_only_execution_phase_request_2.set_combined_register_vector(
        complete_reach_only_execution_phase_response_1
            .combined_register_vector());
    complete_reach_only_execution_phase_request_2
        .set_serialized_excessive_noise_ciphertext(
            complete_reach_only_execution_phase_response_1
                .serialized_excessive_noise_ciphertext());
    ASSIGN_OR_RETURN(CompleteReachOnlyExecutionPhaseResponse
                         complete_execution_phase_one_response_2,
                     CompleteReachOnlyExecutionPhase(
                         complete_reach_only_execution_phase_request_2));
    EXPECT_THAT(
        complete_execution_phase_one_response_2.combined_register_vector(),
        IsBlockSorted(kBytesCipherText));

    // Execution phase at duchy 3 (aggregator).
    CompleteReachOnlyExecutionPhaseAtAggregatorRequest
        complete_reach_only_execution_phase_at_aggregator_request;
    complete_reach_only_execution_phase_at_aggregator_request
        .set_combined_register_vector(
            complete_execution_phase_one_response_2.combined_register_vector());
    *complete_reach_only_execution_phase_at_aggregator_request
         .mutable_local_el_gamal_key_pair() = duchy_3_el_gamal_key_pair_;
    complete_reach_only_execution_phase_at_aggregator_request.set_curve_id(
        kTestCurveId);
    complete_reach_only_execution_phase_at_aggregator_request.set_parallelism(
        kParallelism);
    *complete_reach_only_execution_phase_at_aggregator_request
         .mutable_serialized_excessive_noise_ciphertext() =
        complete_execution_phase_one_response_2
            .serialized_excessive_noise_ciphertext();
    if (reach_noise_parameters != nullptr) {
      complete_reach_only_execution_phase_at_aggregator_request
          .mutable_reach_dp_noise_baseline()
          ->set_contributors_count(3);
      *complete_reach_only_execution_phase_at_aggregator_request
           .mutable_reach_dp_noise_baseline()
           ->mutable_global_reach_dp_noise() =
          reach_noise_parameters->dp_params().global_reach_dp_noise();
      *complete_reach_only_execution_phase_at_aggregator_request
           .mutable_noise_parameters() = *reach_noise_parameters;
    }
    complete_reach_only_execution_phase_at_aggregator_request
        .mutable_sketch_parameters()
        ->set_decay_rate(kDecayRate);
    complete_reach_only_execution_phase_at_aggregator_request
        .mutable_sketch_parameters()
        ->set_size(kLiquidLegionsSize);
    complete_reach_only_execution_phase_at_aggregator_request
        .set_vid_sampling_interval_width(kVidSamplingIntervalWidth);
    complete_reach_only_execution_phase_at_aggregator_request
        .set_noise_mechanism(noise_mechanism);
    complete_reach_only_execution_phase_at_aggregator_request
        .set_serialized_excessive_noise_ciphertext(
            complete_execution_phase_one_response_2
                .serialized_excessive_noise_ciphertext());
    ASSIGN_OR_RETURN(
        CompleteReachOnlyExecutionPhaseAtAggregatorResponse
            complete_reach_only_execution_phase_at_aggregator_response,
        CompleteReachOnlyExecutionPhaseAtAggregator(
            complete_reach_only_execution_phase_at_aggregator_request));

    ReachOnlyMpcResult result;
    result.reach =
        complete_reach_only_execution_phase_at_aggregator_response.reach();
    return result;
  }
};

TEST(CompleteReachOnlySetupPhase, WrongInputSketchSizeShouldThrow) {
  ReachOnlyTest test_data;
  CompleteReachOnlySetupPhaseRequest request;
  request.set_curve_id(kTestCurveId);
  *request.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;
  request.set_combined_register_vector("1234");
  request.set_parallelism(kParallelism);

  auto result = CompleteReachOnlySetupPhase(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteReachOnlySetupPhase, SetupPhaseWorksAsExpectedWithoutNoise) {
  ReachOnlyTest test_data;
  CompleteReachOnlySetupPhaseRequest request;
  request.set_curve_id(kTestCurveId);
  *request.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;
  request.set_parallelism(kParallelism);

  std::string register1 = "abc";
  std::string register2 = "def";
  for (int i = 3; i < kBytesCipherText; i++) {
    register1 = register1 + " ";
    register2 = register2 + " ";
  }
  std::string registers = register1 + register2;

  request.set_combined_register_vector(registers);

  auto result = CompleteReachOnlySetupPhase(request);
  ASSERT_TRUE(result.ok());

  std::string response_crv = result->combined_register_vector();
  EXPECT_EQ(registers, response_crv);
  EXPECT_EQ(registers.length(), response_crv.length());
  EXPECT_EQ("abc", response_crv.substr(0, 3));
  EXPECT_EQ("def", response_crv.substr(kBytesCipherText, 3));
}

TEST(CompleteReachOnlySetupPhase, SetupPhaseWorksAsExpectedWithGeometricNoise) {
  Context ctx;
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group, ECGroup::Create(kTestCurveId, &ctx));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
                       CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId));
  ASSERT_OK_AND_ASSIGN(auto public_key_pair,
                       el_gamal_cipher->GetPublicKeyBytes());
  ElGamalPublicKey public_key;
  public_key.set_generator(public_key_pair.first);
  public_key.set_element(public_key_pair.second);

  int64_t computed_blinded_histogram_noise_offset = 7;
  int64_t computed_publisher_noise_offset = 7;
  int64_t computed_reach_dp_noise_offset = 4;
  int64_t expected_total_register_count =
      computed_publisher_noise_offset * 2 + computed_reach_dp_noise_offset * 2 +
      computed_blinded_histogram_noise_offset * kPublisherCount *
          (kPublisherCount + 1) +
      2;

  CompleteReachOnlySetupPhaseRequest request;
  request.set_curve_id(kTestCurveId);
  RegisterNoiseGenerationParameters* noise_parameters =
      request.mutable_noise_parameters();
  noise_parameters->set_curve_id(kTestCurveId);
  noise_parameters->set_total_sketches_count(kPublisherCount);
  noise_parameters->set_contributors_count(kWorkerCount);
  *noise_parameters->mutable_composite_el_gamal_public_key() = public_key;
  // resulted p ~= 0 , offset = 7
  *noise_parameters->mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  // resulted p ~= 0 , offset = 7
  *noise_parameters->mutable_dp_params()->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-40));
  // resulted p ~= 0 , offset = 4
  *noise_parameters->mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  request.set_noise_mechanism(NoiseMechanism::GEOMETRIC);
  request.set_parallelism(kParallelism);

  ASSERT_OK_AND_ASSIGN(CompleteReachOnlySetupPhaseResponse response,
                       CompleteReachOnlySetupPhase(request));

  // There was no data in the request, so all registers in the response are
  // noise.
  std::string noises = response.combined_register_vector();
  ASSERT_THAT(noises,
              SizeIs(expected_total_register_count * kBytesPerCipherText));
}

TEST(CompleteReachOnlySetupPhase, SetupPhaseWorksAsExpectedWithGaussianNoise) {
  Context ctx;
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group, ECGroup::Create(kTestCurveId, &ctx));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
                       CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId));
  ASSERT_OK_AND_ASSIGN(auto public_key_pair,
                       el_gamal_cipher->GetPublicKeyBytes());
  ElGamalPublicKey public_key;
  public_key.set_generator(public_key_pair.first);
  public_key.set_element(public_key_pair.second);

  int64_t computed_blinded_histogram_noise_offset = 3;
  int64_t computed_publisher_noise_offset = 2;
  int64_t computed_reach_dp_noise_offset = 3;
  int64_t expected_total_register_count =
      computed_publisher_noise_offset * 2 + computed_reach_dp_noise_offset * 2 +
      computed_blinded_histogram_noise_offset * kPublisherCount *
          (kPublisherCount + 1) +
      2;

  CompleteReachOnlySetupPhaseRequest request;
  request.set_curve_id(kTestCurveId);
  RegisterNoiseGenerationParameters* noise_parameters =
      request.mutable_noise_parameters();
  noise_parameters->set_curve_id(kTestCurveId);
  noise_parameters->set_total_sketches_count(kPublisherCount);
  noise_parameters->set_contributors_count(kWorkerCount);
  *noise_parameters->mutable_composite_el_gamal_public_key() = public_key;
  // resulted sigma_distributed ~= 0.18, offset = 3
  *noise_parameters->mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  // resulted sigma_distributed ~= 0.13, offset = 2
  *noise_parameters->mutable_dp_params()->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-40));
  // resulted sigma_distributed ~= 0.18, offset = 3
  *noise_parameters->mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  request.set_noise_mechanism(NoiseMechanism::DISCRETE_GAUSSIAN);
  request.set_parallelism(kParallelism);

  ASSERT_OK_AND_ASSIGN(CompleteReachOnlySetupPhaseResponse response,
                       CompleteReachOnlySetupPhase(request));
  // There was no data in the request, so all registers in the response are
  // noise.
  std::string noises = response.combined_register_vector();
  ASSERT_THAT(noises,
              SizeIs(expected_total_register_count * kBytesPerCipherText));
}

TEST(CompleteReachOnlyExecutionPhase, WrongInputSketchSizeShouldThrow) {
  CompleteReachOnlyExecutionPhaseRequest request;
  request.set_combined_register_vector("1234");
  request.set_parallelism(kParallelism);

  auto result = CompleteReachOnlyExecutionPhase(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteReachOnlyExecutionPhaseAtAggregator,
     WrongInputSketchSizeShouldThrow) {
  CompleteReachOnlyExecutionPhaseAtAggregatorRequest request;
  request.set_curve_id(kTestCurveId);
  request.set_combined_register_vector("1234");
  request.set_parallelism(kParallelism);

  auto result = CompleteReachOnlyExecutionPhaseAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(EndToEnd, SumOfCountsShouldBeCorrectWithoutNoise) {
  ReachOnlyTest test_data;
  Sketch plain_sketch = CreateReachOnlyEmptyLiquidLegionsSketch();
  int num_registers = 100;
  for (int i = 1; i <= num_registers; i++) {
    AddRegister(&plain_sketch, /*index=*/i);
  }

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();
  int64_t expected_reach = wfa::estimation::EstimateCardinalityLiquidLegions(
      kDecayRate, kLiquidLegionsSize, num_registers, kVidSamplingIntervalWidth);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_geometric_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           NoiseMechanism::GEOMETRIC));

  EXPECT_EQ(result_with_geometric_noise.reach, expected_reach);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_gaussian_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           NoiseMechanism::DISCRETE_GAUSSIAN));
  EXPECT_EQ(result_with_gaussian_noise.reach, expected_reach);
}

TEST(EndToEnd, CombinedCasesWithDeterministicReachDpNoises) {
  ReachOnlyTest test_data;
  Sketch plain_sketch = CreateReachOnlyEmptyLiquidLegionsSketch();
  int valid_register_count = 30;
  for (int i = 1; i <= valid_register_count; i++) {
    AddRegister(&plain_sketch, /*index=*/i);
  }

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  RegisterNoiseGenerationParameters reach_noise_parameters;
  reach_noise_parameters.set_curve_id(kTestCurveId);
  reach_noise_parameters.set_total_sketches_count(3);
  reach_noise_parameters.set_contributors_count(kWorkerCount);
  // For geometric noise, resulted p = 0.716531, offset = 15.
  // Random blind histogram noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(0.11, 0.11);
  // For geometric noise, resulted p = 0.716531, offset = 10.
  // Random noise for publisher noise.
  *reach_noise_parameters.mutable_dp_params()
       ->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(1, 1);
  // For geometric noise, resulted p ~= 0 , offset = 3.
  // Deterministic reach dp noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  *reach_noise_parameters.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;

  int64_t expected_reach = wfa::estimation::EstimateCardinalityLiquidLegions(
      kDecayRate, kLiquidLegionsSize, valid_register_count,
      kVidSamplingIntervalWidth);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_geometric_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, &reach_noise_parameters,
                           NoiseMechanism::GEOMETRIC));

  EXPECT_EQ(result_with_geometric_noise.reach, expected_reach);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_gaussian_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, &reach_noise_parameters,
                           NoiseMechanism::DISCRETE_GAUSSIAN));

  EXPECT_EQ(result_with_gaussian_noise.reach, expected_reach);
}

TEST(ReachEstimation, NonDpNoiseShouldNotImpactTheResult) {
  ReachOnlyTest test_data;
  Sketch plain_sketch = CreateReachOnlyEmptyLiquidLegionsSketch();
  int valid_register_count = 30;
  for (int i = 1; i <= valid_register_count; ++i) {
    AddRegister(&plain_sketch, /*index =*/i);
  }

  RegisterNoiseGenerationParameters reach_noise_parameters;
  reach_noise_parameters.set_curve_id(kTestCurveId);
  reach_noise_parameters.set_total_sketches_count(kPublisherCount);
  reach_noise_parameters.set_contributors_count(kWorkerCount);
  // For geometric noise, resulted p = 0.716531, offset = 15.
  // Random blind histogram noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(0.1, 0.1);
  // For geometric noise, resulted p = 0.716531, offset = 10.
  // Random noise for publisher noise.
  *reach_noise_parameters.mutable_dp_params()
       ->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(1, 1);
  // For geometric noise, resulted p ~= 0 , offset = 3.
  // Deterministic reach dp noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  *reach_noise_parameters.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  int64_t expected_reach = wfa::estimation::EstimateCardinalityLiquidLegions(
      kDecayRate, kLiquidLegionsSize, valid_register_count,
      kVidSamplingIntervalWidth);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_geometric_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, &reach_noise_parameters,
                           NoiseMechanism::GEOMETRIC));
  EXPECT_EQ(result_with_geometric_noise.reach, expected_reach);

  ASSERT_OK_AND_ASSIGN(ReachOnlyMpcResult result_with_gaussian_noise,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, &reach_noise_parameters,
                           NoiseMechanism::DISCRETE_GAUSSIAN));
  EXPECT_EQ(result_with_gaussian_noise.reach, expected_reach);
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
