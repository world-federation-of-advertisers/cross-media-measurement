// Copyright 2020 The Measurement System Authors
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

#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h"

#include <openssl/obj_mac.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/main/cc/any_sketch/crypto/sketch_encrypter.h"
#include "src/test/cc/testutil/matchers.h"
#include "src/test/cc/testutil/status_macros.h"
#include "wfa/measurement/api/v1alpha/sketch.pb.h"
#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_methods.pb.h"

namespace wfa::measurement::common::crypto {
namespace {

using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::testing::SizeIs;
using ::wfa::measurement::api::v1alpha::Sketch;
using ::wfa::measurement::api::v1alpha::SketchConfig;

constexpr int kMaxFrequency = 10;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
constexpr int kBytesPerEcPoint = 33;
constexpr int kBytesCipherText = kBytesPerEcPoint * 2;
constexpr int kBytesPerEncryptedRegister = kBytesCipherText * 3;
constexpr int kBytesPerFlagsCountTuple = kBytesCipherText * 3;
constexpr int kDecayRate = 12;
constexpr int kLiquidLegionsSize = 100 * 1000;

ElGamalKeyPair GenerateRandomElGamalKeyPair(const int curve_id) {
  ElGamalKeyPair el_gamal_key_pair;
  auto el_gamal_cipher =
      CommutativeElGamal::CreateWithNewKeyPair(curve_id).value();
  *el_gamal_key_pair.mutable_secret_key() =
      el_gamal_cipher.get()->GetPrivateKeyBytes().value();
  *el_gamal_key_pair.mutable_public_key()->mutable_generator() =
      el_gamal_cipher.get()->GetPublicKeyBytes().value().first;
  *el_gamal_key_pair.mutable_public_key()->mutable_element() =
      el_gamal_cipher.get()->GetPublicKeyBytes().value().second;
  return el_gamal_key_pair;
}

void AddRegister(Sketch* sketch, const int index, const int key,
                 const int count) {
  auto register_ptr = sketch->add_registers();
  register_ptr->set_index(index);
  register_ptr->add_values(key);
  register_ptr->add_values(count);
}

Sketch CreateEmptyLiquidLegionsSketch() {
  Sketch plain_sketch;
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::UNIQUE);
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::SUM);
  return plain_sketch;
}

// The TestData generates cipher keys for 3 duchies, and the combined public
// key for the data providers.
class TestData {
 public:
  ElGamalKeyPair duchy_1_el_gamal_key_pair_;
  std::string duchy_1_p_h_key_;
  ElGamalKeyPair duchy_2_el_gamal_key_pair_;
  std::string duchy_2_p_h_key_;
  ElGamalKeyPair duchy_3_el_gamal_key_pair_;
  std::string duchy_3_p_h_key_;
  ElGamalPublicKey client_el_gamal_public_key_;  // combined from 3 duchy keys;
  std::unique_ptr<any_sketch::crypto::SketchEncrypter> sketch_encrypter_;

  TestData() {
    duchy_1_el_gamal_key_pair_ = GenerateRandomElGamalKeyPair(kTestCurveId);
    duchy_2_el_gamal_key_pair_ = GenerateRandomElGamalKeyPair(kTestCurveId);
    duchy_3_el_gamal_key_pair_ = GenerateRandomElGamalKeyPair(kTestCurveId);

    // Combine the el_gamal keys from all duchies to generate the data provider
    // el_gamal key.
    Context ctx;
    ECGroup ec_group = ECGroup::Create(kTestCurveId, &ctx).value();
    ECPoint duchy_1_public_el_gamal_y_ec =
        ec_group
            .CreateECPoint(duchy_1_el_gamal_key_pair_.public_key().element())
            .value();
    ECPoint duchy_2_public_el_gamal_y_ec =
        ec_group
            .CreateECPoint(duchy_2_el_gamal_key_pair_.public_key().element())
            .value();
    ECPoint duchy_3_public_el_gamal_y_ec =
        ec_group
            .CreateECPoint(duchy_3_el_gamal_key_pair_.public_key().element())
            .value();
    ECPoint client_public_el_gamal_y_ec =
        duchy_1_public_el_gamal_y_ec.Add(duchy_2_public_el_gamal_y_ec)
            .value()
            .Add(duchy_3_public_el_gamal_y_ec)
            .value();
    client_el_gamal_public_key_.set_generator(
        duchy_1_el_gamal_key_pair_.public_key().generator());
    client_el_gamal_public_key_.set_element(
        client_public_el_gamal_y_ec.ToBytesCompressed().value());

    any_sketch::crypto::CiphertextString client_public_key = {
        .u = client_el_gamal_public_key_.generator(),
        .e = client_el_gamal_public_key_.element(),
    };

    // Create a sketch_encryter for encrypting plaintext any_sketch data.
    sketch_encrypter_ = any_sketch::crypto::CreateWithPublicKey(
                            kTestCurveId, kMaxFrequency, client_public_key)
                            .value();
  }

  absl::StatusOr<std::string> EncryptWithFlaggedKey(const Sketch& sketch) {
    return sketch_encrypter_->Encrypt(
        sketch, any_sketch::crypto::EncryptSketchRequest::FLAGGED_KEY);
  }

  // Helper function to go through the entire MPC protocol using the input data.
  // The final (flag, count) lists are returned.
  absl::StatusOr<CompleteExecutionPhaseThreeAtAggregatorResponse>
  GoThroughEntireMpcProtocolWithoutNoise(const std::string& encrypted_sketch) {
    // Setup phase at Duchy 1.
    // We assume all test data comes from duchy 1 in the test, so we ignore
    // setup phase of Duchy 2 and 3.
    CompleteSetupPhaseRequest complete_setup_phase_request;
    complete_setup_phase_request.set_combined_register_vector(encrypted_sketch);
    ASSIGN_OR_RETURN(CompleteSetupPhaseResponse complete_setup_phase_response,
                     CompleteSetupPhase(complete_setup_phase_request));
    EXPECT_THAT(complete_setup_phase_response.combined_register_vector(),
                IsBlockSorted(kBytesPerEncryptedRegister));

    // Execution phase one at duchy 1 (non-aggregator).
    CompleteExecutionPhaseOneRequest complete_execution_phase_one_request_1;
    *complete_execution_phase_one_request_1.mutable_local_el_gamal_key_pair() =
        duchy_1_el_gamal_key_pair_;
    *complete_execution_phase_one_request_1
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_one_request_1.set_curve_id(kTestCurveId);
    complete_execution_phase_one_request_1.set_combined_register_vector(
        complete_setup_phase_response.combined_register_vector());
    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseOneResponse
            complete_execution_phase_one_response_1,
        CompleteExecutionPhaseOne(complete_execution_phase_one_request_1));
    EXPECT_THAT(
        complete_execution_phase_one_response_1.combined_register_vector(),
        IsBlockSorted(kBytesPerEncryptedRegister));

    // Execution phase onee at duchy 2 (non-aggregator).
    CompleteExecutionPhaseOneRequest complete_execution_phase_one_request_2;
    *complete_execution_phase_one_request_2.mutable_local_el_gamal_key_pair() =
        duchy_2_el_gamal_key_pair_;
    *complete_execution_phase_one_request_2
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_one_request_2.set_curve_id(kTestCurveId);
    complete_execution_phase_one_request_2.set_combined_register_vector(
        complete_execution_phase_one_response_1.combined_register_vector());
    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseOneResponse
            complete_execution_phase_one_response_2,
        CompleteExecutionPhaseOne(complete_execution_phase_one_request_2));
    EXPECT_THAT(
        complete_execution_phase_one_response_2.combined_register_vector(),
        IsBlockSorted(kBytesPerEncryptedRegister));

    // Execution phase one at duchy 3 (aggregator).
    CompleteExecutionPhaseOneAtAggregatorRequest
        complete_execution_phase_one_at_aggregator_request;
    *complete_execution_phase_one_at_aggregator_request
         .mutable_local_el_gamal_key_pair() = duchy_3_el_gamal_key_pair_;
    *complete_execution_phase_one_at_aggregator_request
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_one_at_aggregator_request.set_curve_id(
        kTestCurveId);
    complete_execution_phase_one_at_aggregator_request
        .mutable_liquid_legions_parameters()
        ->set_decay_rate(kDecayRate);
    complete_execution_phase_one_at_aggregator_request
        .mutable_liquid_legions_parameters()
        ->set_size(kLiquidLegionsSize);
    complete_execution_phase_one_at_aggregator_request
        .set_combined_register_vector(
            complete_execution_phase_one_response_2.combined_register_vector());
    ASSIGN_OR_RETURN(CompleteExecutionPhaseOneAtAggregatorResponse
                         complete_execution_phase_one_at_aggregator_response,
                     CompleteExecutionPhaseOneAtAggregator(
                         complete_execution_phase_one_at_aggregator_request));
    EXPECT_GT(complete_execution_phase_one_at_aggregator_response.reach(), 0);
    EXPECT_THAT(
        complete_execution_phase_one_at_aggregator_response.flag_count_tuples(),
        IsBlockSorted(kBytesPerFlagsCountTuple));

    // Execution phase two at duchy 1 (non-aggregator).
    CompleteExecutionPhaseTwoRequest complete_execution_phase_two_request_1;
    *complete_execution_phase_two_request_1.mutable_local_el_gamal_key_pair() =
        duchy_1_el_gamal_key_pair_;
    *complete_execution_phase_two_request_1
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_two_request_1.set_curve_id(kTestCurveId);
    complete_execution_phase_two_request_1.set_flag_count_tuples(
        complete_execution_phase_one_at_aggregator_response
            .flag_count_tuples());

    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseTwoResponse
            complete_execution_phase_two_response_1,
        CompleteExecutionPhaseTwo(complete_execution_phase_two_request_1));
    EXPECT_THAT(complete_execution_phase_two_response_1.flag_count_tuples(),
                IsBlockSorted(kBytesPerFlagsCountTuple));

    // Execution phase two at duchy 2 (non-aggregator).
    CompleteExecutionPhaseTwoRequest complete_execution_phase_two_request_2;
    *complete_execution_phase_two_request_2.mutable_local_el_gamal_key_pair() =
        duchy_2_el_gamal_key_pair_;
    *complete_execution_phase_two_request_2
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_two_request_2.set_curve_id(kTestCurveId);
    complete_execution_phase_two_request_2.set_flag_count_tuples(
        complete_execution_phase_two_response_1.flag_count_tuples());

    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseTwoResponse
            complete_execution_phase_two_response_2,
        CompleteExecutionPhaseTwo(complete_execution_phase_two_request_2));
    EXPECT_THAT(complete_execution_phase_two_response_2.flag_count_tuples(),
                IsBlockSorted(kBytesPerFlagsCountTuple));

    // Execution phase two at duchy 3 (aggregator).
    CompleteExecutionPhaseTwoAtAggregatorRequest
        complete_execution_phase_two_at_aggregator_request;
    *complete_execution_phase_two_at_aggregator_request
         .mutable_local_el_gamal_key_pair() = duchy_3_el_gamal_key_pair_;
    *complete_execution_phase_two_at_aggregator_request
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_two_at_aggregator_request.set_curve_id(
        kTestCurveId);
    complete_execution_phase_two_at_aggregator_request.set_maximum_frequency(
        kMaxFrequency);
    complete_execution_phase_two_at_aggregator_request.set_flag_count_tuples(
        complete_execution_phase_two_response_2.flag_count_tuples());
    ASSIGN_OR_RETURN(CompleteExecutionPhaseTwoAtAggregatorResponse
                         complete_execution_phase_two_at_aggregator_response,
                     CompleteExecutionPhaseTwoAtAggregator(
                         complete_execution_phase_two_at_aggregator_request));

    // Execution phase three at duchy 1 (non-aggregator).
    CompleteExecutionPhaseThreeRequest complete_execution_phase_three_request_1;
    *complete_execution_phase_three_request_1
         .mutable_local_el_gamal_key_pair() = duchy_1_el_gamal_key_pair_;
    complete_execution_phase_three_request_1.set_curve_id(kTestCurveId);
    complete_execution_phase_three_request_1.set_same_key_aggregator_matrix(
        complete_execution_phase_two_at_aggregator_response
            .same_key_aggregator_matrix());

    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseThreeResponse
            complete_execution_phase_three_response_1,
        CompleteExecutionPhaseThree(complete_execution_phase_three_request_1));

    // Execution phase three at duchy 2 (non-aggregator).
    CompleteExecutionPhaseThreeRequest complete_execution_phase_three_request_2;
    *complete_execution_phase_three_request_2
         .mutable_local_el_gamal_key_pair() = duchy_2_el_gamal_key_pair_;
    complete_execution_phase_three_request_2.set_curve_id(kTestCurveId);
    complete_execution_phase_three_request_2.set_same_key_aggregator_matrix(
        complete_execution_phase_three_response_1.same_key_aggregator_matrix());

    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseThreeResponse
            complete_execution_phase_three_response_2,
        CompleteExecutionPhaseThree(complete_execution_phase_three_request_2));

    // Execution phase three at duchy 3 (aggregator).
    CompleteExecutionPhaseThreeAtAggregatorRequest
        complete_execution_phase_three_at_aggregator_request;
    *complete_execution_phase_three_at_aggregator_request
         .mutable_local_el_gamal_key_pair() = duchy_3_el_gamal_key_pair_;
    complete_execution_phase_three_at_aggregator_request.set_curve_id(
        kTestCurveId);
    complete_execution_phase_three_at_aggregator_request.set_maximum_frequency(
        kMaxFrequency);
    complete_execution_phase_three_at_aggregator_request
        .set_same_key_aggregator_matrix(
            complete_execution_phase_three_response_2
                .same_key_aggregator_matrix());

    return CompleteExecutionPhaseThreeAtAggregator(
        complete_execution_phase_three_at_aggregator_request);
  }
};

TEST(CompleteSetupPhase, WrongInputSketchSizeShouldThrow) {
  CompleteSetupPhaseRequest request;
  request.set_combined_register_vector("1234");

  auto result = CompleteSetupPhase(request);

  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseOne, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseOneRequest request;

  auto result = CompleteExecutionPhaseOne(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_combined_register_vector("1234");
  result = CompleteExecutionPhaseOne(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseOneAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseOneAtAggregatorRequest request;

  auto result = CompleteExecutionPhaseOneAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_combined_register_vector("1234");
  result = CompleteExecutionPhaseOneAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseTwo, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseTwoRequest request;

  auto result = CompleteExecutionPhaseTwo(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_flag_count_tuples("1234");
  result = CompleteExecutionPhaseTwo(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseTwoAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseTwoAtAggregatorRequest request;

  auto result = CompleteExecutionPhaseTwoAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_flag_count_tuples("1234");
  result = CompleteExecutionPhaseTwoAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseThree, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseThreeRequest request;

  auto result = CompleteExecutionPhaseThree(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_same_key_aggregator_matrix("1234");
  result = CompleteExecutionPhaseThree(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseThreeAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseThreeAtAggregatorRequest request;

  auto result = CompleteExecutionPhaseThreeAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "empty"));

  request.set_same_key_aggregator_matrix("1234");
  result = CompleteExecutionPhaseThreeAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(EndToEnd, SumOfCountsShouldBeCorrect) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 1);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(
      CompleteExecutionPhaseThreeAtAggregatorResponse final_response,
      test_data.GoThroughEntireMpcProtocolWithoutNoise(encrypted_sketch));

  auto frequency_distribution = final_response.frequency_distribution();
  ASSERT_THAT(frequency_distribution, SizeIs(1));
  EXPECT_NEAR(frequency_distribution[6], 1.0, 0.001);
}

TEST(EndToEnd, LocallyDistroyedRegisterShouldBeIgnored) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ -1,
              /* count = */ 2);  // locally destroyed.

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(
      CompleteExecutionPhaseThreeAtAggregatorResponse final_response,
      test_data.GoThroughEntireMpcProtocolWithoutNoise(encrypted_sketch));

  auto frequency_distribution = final_response.frequency_distribution();
  ASSERT_THAT(frequency_distribution, SizeIs(1));
  EXPECT_NEAR(frequency_distribution[3], 1.0, 0.001);
}

TEST(EndToEnd, CrossPublisherDistroyedRegistersShouldBeIgnored) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 201, /* count = */ 4);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 202, /* count = */ 1);
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(
      CompleteExecutionPhaseThreeAtAggregatorResponse final_response,
      test_data.GoThroughEntireMpcProtocolWithoutNoise(encrypted_sketch));

  auto frequency_distribution = final_response.frequency_distribution();
  ASSERT_THAT(frequency_distribution, SizeIs(1));
  EXPECT_NEAR(frequency_distribution[3], 1.0, 0.001);
}

TEST(EndToEnd, SumOfCountsShouldBeCappedbyMaxFrequency) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 222, /* count = */ 5);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 222,
              /* count = */ 7);  // 5+7>10
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(
      CompleteExecutionPhaseThreeAtAggregatorResponse final_response,
      test_data.GoThroughEntireMpcProtocolWithoutNoise(encrypted_sketch));

  auto frequency_distribution = final_response.frequency_distribution();
  ASSERT_THAT(frequency_distribution, SizeIs(2));
  EXPECT_NEAR(frequency_distribution[3], 0.5, 0.001);
  EXPECT_NEAR(frequency_distribution[kMaxFrequency + 1], 0.5, 0.001);
}

TEST(EndToEnd, ComnbinedCases) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  // register 1: count = 6
  // register 2: count = 3
  // register 3: count = 1+2+3 = 6
  // register 4: count = 3+4+5 = 12 (capped as kMaxFrequency+1, i.e. 11
  // register 5: locally destroyed (key=-1), ignored
  // register 6: destroyed by conflicting keys (601 and 602), ignored
  // register 7: locally destroyed (key=-1) + normal register, ignored
  //
  // final result should be { 3->1/4, 6->2/4, 11->1/4 }
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 6);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 222, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 333, /* count = */ 1);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 333, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 333, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 4, /* key = */ 444, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 4, /* key = */ 444, /* count = */ 4);
  AddRegister(&plain_sketch, /* index = */ 4, /* key = */ 444, /* count = */ 5);
  AddRegister(&plain_sketch, /* index = */ 5, /* key = */ -1, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 6, /* key = */ 601, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 6, /* key = */ 602, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 7, /* key = */ -1, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 7, /* key = */ 777, /* count = */ 2);

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(
      CompleteExecutionPhaseThreeAtAggregatorResponse final_response,
      test_data.GoThroughEntireMpcProtocolWithoutNoise(encrypted_sketch));

  auto frequency_distribution = final_response.frequency_distribution();
  ASSERT_THAT(frequency_distribution, SizeIs(3));
  EXPECT_NEAR(frequency_distribution[3], 0.25, 0.001);
  EXPECT_NEAR(frequency_distribution[6], 0.5, 0.001);
  EXPECT_NEAR(frequency_distribution[kMaxFrequency + 1], 0.25, 0.001);
}

}  // namespace
}  // namespace wfa::measurement::common::crypto