// Copyright 2020 The Cross-Media Measurement Authors
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

#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2/liquid_legions_v2_encryption_utility.h"

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
#include "wfa/measurement/internal/duchy/protocol/liquid_legions_v2_encryption_methods.pb.h"

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
using ::wfa::measurement::common::crypto::kBytesPerCipherRegister;
using ::wfa::measurement::common::crypto::kDestroyedRegisterKey;
using ::wfa::measurement::common::crypto::kPaddingNoiseRegisterId;
using ::wfa::measurement::common::crypto::kPublisherNoiseRegisterId;
using ::wfa::measurement::internal::duchy::DifferentialPrivacyParams;
using ::wfa::measurement::internal::duchy::ElGamalKeyPair;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;

constexpr int kWorkerCount = 3;
constexpr int kPublisherCount = 3;
constexpr int kMaxFrequency = 10;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
constexpr int kBytesPerEcPoint = 33;
constexpr int kBytesCipherText = kBytesPerEcPoint * 2;
constexpr int kBytesPerEncryptedRegister = kBytesCipherText * 3;
constexpr int kBytesPerFlagsCountTuple = kBytesCipherText * 4;
constexpr int kDecayRate = 12;
constexpr int kLiquidLegionsSize = 100 * 1000;
constexpr float kVidSamplingIntervalWidth = 0.5;

struct MpcResult {
  int64_t reach;
  absl::flat_hash_map<int64_t, double> frequency_distribution;
};

void AddRegister(Sketch* sketch, const int index, const int key,
                 const int count) {
  auto register_ptr = sketch->add_registers();
  register_ptr->set_index(index);
  register_ptr->add_values(key);
  register_ptr->add_values(count);
}

::wfa::any_sketch::crypto::ElGamalPublicKey ToAnysketchElGamalKey(
    ElGamalPublicKey key) {
  ::wfa::any_sketch::crypto::ElGamalPublicKey result;
  result.set_generator(key.generator());
  result.set_element(key.element());
  return result;
}

ElGamalPublicKey ToCmmsElGamalKey(
    ::wfa::any_sketch::crypto::ElGamalPublicKey key) {
  ElGamalPublicKey result;
  result.set_generator(key.generator());
  result.set_element(key.element());
  return result;
}

Sketch CreateEmptyLiquidLegionsSketch() {
  Sketch plain_sketch;
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::UNIQUE);
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::SUM);
  return plain_sketch;
}

DifferentialPrivacyParams MakeDifferentialPrivacyParams(double epsilon,
                                                        double delta) {
  DifferentialPrivacyParams params;
  params.set_epsilon(epsilon);
  params.set_delta(delta);
  return params;
}

// Partition the char vector 33 by 33, and convert the results to strings
std::vector<std::string> GetCipherStrings(absl::string_view bytes) {
  ABSL_ASSERT(bytes.size() % 66 == 0);
  std::vector<std::string> result;
  int word_cnt = bytes.size() / 33;
  result.reserve(word_cnt);
  for (int i = 0; i < word_cnt; ++i) {
    result.emplace_back(bytes.substr(i * 33, 33));
  }
  return result;
}

absl::StatusOr<bool> IsEncryptionOfPlaintext(CommutativeElGamal& cipher,
                                             absl::string_view plaintext,
                                             ElGamalCiphertext ciphertext) {
  ASSIGN_OR_RETURN(std::string decrypted_plaintext, cipher.Decrypt(ciphertext));
  return decrypted_plaintext == plaintext;
}

absl::StatusOr<bool> IsEncryptionOfRawText(CommutativeElGamal& cipher,
                                           ECGroup& ec_group,
                                           absl::string_view raw_text,
                                           ElGamalCiphertext ciphertext) {
  ASSIGN_OR_RETURN(ECPoint expected_plaintext_ec,
                   ec_group.GetPointByHashingToCurveSha256(raw_text));
  ASSIGN_OR_RETURN(std::string expected_plaintext,
                   expected_plaintext_ec.ToBytesCompressed());
  return IsEncryptionOfPlaintext(cipher, expected_plaintext, ciphertext);
}

absl::StatusOr<bool> IsBlindedHistogramNoise(CommutativeElGamal& cipher,
                                             ECGroup& ec_group,
                                             absl::string_view register_bytes) {
  ABSL_ASSERT(register_bytes.size() % kBytesPerCipherRegister == 0);
  std::vector<std::string> ciphertexts = GetCipherStrings(register_bytes);
  // check if the key is kBlindedHistogramNoiseRegisterKey
  return IsEncryptionOfRawText(cipher, ec_group,
                               kBlindedHistogramNoiseRegisterKey,
                               std::make_pair(ciphertexts[2], ciphertexts[3]));
}

absl::StatusOr<bool> IsNoiseForPublisherNoiseRegister(
    CommutativeElGamal& cipher, ECGroup& ec_group,
    absl::string_view register_bytes) {
  ABSL_ASSERT(register_bytes.size() % kBytesPerCipherRegister == 0);
  std::vector<std::string> ciphertexts = GetCipherStrings(register_bytes);
  // check if the register id is kPublisherNoiseRegisterId
  return IsEncryptionOfRawText(cipher, ec_group, kPublisherNoiseRegisterId,
                               std::make_pair(ciphertexts[0], ciphertexts[1]));
}

absl::StatusOr<bool> IsReachDpNoiseRegister(CommutativeElGamal& cipher,
                                            ECGroup& ec_group,
                                            absl::string_view register_bytes) {
  ABSL_ASSERT(register_bytes.size() % kBytesPerCipherRegister == 0);
  std::vector<std::string> ciphertexts = GetCipherStrings(register_bytes);
  // check if the key is kDestroyedRegisterKey
  return IsEncryptionOfRawText(cipher, ec_group, kDestroyedRegisterKey,
                               std::make_pair(ciphertexts[2], ciphertexts[3]));
}

absl::StatusOr<bool> IsPaddingNoiseRegister(CommutativeElGamal& cipher,
                                            ECGroup& ec_group,
                                            absl::string_view register_bytes) {
  ABSL_ASSERT(register_bytes.size() % kBytesPerCipherRegister == 0);
  std::vector<std::string> ciphertexts = GetCipherStrings(register_bytes);
  // check if the register id is kPaddingNoiseRegisterId
  return IsEncryptionOfRawText(cipher, ec_group, kPaddingNoiseRegisterId,
                               std::make_pair(ciphertexts[0], ciphertexts[1]));
}

absl::StatusOr<bool> IsDecryptionNonZero(CommutativeElGamal& cipher,
                                         const ElGamalCiphertext& ciphertext) {
  absl::StatusOr<std::string> decryption = cipher.Decrypt(ciphertext);
  if (decryption.ok()) {
    return true;
  } else if (absl::IsInternal(decryption.status()) &&
             decryption.status().message().find("POINT_AT_INFINITY") !=
                 std::string::npos) {
    // When the value is 0 (Point at Infinity), the decryption would
    // fail with message "POINT_AT_INFINITY".
    return false;
  } else {
    return decryption.status();
  }
}

absl::StatusOr<std::array<bool, 3>> GetFlagsFromFourTuples(
    CommutativeElGamal& cipher, absl::string_view four_tuples_bytes) {
  ABSL_ASSERT(four_tuples_bytes.size() % kBytesPerFlagsCountTuple == 0);
  std::vector<std::string> ciphertexts = GetCipherStrings(four_tuples_bytes);
  std::array<bool, 3> flags;
  ASSIGN_OR_RETURN(flags[0],
                   IsDecryptionNonZero(
                       cipher, std::make_pair(ciphertexts[0], ciphertexts[1])));
  ASSIGN_OR_RETURN(flags[1],
                   IsDecryptionNonZero(
                       cipher, std::make_pair(ciphertexts[2], ciphertexts[3])));
  ASSIGN_OR_RETURN(flags[2],
                   IsDecryptionNonZero(
                       cipher, std::make_pair(ciphertexts[4], ciphertexts[5])));
  return flags;
}

absl::StatusOr<bool> IsFrequencyDpNoiseTuples(
    CommutativeElGamal& cipher, absl::string_view four_tuples_bytes) {
  ASSIGN_OR_RETURN(auto flags,
                   GetFlagsFromFourTuples(cipher, four_tuples_bytes));
  // (0, R, R)
  return !flags[0] && flags[1] && flags[2];
}

absl::StatusOr<bool> IsDestroyedFrequencyNoiseTuples(
    CommutativeElGamal& cipher, absl::string_view four_tuples_bytes) {
  ASSIGN_OR_RETURN(auto flags,
                   GetFlagsFromFourTuples(cipher, four_tuples_bytes));
  // (R, R, R)
  return flags[0] && flags[1] && flags[2];
}

absl::StatusOr<bool> IsPaddingFrequencyNoiseTuples(
    CommutativeElGamal& cipher, absl::string_view four_tuples_bytes) {
  ASSIGN_OR_RETURN(auto flags,
                   GetFlagsFromFourTuples(cipher, four_tuples_bytes));
  // (0, 0, R)
  return !flags[0] && !flags[1] && flags[2];
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
  ElGamalPublicKey duchy_2_3_composite_public_key_;  // combined from duchy 2
                                                     // and duchy_3 public keys;
  std::unique_ptr<any_sketch::crypto::SketchEncrypter> sketch_encrypter_;

  TestData() {
    CompleteInitializationPhaseRequest complete_initialization_phase_request;
    complete_initialization_phase_request.set_curve_id(kTestCurveId);

    duchy_1_el_gamal_key_pair_ =
        CompleteInitializationPhase(complete_initialization_phase_request)
            ->el_gamal_key_pair();
    duchy_2_el_gamal_key_pair_ =
        CompleteInitializationPhase(complete_initialization_phase_request)
            ->el_gamal_key_pair();
    duchy_3_el_gamal_key_pair_ =
        CompleteInitializationPhase(complete_initialization_phase_request)
            ->el_gamal_key_pair();

    // Combine the el_gamal keys from all duchies to generate the data provider
    // el_gamal key.
    client_el_gamal_public_key_ = ToCmmsElGamalKey(
        any_sketch::crypto::CombineElGamalPublicKeys(
            kTestCurveId,
            {ToAnysketchElGamalKey(duchy_1_el_gamal_key_pair_.public_key()),
             ToAnysketchElGamalKey(duchy_2_el_gamal_key_pair_.public_key()),
             ToAnysketchElGamalKey(duchy_3_el_gamal_key_pair_.public_key())})
            .value());
    duchy_2_3_composite_public_key_ = ToCmmsElGamalKey(
        any_sketch::crypto::CombineElGamalPublicKeys(
            kTestCurveId,
            {ToAnysketchElGamalKey(duchy_2_el_gamal_key_pair_.public_key()),
             ToAnysketchElGamalKey(duchy_3_el_gamal_key_pair_.public_key())})
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
  // The final MpcResult are returned.
  absl::StatusOr<MpcResult> GoThroughEntireMpcProtocol(
      const std::string& encrypted_sketch,
      RegisterNoiseGenerationParameters* reach_noise_parameters,
      FlagCountTupleNoiseGenerationParameters* frequency_noise_parameters) {
    // Setup phase at Duchy 1.
    // We assume all test data comes from duchy 1 in the test.
    CompleteSetupPhaseRequest complete_setup_phase_request_1;
    complete_setup_phase_request_1.set_combined_register_vector(
        encrypted_sketch);

    if (reach_noise_parameters != nullptr) {
      *complete_setup_phase_request_1.mutable_noise_parameters() =
          *reach_noise_parameters;
    }

    ASSIGN_OR_RETURN(CompleteSetupPhaseResponse complete_setup_phase_response_1,
                     CompleteSetupPhase(complete_setup_phase_request_1));
    EXPECT_THAT(complete_setup_phase_response_1.combined_register_vector(),
                IsBlockSorted(kBytesPerEncryptedRegister));

    // Setup phase at Duchy 2.
    // We assume all test data comes from duchy 1 in the test, so there is only
    // noise from duchy 2 (if configured)
    CompleteSetupPhaseRequest complete_setup_phase_request_2;
    if (reach_noise_parameters != nullptr) {
      *complete_setup_phase_request_2.mutable_noise_parameters() =
          *reach_noise_parameters;
    }

    ASSIGN_OR_RETURN(CompleteSetupPhaseResponse complete_setup_phase_response_2,
                     CompleteSetupPhase(complete_setup_phase_request_2));
    EXPECT_THAT(complete_setup_phase_response_2.combined_register_vector(),
                IsBlockSorted(kBytesPerEncryptedRegister));

    // Setup phase at Duchy 3.
    // We assume all test data comes from duchy 1 in the test, so there is only
    // noise from duchy 3 (if configured)
    CompleteSetupPhaseRequest complete_setup_phase_request_3;
    if (reach_noise_parameters != nullptr) {
      *complete_setup_phase_request_3.mutable_noise_parameters() =
          *reach_noise_parameters;
    }

    ASSIGN_OR_RETURN(CompleteSetupPhaseResponse complete_setup_phase_response_3,
                     CompleteSetupPhase(complete_setup_phase_request_3));
    EXPECT_THAT(complete_setup_phase_response_3.combined_register_vector(),
                IsBlockSorted(kBytesPerEncryptedRegister));

    // Combine all CRVs.
    std::string combine_data = absl::StrCat(
        complete_setup_phase_response_1.combined_register_vector(),
        complete_setup_phase_response_2.combined_register_vector(),
        complete_setup_phase_response_3.combined_register_vector());

    // Execution phase one at duchy 1 (non-aggregator).
    CompleteExecutionPhaseOneRequest complete_execution_phase_one_request_1;
    *complete_execution_phase_one_request_1.mutable_local_el_gamal_key_pair() =
        duchy_1_el_gamal_key_pair_;
    *complete_execution_phase_one_request_1
         .mutable_composite_el_gamal_public_key() = client_el_gamal_public_key_;
    complete_execution_phase_one_request_1.set_curve_id(kTestCurveId);
    complete_execution_phase_one_request_1.set_combined_register_vector(
        combine_data);
    ASSIGN_OR_RETURN(
        CompleteExecutionPhaseOneResponse
            complete_execution_phase_one_response_1,
        CompleteExecutionPhaseOne(complete_execution_phase_one_request_1));
    EXPECT_THAT(
        complete_execution_phase_one_response_1.combined_register_vector(),
        IsBlockSorted(kBytesPerEncryptedRegister));

    // Execution phase one at duchy 2 (non-aggregator).
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
        .set_combined_register_vector(
            complete_execution_phase_one_response_2.combined_register_vector());
    complete_execution_phase_one_at_aggregator_request.set_total_sketches_count(
        kPublisherCount);
    if (frequency_noise_parameters != nullptr) {
      *complete_execution_phase_one_at_aggregator_request
           .mutable_noise_parameters() = *frequency_noise_parameters;
    }

    ASSIGN_OR_RETURN(CompleteExecutionPhaseOneAtAggregatorResponse
                         complete_execution_phase_one_at_aggregator_response,
                     CompleteExecutionPhaseOneAtAggregator(
                         complete_execution_phase_one_at_aggregator_request));
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
    *complete_execution_phase_two_request_1
         .mutable_partial_composite_el_gamal_public_key() =
        duchy_2_3_composite_public_key_;
    if (frequency_noise_parameters != nullptr) {
      *complete_execution_phase_two_request_1.mutable_noise_parameters() =
          *frequency_noise_parameters;
    }

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
    *complete_execution_phase_two_request_2
         .mutable_partial_composite_el_gamal_public_key() =
        duchy_3_el_gamal_key_pair_.public_key();
    if (frequency_noise_parameters != nullptr) {
      *complete_execution_phase_two_request_2.mutable_noise_parameters() =
          *frequency_noise_parameters;
    }

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
    complete_execution_phase_two_at_aggregator_request
        .mutable_liquid_legions_parameters()
        ->set_decay_rate(kDecayRate);
    complete_execution_phase_two_at_aggregator_request
        .mutable_liquid_legions_parameters()
        ->set_size(kLiquidLegionsSize);
    if (reach_noise_parameters != nullptr) {
      complete_execution_phase_two_at_aggregator_request
          .mutable_reach_dp_noise_baseline()
          ->set_contributors_count(3);
      *complete_execution_phase_two_at_aggregator_request
           .mutable_reach_dp_noise_baseline()
           ->mutable_global_reach_dp_noise() =
          reach_noise_parameters->dp_params().global_reach_dp_noise();
    }
    if (frequency_noise_parameters != nullptr) {
      *complete_execution_phase_two_at_aggregator_request
           .mutable_frequency_noise_parameters() = *frequency_noise_parameters;
    }
    complete_execution_phase_two_at_aggregator_request.set_flag_count_tuples(
        complete_execution_phase_two_response_2.flag_count_tuples());
    complete_execution_phase_two_at_aggregator_request
        .set_vid_sampling_interval_width(kVidSamplingIntervalWidth);
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
    if (frequency_noise_parameters != nullptr) {
      *complete_execution_phase_three_at_aggregator_request
           .mutable_global_frequency_dp_noise_per_bucket()
           ->mutable_dp_params() = frequency_noise_parameters->dp_params();
      complete_execution_phase_three_at_aggregator_request
          .mutable_global_frequency_dp_noise_per_bucket()
          ->set_contributors_count(3);
    }

    ASSIGN_OR_RETURN(CompleteExecutionPhaseThreeAtAggregatorResponse
                         complete_execution_phase_three_at_aggregator_response,
                     CompleteExecutionPhaseThreeAtAggregator(
                         complete_execution_phase_three_at_aggregator_request));

    MpcResult result;
    result.reach = complete_execution_phase_two_at_aggregator_response.reach();
    for (auto pair : complete_execution_phase_three_at_aggregator_response
                         .frequency_distribution()) {
      result.frequency_distribution[pair.first] = pair.second;
    }
    return result;
  }
};

TEST(CompleteSetupPhase, NoiseSumAndMeanShouldBeCorrect) {
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
          (kPublisherCount + 1);

  CompleteSetupPhaseRequest request;
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

  ASSERT_OK_AND_ASSIGN(CompleteSetupPhaseResponse response,
                       CompleteSetupPhase(request));
  // There was no data in the request, so all registers in the response are
  // noise.
  std::string noises = response.combined_register_vector();
  ASSERT_THAT(noises, SizeIs(expected_total_register_count *
                             kBytesPerEncryptedRegister));

  int64_t blinded_histogram_noise_count = 0;
  int64_t publisher_noise_count = 0;
  int64_t reach_dp_noise_count = 0;
  int64_t padding_noise_count = 0;

  for (int i = 0; i < expected_total_register_count; ++i) {
    ASSERT_OK_AND_ASSIGN(
        bool is_blinded_histogram_noise,
        IsBlindedHistogramNoise(*el_gamal_cipher, ec_group,
                                noises.substr(i * kBytesPerEncryptedRegister,
                                              kBytesPerEncryptedRegister)));
    ASSERT_OK_AND_ASSIGN(
        bool is_reach_dp_noise,
        IsReachDpNoiseRegister(*el_gamal_cipher, ec_group,
                               noises.substr(i * kBytesPerEncryptedRegister,
                                             kBytesPerEncryptedRegister)));
    ASSERT_OK_AND_ASSIGN(bool is_publisher_noise,
                         IsNoiseForPublisherNoiseRegister(
                             *el_gamal_cipher, ec_group,
                             noises.substr(i * kBytesPerEncryptedRegister,
                                           kBytesPerEncryptedRegister)));
    ASSERT_OK_AND_ASSIGN(
        bool is_padding_noise,
        IsPaddingNoiseRegister(*el_gamal_cipher, ec_group,
                               noises.substr(i * kBytesPerEncryptedRegister,
                                             kBytesPerEncryptedRegister)));
    blinded_histogram_noise_count += is_blinded_histogram_noise;
    reach_dp_noise_count += is_reach_dp_noise;
    publisher_noise_count += is_publisher_noise;
    padding_noise_count += is_padding_noise;
    // Assert exact 1 boolean is true.
    EXPECT_EQ(is_blinded_histogram_noise + is_reach_dp_noise +
                  is_publisher_noise + is_padding_noise,
              1);
  }

  EXPECT_EQ(publisher_noise_count, computed_publisher_noise_offset);
  EXPECT_EQ(blinded_histogram_noise_count,
            computed_blinded_histogram_noise_offset * kPublisherCount *
                (kPublisherCount + 1) / 2);
  EXPECT_EQ(reach_dp_noise_count, computed_reach_dp_noise_offset);
  EXPECT_EQ(padding_noise_count,
            expected_total_register_count - publisher_noise_count -
                blinded_histogram_noise_count - reach_dp_noise_count);
}

TEST(CompleteSetupPhase, WrongInputSketchSizeShouldThrow) {
  CompleteSetupPhaseRequest request;
  request.set_combined_register_vector("1234");

  auto result = CompleteSetupPhase(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteSetupPhase, FrequencyOneWorksAsExpectedWithoutNoise) {
  CompleteSetupPhaseRequest request;

  std::string register1 = "abc";
  std::string register2 = "def";
  for (int i = 3; i < kBytesPerCipherRegister; i++) {
    register1 = register1 + " ";
    register2 = register2 + " ";
  }
  std::string registers = register1 + register2;

  request.set_combined_register_vector(registers);
  request.set_maximum_frequency(1);

  auto result = CompleteSetupPhase(request);
  ASSERT_TRUE(result.ok());

  std::string response_crv = result->combined_register_vector();
  EXPECT_NE(registers, response_crv);
  EXPECT_EQ(registers.length(), response_crv.length());
  EXPECT_EQ("abc", response_crv.substr(0, 3));
  EXPECT_EQ("def", response_crv.substr(kBytesPerCipherRegister, 3));
}

TEST(CompleteSetupPhase, FrequencyOneWorksAsExpectedWithNoise) {
  CompleteSetupPhaseRequest request;
  TestData test_data;

  RegisterNoiseGenerationParameters reach_noise_parameters;
  reach_noise_parameters.set_curve_id(kTestCurveId);
  reach_noise_parameters.set_total_sketches_count(kPublisherCount);
  reach_noise_parameters.set_contributors_count(kWorkerCount);
  *reach_noise_parameters.mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(100, 1);
  *reach_noise_parameters.mutable_dp_params()
       ->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(100, 1);
  *reach_noise_parameters.mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  *reach_noise_parameters.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;

  *request.mutable_noise_parameters() = reach_noise_parameters;

  std::string register1 = "abc";
  std::string register2 = "def";
  for (int i = 3; i < kBytesPerCipherRegister; i++) {
    register1 = register1 + " ";
    register2 = register2 + " ";
  }
  std::string registers = register1 + register2;

  request.set_combined_register_vector(registers);
  request.set_maximum_frequency(1);

  auto result = CompleteSetupPhase(request);
  ASSERT_TRUE(result.ok());
  EXPECT_NE(registers, result->combined_register_vector());
}

TEST(CompleteExecutionPhaseOne, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseOneRequest request;
  request.set_combined_register_vector("1234");

  auto result = CompleteExecutionPhaseOne(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseOneAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseOneAtAggregatorRequest request;
  request.set_combined_register_vector("1234");

  auto result = CompleteExecutionPhaseOneAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseTwo, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseTwoRequest request;
  request.set_flag_count_tuples("1234");

  auto result = CompleteExecutionPhaseTwo(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseTwoAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseTwoAtAggregatorRequest request;
  request.set_flag_count_tuples("1234");

  auto result = CompleteExecutionPhaseTwoAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseThree, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseThreeRequest request;
  request.set_same_key_aggregator_matrix("1234");

  auto result = CompleteExecutionPhaseThree(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(CompleteExecutionPhaseThreeAtAggregator, WrongInputSketchSizeShouldThrow) {
  CompleteExecutionPhaseThreeAtAggregatorRequest request;
  request.set_same_key_aggregator_matrix("1234");

  auto result = CompleteExecutionPhaseThreeAtAggregator(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "not divisible"));
}

TEST(EndToEnd, SumOfCountsShouldBeCorrect) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/1);
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/3);

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           /*frequency_noise=*/nullptr));

  ASSERT_THAT(result.frequency_distribution, SizeIs(1));
  EXPECT_NEAR(result.frequency_distribution[6], 1.0, 0.001);
}

TEST(EndToEnd, LocallyDestroyedRegisterShouldBeIgnored) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/-1,
              /*count=*/2);  // locally destroyed.

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           /*frequency_noise=*/nullptr));

  ASSERT_THAT(result.frequency_distribution, SizeIs(1));
  EXPECT_NEAR(result.frequency_distribution[3], 1.0, 0.001);
}

TEST(EndToEnd, CrossPublisherDestroyedRegistersShouldBeIgnored) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/201, /*count=*/4);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/202, /*count=*/1);
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           /*frequency_noise=*/nullptr));

  ASSERT_THAT(result.frequency_distribution, SizeIs(1));
  EXPECT_NEAR(result.frequency_distribution[3], 1.0, 0.001);
}

TEST(EndToEnd, SumOfCountsShouldBeCappedbyMaxFrequency) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/222, /*count=*/5);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/222, /*count=*/7);  // 5+7>10
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, /*reach_noise=*/nullptr,
                           /*frequency_noise=*/nullptr));

  ASSERT_THAT(result.frequency_distribution, SizeIs(2));
  EXPECT_NEAR(result.frequency_distribution[3], 0.5, 0.001);
  EXPECT_NEAR(result.frequency_distribution[kMaxFrequency], 0.5, 0.001);
}

TEST(ReachEstimation, NonDpNoiseShouldNotImpactTheResult) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  int valid_register_count = 30;
  for (int i = 1; i <= valid_register_count; ++i) {
    AddRegister(&plain_sketch, /*index =*/i, /*key=*/i, /*count=*/1);
  }

  RegisterNoiseGenerationParameters reach_noise_parameters;
  reach_noise_parameters.set_curve_id(kTestCurveId);
  reach_noise_parameters.set_total_sketches_count(kPublisherCount);
  reach_noise_parameters.set_contributors_count(kWorkerCount);
  // resulted p = 0.716531, offset = 15.
  // Random blind histogram noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(1, 1);
  // resulted p = 0.716531, offset = 10.
  // Random noise for publisher noise.
  *reach_noise_parameters.mutable_dp_params()
       ->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(1, 1);
  // resulted p ~= 0 , offset = 3.
  // Deterministic reach dp noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  *reach_noise_parameters.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();
  ASSERT_OK_AND_ASSIGN(MpcResult result,
                       test_data.GoThroughEntireMpcProtocol(
                           encrypted_sketch, &reach_noise_parameters,
                           /*frequency_noise=*/nullptr));
  int64_t expected_reach = wfa::estimation::EstimateCardinalityLiquidLegions(
      kDecayRate, kLiquidLegionsSize, valid_register_count,
      kVidSamplingIntervalWidth);

  EXPECT_EQ(result.reach, expected_reach);
}

TEST(FrequencyNoise, TotalNoiseBytesShouldBeCorrect) {
  Context ctx;
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group, ECGroup::Create(kTestCurveId, &ctx));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
                       CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId));
  ASSERT_OK_AND_ASSIGN(auto public_key_pair,
                       el_gamal_cipher->GetPublicKeyBytes());
  ASSERT_OK_AND_ASSIGN(std::string private_key,
                       el_gamal_cipher->GetPrivateKeyBytes());
  ElGamalPublicKey public_key;
  public_key.set_generator(public_key_pair.first);
  public_key.set_element(public_key_pair.second);

  FlagCountTupleNoiseGenerationParameters frequency_noise_params;
  frequency_noise_params.set_maximum_frequency(kMaxFrequency);
  frequency_noise_params.set_contributors_count(kWorkerCount);
  // resulted p = 0.606531, offset = 8
  *frequency_noise_params.mutable_dp_params() =
      MakeDifferentialPrivacyParams(1, 1);

  int computed_offset = 8;
  int64_t expected_total_noise_tuple_count =
      (kMaxFrequency + 1) * computed_offset * 2;

  CompleteExecutionPhaseTwoRequest request;
  request.set_curve_id(kTestCurveId);
  *request.mutable_local_el_gamal_key_pair()->mutable_public_key() = public_key;
  *request.mutable_local_el_gamal_key_pair()->mutable_secret_key() =
      private_key;
  *request.mutable_composite_el_gamal_public_key() = public_key;
  *request.mutable_partial_composite_el_gamal_public_key() = public_key;
  *request.mutable_noise_parameters() = frequency_noise_params;

  ASSERT_OK_AND_ASSIGN(CompleteExecutionPhaseTwoResponse Response,
                       CompleteExecutionPhaseTwo(request));
  ASSERT_THAT(
      Response.flag_count_tuples(),
      SizeIs(expected_total_noise_tuple_count * kBytesPerFlagsCountTuple));

  int frequency_dp_noise_tuples_count = 0;
  int idestroyed_frequency_noise_tuples_count = 0;
  int padding_frequency_noise_tuples_count = 0;

  for (int i = 0; i < expected_total_noise_tuple_count; ++i) {
    ASSERT_OK_AND_ASSIGN(
        bool is_frequency_dp_noise_tuples,
        IsFrequencyDpNoiseTuples(
            *el_gamal_cipher,
            Response.flag_count_tuples().substr(i * kBytesPerFlagsCountTuple,
                                                kBytesPerFlagsCountTuple)));
    ASSERT_OK_AND_ASSIGN(
        bool is_destroyed_frequency_noise_tuples,
        IsDestroyedFrequencyNoiseTuples(
            *el_gamal_cipher,
            Response.flag_count_tuples().substr(i * kBytesPerFlagsCountTuple,
                                                kBytesPerFlagsCountTuple)));
    ASSERT_OK_AND_ASSIGN(
        bool is_padding_frequency_noise_tuples,
        IsPaddingFrequencyNoiseTuples(
            *el_gamal_cipher,
            Response.flag_count_tuples().substr(i * kBytesPerFlagsCountTuple,
                                                kBytesPerFlagsCountTuple)));

    frequency_dp_noise_tuples_count += is_frequency_dp_noise_tuples;
    idestroyed_frequency_noise_tuples_count +=
        is_destroyed_frequency_noise_tuples;
    padding_frequency_noise_tuples_count += is_padding_frequency_noise_tuples;
    // Assert exact 1 boolean is true.
    EXPECT_EQ(is_frequency_dp_noise_tuples +
                  is_destroyed_frequency_noise_tuples +
                  is_padding_frequency_noise_tuples,
              1);
  }
  EXPECT_EQ(expected_total_noise_tuple_count,
            frequency_dp_noise_tuples_count +
                idestroyed_frequency_noise_tuples_count +
                padding_frequency_noise_tuples_count);
}

TEST(FrequencyNoise, AllFrequencyBucketsShouldHaveNoise) {
  Context ctx;
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group, ECGroup::Create(kTestCurveId, &ctx));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
                       CommutativeElGamal::CreateWithNewKeyPair(kTestCurveId));
  ASSERT_OK_AND_ASSIGN(auto public_key_pair,
                       el_gamal_cipher->GetPublicKeyBytes());
  ASSERT_OK_AND_ASSIGN(std::string private_key,
                       el_gamal_cipher->GetPrivateKeyBytes());
  ElGamalPublicKey public_key;
  public_key.set_generator(public_key_pair.first);
  public_key.set_element(public_key_pair.second);

  FlagCountTupleNoiseGenerationParameters frequency_noise_params;
  frequency_noise_params.set_maximum_frequency(kMaxFrequency);
  frequency_noise_params.set_contributors_count(kWorkerCount);
  // resulted p = 0.606531, offset = 13
  *frequency_noise_params.mutable_dp_params() =
      MakeDifferentialPrivacyParams(1, 0.1);

  int computed_offset = 13;
  int64_t expected_total_noise_tuple_count =
      (kMaxFrequency + 1) * computed_offset * 2;

  CompleteExecutionPhaseTwoRequest request;
  request.set_curve_id(kTestCurveId);
  *request.mutable_local_el_gamal_key_pair()->mutable_public_key() = public_key;
  *request.mutable_local_el_gamal_key_pair()->mutable_secret_key() =
      private_key;
  *request.mutable_composite_el_gamal_public_key() = public_key;
  *request.mutable_partial_composite_el_gamal_public_key() = public_key;
  *request.mutable_noise_parameters() = frequency_noise_params;

  ASSERT_OK_AND_ASSIGN(CompleteExecutionPhaseTwoResponse Response,
                       CompleteExecutionPhaseTwo(request));
  ASSERT_THAT(
      Response.flag_count_tuples(),
      SizeIs(expected_total_noise_tuple_count * kBytesPerFlagsCountTuple));

  ASSERT_OK_AND_ASSIGN(std::vector<std::string> count_values_plaintext,
                       GetCountValuesPlaintext(kMaxFrequency, kTestCurveId));

  std::array<int, kMaxFrequency> noise_count_per_bucket = {};
  for (int i = 0; i < expected_total_noise_tuple_count; ++i) {
    absl::string_view current_tuples =
        absl::string_view(Response.flag_count_tuples())
            .substr(i * kBytesPerFlagsCountTuple, kBytesPerFlagsCountTuple);
    ASSERT_OK_AND_ASSIGN(
        bool is_frequency_dp_noise_tuples,
        IsFrequencyDpNoiseTuples(*el_gamal_cipher, current_tuples));
    if (is_frequency_dp_noise_tuples) {
      ASSERT_OK_AND_ASSIGN(
          ElGamalCiphertext count,
          ExtractElGamalCiphertextFromString(
              current_tuples.substr(kBytesCipherText * 3, kBytesCipherText)));
      for (int j = 0; j < kMaxFrequency; ++j) {
        ASSERT_OK_AND_ASSIGN(
            bool is_count_j,
            IsEncryptionOfPlaintext(*el_gamal_cipher, count_values_plaintext[j],
                                    count));
        if (is_count_j) {
          ++noise_count_per_bucket[j];
          break;
        }
      }
    }
  }

  for (int i = 0; i < kMaxFrequency; ++i) {
    EXPECT_GT(noise_count_per_bucket[i], 0);
  }
}

TEST(FrequencyNoise, DeterministicNoiseShouldHaveNoImpact) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/222, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/3, /*key=*/333, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/4, /*key=*/444, /*count=*/3);
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  FlagCountTupleNoiseGenerationParameters frequency_noise_params;
  frequency_noise_params.set_maximum_frequency(kMaxFrequency);
  frequency_noise_params.set_contributors_count(kWorkerCount);
  // resulted p ~= 0, offset = 7, such that the number of frequency DP noise is
  // almost a constant, and thus the result is deterministic.
  *frequency_noise_params.mutable_dp_params() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));

  ASSERT_OK_AND_ASSIGN(
      MpcResult result,
      test_data.GoThroughEntireMpcProtocol(
          encrypted_sketch, /*reach_noise=*/nullptr, &frequency_noise_params));

  ASSERT_THAT(result.frequency_distribution, SizeIs(2));
  // All noises are compensated since the p is ~=0.
  EXPECT_EQ(result.frequency_distribution[2], 0.25);
  EXPECT_EQ(result.frequency_distribution[3], 0.75);
}

TEST(FrequencyNoise, NonDeterministicNoiseShouldRandomizeTheResult) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyLiquidLegionsSketch();
  for (int i = 1; i <= 10; ++i) {
    AddRegister(&plain_sketch, /*index=*/i, /*key=*/i, /*count=*/2);
  }
  for (int i = 1; i <= 30; ++i) {
    AddRegister(&plain_sketch, /*index=*/100 + i, /*key=*/100 + i, /*count=*/3);
  }
  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  FlagCountTupleNoiseGenerationParameters frequency_noise_params;
  // Add 30 buckets to reduce flakiness.
  frequency_noise_params.set_maximum_frequency(30);
  frequency_noise_params.set_contributors_count(kWorkerCount);
  // resulted p = 0.606531, offset = 9
  *frequency_noise_params.mutable_dp_params() =
      MakeDifferentialPrivacyParams(1, 0.5);

  ASSERT_OK_AND_ASSIGN(
      MpcResult result,
      test_data.GoThroughEntireMpcProtocol(
          encrypted_sketch, /*reach_noise=*/nullptr, &frequency_noise_params));

  // Since there are noise, there should be other entries in the histogram.
  ASSERT_THAT(result.frequency_distribution, testing::Not(SizeIs(2)));
  EXPECT_NE(result.frequency_distribution[2], 0.25);
  EXPECT_NE(result.frequency_distribution[3], 0.75);

  double total_p = 0;
  for (auto x : result.frequency_distribution) {
    total_p += x.second;
  }
  EXPECT_NEAR(total_p, 1, 0.0001);
}

TEST(EndToEnd, CombinedCasesWithDeterministicReachAndFrequencyDpNoises) {
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
  AddRegister(&plain_sketch, /*index=*/1, /*key=*/111, /*count=*/6);
  AddRegister(&plain_sketch, /*index=*/2, /*key=*/222, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/3, /*key=*/333, /*count=*/1);
  AddRegister(&plain_sketch, /*index=*/3, /*key=*/333, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/3, /*key=*/333, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/4, /*key=*/444, /*count=*/3);
  AddRegister(&plain_sketch, /*index=*/4, /*key=*/444, /*count=*/4);
  AddRegister(&plain_sketch, /*index=*/4, /*key=*/444, /*count=*/5);
  AddRegister(&plain_sketch, /*index=*/5, /*key=*/-1, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/6, /*key=*/601, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/6, /*key=*/602, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/7, /*key=*/-1, /*count=*/2);
  AddRegister(&plain_sketch, /*index=*/7, /*key=*/777, /*count=*/2);

  std::string encrypted_sketch =
      test_data.EncryptWithFlaggedKey(plain_sketch).value();

  RegisterNoiseGenerationParameters reach_noise_parameters;
  reach_noise_parameters.set_curve_id(kTestCurveId);
  reach_noise_parameters.set_total_sketches_count(3);
  reach_noise_parameters.set_contributors_count(kWorkerCount);
  // resulted p = 0.716531, offset = 15.
  // Random blind histogram noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_blind_histogram() =
      MakeDifferentialPrivacyParams(1, 1);
  // resulted p = 0.716531, offset = 10.
  // Random noise for publisher noise.
  *reach_noise_parameters.mutable_dp_params()
       ->mutable_noise_for_publisher_noise() =
      MakeDifferentialPrivacyParams(1, 1);
  // resulted p ~= 0 , offset = 3.
  // Deterministic reach dp noise.
  *reach_noise_parameters.mutable_dp_params()->mutable_global_reach_dp_noise() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));
  *reach_noise_parameters.mutable_composite_el_gamal_public_key() =
      test_data.client_el_gamal_public_key_;

  FlagCountTupleNoiseGenerationParameters frequency_noise_params;
  frequency_noise_params.set_maximum_frequency(kMaxFrequency);
  frequency_noise_params.set_contributors_count(kWorkerCount);
  // resulted p ~= 0, offset = 5, such that the number of frequency DP noise is
  // almost a constant, and thus the result is deterministic.
  *frequency_noise_params.mutable_dp_params() =
      MakeDifferentialPrivacyParams(40, std::exp(-80));

  ASSERT_OK_AND_ASSIGN(
      MpcResult result,
      test_data.GoThroughEntireMpcProtocol(
          encrypted_sketch, &reach_noise_parameters, &frequency_noise_params));

  int64_t expected_reach = wfa::estimation::EstimateCardinalityLiquidLegions(
      kDecayRate, kLiquidLegionsSize, 7, kVidSamplingIntervalWidth);

  EXPECT_EQ(result.reach, expected_reach);

  EXPECT_THAT(
      result.frequency_distribution,
      UnorderedElementsAre(Pair(3, DoubleNear(0.25, 0.001)),
                           Pair(6, DoubleNear(0.5, 0.001)),
                           Pair(kMaxFrequency, DoubleNear(0.25, 0.001))));
}

}  // namespace
}  // namespace wfa::measurement::internal::duchy::protocol::liquid_legions_v2
