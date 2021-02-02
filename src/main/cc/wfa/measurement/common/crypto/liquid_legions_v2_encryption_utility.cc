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

#include "wfa/measurement/common/crypto/liquid_legions_v2_encryption_utility.h"

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "src/main/cc/estimation/estimators.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/noise_parameters_computation.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
#include "wfa/measurement/common/crypto/started_thread_cpu_timer.h"
#include "wfa/measurement/common/macros.h"
#include "wfa/measurement/common/math/distributions.h"
#include "wfa/measurement/common/string_block_sorter.h"

namespace wfa::measurement::common::crypto {

namespace {

// Merge all the counts in each group using the SameKeyAggregation algorithm.
// The calculated (flag_1, flag_2, flag_3, count) tuple is appended to the
// response. 'sub_permutation' contains the locations of the registers belonging
// to this group, i.e., having the same blinded register index.
absl::Status MergeCountsUsingSameKeyAggregation(
    absl::Span<const size_t> sub_permutation, absl::string_view registers,
    ProtocolCryptor& protocol_cryptor, std::string& response) {
  if (sub_permutation.empty()) {
    return absl::InternalError("Empty sub permutation.");
  }

  ASSIGN_OR_RETURN(ElGamalEcPointPair destroyed_key_constant_ec_pair,
                   protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
                       kDestroyedRegisterKey));
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair blinded_histogram_noise_key_constant_ec_pair,
      protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
          kBlindedHistogramNoiseRegisterKey));

  ASSIGN_OR_RETURN(
      KeyCountPairCipherText key_count_0,
      ExtractKeyCountPairFromRegisters(registers, sub_permutation[0]));

  // Initialize flag_a as 0
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair flag_a,
      protocol_cryptor.EncryptIdentityElementToEcPointsCompositeElGamal());
  ASSIGN_OR_RETURN(ElGamalEcPointPair total_count,
                   protocol_cryptor.ToElGamalEcPoints(key_count_0.count));
  // calculate the inverse of key_0. i.e., -K0
  ASSIGN_OR_RETURN(ElGamalEcPointPair key_0,
                   protocol_cryptor.ToElGamalEcPoints(key_count_0.key));
  ASSIGN_OR_RETURN(ElGamalEcPointPair key_0_inverse, InvertEcPointPair(key_0));
  // Merge all addition points to the result
  for (size_t i = 1; i < sub_permutation.size(); ++i) {
    ASSIGN_OR_RETURN(
        KeyCountPairCipherText next_key_count,
        ExtractKeyCountPairFromRegisters(registers, sub_permutation[i]));
    // Get the ECPoints of this (Key, count) pair, 2 for key, and 2 for count.
    ASSIGN_OR_RETURN(ElGamalEcPointPair count_i,
                     protocol_cryptor.ToElGamalEcPoints(next_key_count.count));
    ASSIGN_OR_RETURN(ElGamalEcPointPair key_i,
                     protocol_cryptor.ToElGamalEcPoints(next_key_count.key));
    ASSIGN_OR_RETURN(
        ElGamalEcPointPair destructor,
        protocol_cryptor.CalculateDestructor(key_0_inverse, key_i));
    // flag_a +=  destructor
    ASSIGN_OR_RETURN(flag_a, AddEcPointPairs(flag_a, destructor));
    // total_count +=  count_i
    ASSIGN_OR_RETURN(total_count, AddEcPointPairs(total_count, count_i));
  }
  // group_count = total_count + flag_a
  ASSIGN_OR_RETURN(ElGamalEcPointPair group_count,
                   AddEcPointPairs(total_count, flag_a));
  // flag_b =  r*(destroyed_key_constant + flag_a - Key[0])
  ASSIGN_OR_RETURN(ElGamalEcPointPair flag_b,
                   AddEcPointPairs(destroyed_key_constant_ec_pair, flag_a));
  ASSIGN_OR_RETURN(flag_b,
                   protocol_cryptor.CalculateDestructor(key_0_inverse, flag_b));
  // flag_c =  r*(blinded_histogram_noise_key_constant + flag_a - Key[0])
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair flag_c,
      AddEcPointPairs(blinded_histogram_noise_key_constant_ec_pair, flag_a));
  ASSIGN_OR_RETURN(flag_c,
                   protocol_cryptor.CalculateDestructor(key_0_inverse, flag_c));
  // Append the (flag_a, flag_b, flag_c, count) tuple for this group of
  // registers to the final response.
  RETURN_IF_ERROR(AppendEcPointPairToString(flag_a, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(flag_b, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(flag_c, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(group_count, response));
  return absl::OkStatus();
}

// Join registers with the same blinded register index as a group, and merge all
// the counts in each group using the SameKeyAggregation algorithm. Then, append
// the (flag, count) result of each group to the response.
absl::Status JoinRegistersByIndexAndMergeCounts(
    ProtocolCryptor& protocol_cryptor, absl::string_view registers,
    const std::vector<std::string>& blinded_register_indexes,
    absl::Span<const size_t> permutation, std::string& response) {
  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(registers, kBytesPerCipherRegister));

  if (register_count == 0) {
    return absl::OkStatus();
  }

  int start = 0;
  for (size_t i = 0; i < register_count; ++i) {
    if (blinded_register_indexes[permutation[i]] ==
        blinded_register_indexes[permutation[start]]) {
      // This register has the same index, it belongs to the same group;
      continue;
    } else {
      // This register belongs to a new group. Process the previous group and
      // append the result to the response.
      RETURN_IF_ERROR(MergeCountsUsingSameKeyAggregation(
          permutation.subspan(start, i - start), registers, protocol_cryptor,
          response));
      // Reset the starting point.
      start = i;
    }
  }
  // Process the last group and append the result to the response.
  return MergeCountsUsingSameKeyAggregation(
      permutation.subspan(start, register_count - start), registers,
      protocol_cryptor, response);
}

absl::StatusOr<int64_t> EstimateReach(double liquid_legions_decay_rate,
                                      int64_t liquid_legions_size,
                                      size_t non_empty_register_count) {
  if (liquid_legions_decay_rate <= 1.0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The decay rate should be > 1, but is ", liquid_legions_decay_rate));
  }
  if (liquid_legions_size <= non_empty_register_count) {
    return absl::InvalidArgumentError(absl::StrCat(
        "liquid legions size (", liquid_legions_size,
        ") should be greater then the number of non empty registers (",
        non_empty_register_count, ")."));
  }
  return wfa::estimation::EstimateCardinalityLiquidLegions(
      liquid_legions_decay_rate, liquid_legions_size, non_empty_register_count);
}

absl::StatusOr<std::vector<ElGamalEcPointPair>> GetSameKeyAggregatorMatrixBase(
    ProtocolCryptor& protocol_cryptor, int64_t max_frequency) {
  if (max_frequency < 2) {
    return absl::InvalidArgumentError("max_frequency should be at least 2");
  }
  // Result[i] =  - encrypted_one * (i+1)
  std::vector<ElGamalEcPointPair> result;
  ASSIGN_OR_RETURN(ElGamalEcPointPair one_ec,
                   protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
                       kUnitECPointSeed));
  ASSIGN_OR_RETURN(ElGamalEcPointPair negative_one_ec,
                   InvertEcPointPair(one_ec));
  result.push_back(std::move(negative_one_ec));
  for (size_t i = 1; i < max_frequency - 1; ++i) {
    ASSIGN_OR_RETURN(ElGamalEcPointPair next,
                     AddEcPointPairs(result.back(), result[0]));
    result.push_back(std::move(next));
  }
  return std::move(result);
}

absl::Status EncryptCompositeElGamalAndAppendToString(
    ProtocolCryptor& protocol_cryptor, absl::string_view plaintext_ec,
    std::string& data) {
  ASSIGN_OR_RETURN(ElGamalCiphertext key,
                   protocol_cryptor.EncryptCompositeElGamal(plaintext_ec));
  data.append(key.first);
  data.append(key.second);
  return absl::OkStatus();
}

// Adds encrypted blinded-histogram-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddBlindedHistogramNoise(
    ProtocolCryptor& protocol_cryptor, int total_sketches_count,
    const math::DistributedGeometricRandomComponentOptions& options,
    std::string& data) {
  ASSIGN_OR_RETURN(
      std::string blinded_histogram_noise_key_ec,
      protocol_cryptor.MapToCurve(kBlindedHistogramNoiseRegisterKey));

  int64_t noise_register_added = 0;
  for (int k = 1; k <= total_sketches_count; ++k) {
    // The random number of distinct register_ids that should appear k times.
    ASSIGN_OR_RETURN(int64_t noise_register_count_for_bucket_k,
                     math::GetDistributedGeometricRandomComponent(options));
    // Add noise_register_count_for_bucket_k such distinct register ids.
    for (int i = 0; i < noise_register_count_for_bucket_k; ++i) {
      // The prefix is to ensure the value is not in the regular id space.
      std::string register_id = absl::StrCat(
          "blinded_histogram_noise", protocol_cryptor.NextRandomBigNum());
      ASSIGN_OR_RETURN(std::string register_id_ec,
                       protocol_cryptor.MapToCurve(register_id));
      // Add k registers with the same register_id but different keys and
      // counts.
      for (int j = 0; j < k; ++j) {
        // Add register_id
        RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
            protocol_cryptor, register_id_ec, data));
        // Add register key, which is the constant blinded_histogram_noise_key.
        RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
            protocol_cryptor, blinded_histogram_noise_key_ec, data));
        // Add register count, which can be arbitrary value. use the same value
        // as key here.
        RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
            protocol_cryptor, blinded_histogram_noise_key_ec, data));
        ++noise_register_added;
      }
    }
  }
  return noise_register_added;
}

// Adds encrypted noise-for-publisher-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddNoiseForPublisherNoise(
    ProtocolCryptor& protocol_cryptor,
    const math::DistributedGeometricRandomComponentOptions& options,
    std::string& data) {
  ASSIGN_OR_RETURN(std::string publisher_noise_register_id_ec,
                   protocol_cryptor.MapToCurve(kPublisherNoiseRegisterId));

  ASSIGN_OR_RETURN(int64_t noise_registers_count,
                   math::GetDistributedGeometricRandomComponent(options));
  for (int i = 0; i < noise_registers_count; ++i) {
    // Add register id, a predefined constant.
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, publisher_noise_register_id_ec, data));
    // Add register key, a random number.
    ASSIGN_OR_RETURN(
        std::string random_key_ec,
        protocol_cryptor.MapToCurve(protocol_cryptor.NextRandomBigNum()));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, random_key_ec, data));
    // Add register count, which can be of arbitrary value. Use the same value
    // as key here.
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, random_key_ec, data));
  }
  return noise_registers_count;
}

// Adds encrypted global-reach-DP-noise registers to the end of data.
// returns the number of such noise registers added.
absl::StatusOr<int64_t> AddGlobalReachDpNoise(
    ProtocolCryptor& protocol_cryptor,
    const math::DistributedGeometricRandomComponentOptions& options,
    std::string& data) {
  ASSIGN_OR_RETURN(std::string destroyed_register_key_ec,
                   protocol_cryptor.MapToCurve(kDestroyedRegisterKey));

  ASSIGN_OR_RETURN(int64_t noise_registers_count,
                   math::GetDistributedGeometricRandomComponent(options));
  for (int i = 0; i < noise_registers_count; ++i) {
    // Add register id, a random number.
    // The prefix is to ensure the value is not in the regular id space.
    std::string register_id =
        absl::StrCat("reach_dp_noise", protocol_cryptor.NextRandomBigNum());
    ASSIGN_OR_RETURN(std::string register_id_ec,
                     protocol_cryptor.MapToCurve(register_id));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, register_id_ec, data));
    // Add register key, a predefined constant denoting destroyed registers.
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, destroyed_register_key_ec, data));
    // Add register count, which can be of arbitrary value. use the same value
    // as key here.
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, destroyed_register_key_ec, data));
  }
  return noise_registers_count;
}

// Adds encrypted padding-noise registers to the end of data.
absl::Status AddPaddingReachNoise(ProtocolCryptor& protocol_cryptor,
                                  int64_t count, std::string& data) {
  if (count < 0) {
    return absl::InvalidArgumentError("Count should >= 0.");
  }

  ASSIGN_OR_RETURN(std::string padding_noise_register_id_ec,
                   protocol_cryptor.MapToCurve(kPaddingNoiseRegisterId));
  for (int64_t i = 0; i < count; ++i) {
    // Add register_id, a predefined constant
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, padding_noise_register_id_ec, data));
    ASSIGN_OR_RETURN(
        std::string random_key_ec,
        protocol_cryptor.MapToCurve(protocol_cryptor.NextRandomBigNum()));
    // Add register key, random number
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, random_key_ec, data));
    // Add register count, which can be arbitrary value. use the same value
    // as key here.
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        protocol_cryptor, random_key_ec, data));
  }
  return absl::OkStatus();
}

// Adds 4 tuples with flag values equal to (0,R_1,R_2) to the end of data for
// each count value in [1, maximum_frequency], where R_i are random numbers.
absl::StatusOr<int64_t> AddFrequencyDpNoise(
    ProtocolCryptor& full_protocol_cryptor,
    ProtocolCryptor& partial_protocol_cryptor, int maximum_frequency,
    int curve_id,
    const math::DistributedGeometricRandomComponentOptions& options,
    std::string& data) {
  ASSIGN_OR_RETURN(std::vector<std::string> count_values_plaintext,
                   GetCountValuesPlaintext(maximum_frequency, curve_id));
  int total_noise_tuples_added = 0;
  for (int frequency = 1; frequency <= maximum_frequency; ++frequency) {
    ASSIGN_OR_RETURN(int64_t noise_tuples_count,
                     math::GetDistributedGeometricRandomComponent(options));
    for (int i = 0; i < noise_tuples_count; ++i) {
      // Adds flag_1, which is 0 encrypted by the partial_protocol_cryptor.
      ASSIGN_OR_RETURN(ElGamalEcPointPair zero,
                       partial_protocol_cryptor
                           .EncryptIdentityElementToEcPointsCompositeElGamal());
      RETURN_IF_ERROR(AppendEcPointPairToString(zero, data));
      // Adds flag_2 and flag_3, which are random numbers encrypted by the
      // partial_protocol_cryptor.
      for (int j = 0; j < 2; ++j) {
        ASSIGN_OR_RETURN(std::string random_values,
                         partial_protocol_cryptor.MapToCurve(
                             partial_protocol_cryptor.NextRandomBigNum()));
        RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
            partial_protocol_cryptor, random_values, data));
      }
      // Adds the count value.
      ASSIGN_OR_RETURN(ElGamalCiphertext count,
                       full_protocol_cryptor.EncryptCompositeElGamal(
                           count_values_plaintext[frequency - 1]));
      data.append(count.first);
      data.append(count.second);
    }
    total_noise_tuples_added += noise_tuples_count;
  }
  return total_noise_tuples_added;
}

// Adds 4 tuples with flag values equal to (R1,R2,R3) and count value equal to
// R4 to the end of data, where Ri are random numbers.
absl::StatusOr<int64_t> AddDestroyedFrequencyNoise(
    ProtocolCryptor& full_protocol_cryptor,
    ProtocolCryptor& partial_protocol_cryptor,
    const math::DistributedGeometricRandomComponentOptions& options,
    std::string& data) {
  ASSIGN_OR_RETURN(int64_t noise_tuples_count,
                   math::GetDistributedGeometricRandomComponent(options));
  for (int i = 0; i < noise_tuples_count; ++i) {
    for (int j = 0; j < 3; ++j) {
      // Add three random flags encrypted using the partial_protocol_cryptor.
      ASSIGN_OR_RETURN(std::string random_values,
                       partial_protocol_cryptor.MapToCurve(
                           partial_protocol_cryptor.NextRandomBigNum()));
      RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
          partial_protocol_cryptor, random_values, data));
    }
    // Add a random count encrypted using the full_protocol_cryptor.
    ASSIGN_OR_RETURN(std::string random_values,
                     full_protocol_cryptor.MapToCurve(
                         full_protocol_cryptor.NextRandomBigNum()));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        full_protocol_cryptor, random_values, data));
  }
  return noise_tuples_count;
}

// Adds 4 tuples with flag values equal to (0,0,R1) and count value equal to R2
// to the end of data, where Ri are random numbers.
absl::Status AddPaddingFrequencyNoise(ProtocolCryptor& full_protocol_cryptor,
                                      ProtocolCryptor& partial_protocol_cryptor,
                                      int noise_tuples_count,
                                      std::string& data) {
  if (noise_tuples_count < 0) {
    return absl::InvalidArgumentError("Count should >= 0.");
  }
  for (int i = 0; i < noise_tuples_count; ++i) {
    // Adds flag_1 and flag_2, which are 0 encrypted by the
    // partial_protocol_cryptor.
    for (int j = 0; j < 2; ++j) {
      ASSIGN_OR_RETURN(ElGamalEcPointPair zero,
                       partial_protocol_cryptor
                           .EncryptIdentityElementToEcPointsCompositeElGamal());
      RETURN_IF_ERROR(AppendEcPointPairToString(zero, data));
    }
    // Adds flag_3 and count, which are random numbers encrypted by the
    // partial_protocol_cryptor and full_protocol_cryptor respectively. We use a
    // same random number here since the count value is not used and can be of
    // arbitrary value.
    ASSIGN_OR_RETURN(std::string random_values,
                     partial_protocol_cryptor.MapToCurve(
                         partial_protocol_cryptor.NextRandomBigNum()));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        partial_protocol_cryptor, random_values, data));
    RETURN_IF_ERROR(EncryptCompositeElGamalAndAppendToString(
        full_protocol_cryptor, random_values, data));
  }
  return absl::OkStatus();
}

absl::Status ValidateSetupNoiseParameters(
    const RegisterNoiseGenerationParameters& parameters) {
  if (parameters.contributors_count() < 1) {
    return absl::InvalidArgumentError("contributors_count should be positive.");
  }
  if (parameters.total_sketches_count() < 1) {
    return absl::InvalidArgumentError(
        "total_sketches_count should be positive.");
  }
  if (parameters.dp_params().blind_histogram().epsilon() <= 0 ||
      parameters.dp_params().blind_histogram().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid blind_histogram dp parameter. epsilon/delta should be "
        "positive.");
  }
  if (parameters.dp_params().noise_for_publisher_noise().epsilon() <= 0 ||
      parameters.dp_params().noise_for_publisher_noise().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid noise_for_publisher_noise dp parameter. epsilon/delta should "
        "be positive.");
  }
  if (parameters.dp_params().global_reach_dp_noise().epsilon() <= 0 ||
      parameters.dp_params().global_reach_dp_noise().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid global_reach_dp_noise dp parameter. epsilon/delta should be "
        "positive.");
  }
  return absl::OkStatus();
}

absl::Status ValidateFrequencyNoiseParameters(
    const FlagCountTupleNoiseGenerationParameters& parameters) {
  if (parameters.contributors_count() < 1) {
    return absl::InvalidArgumentError("contributors_count should be positive.");
  }
  if (parameters.maximum_frequency() < 2) {
    return absl::InvalidArgumentError(
        "maximum_frequency should be at least 2.");
  }
  if (parameters.dp_params().epsilon() <= 0 ||
      parameters.dp_params().delta() <= 0) {
    return absl::InvalidArgumentError(
        "Invalid frequency noise dp parameter. epsilon/delta should be "
        "positive.");
  }
  return absl::OkStatus();
}

absl::Status AddAllFrequencyNoise(
    ProtocolCryptor& full_protocol_cryptor,
    ProtocolCryptor& partial_protocol_cryptor, int curve_id,
    const FlagCountTupleNoiseGenerationParameters& noise_parameters,
    std::string& data) {
  RETURN_IF_ERROR(ValidateFrequencyNoiseParameters(noise_parameters));

  auto options = GetFrequencyNoiseOptions(
      noise_parameters.dp_params(), noise_parameters.maximum_frequency(),
      noise_parameters.contributors_count());
  int64_t total_noise_tuples_count =
      options.shift_offset * 2 * (noise_parameters.maximum_frequency() + 1);
  ASSIGN_OR_RETURN(
      int frequency_dp_noise_tuples_count,
      AddFrequencyDpNoise(full_protocol_cryptor, partial_protocol_cryptor,
                          noise_parameters.maximum_frequency(), curve_id,
                          options, data));
  ASSIGN_OR_RETURN(
      int destroyed_noise_tuples_count,
      AddDestroyedFrequencyNoise(full_protocol_cryptor,
                                 partial_protocol_cryptor, options, data));
  int64_t padding_noise_tuples_count = total_noise_tuples_count -
                                       frequency_dp_noise_tuples_count -
                                       destroyed_noise_tuples_count;
  RETURN_IF_ERROR(AddPaddingFrequencyNoise(full_protocol_cryptor,
                                           partial_protocol_cryptor,
                                           padding_noise_tuples_count, data));
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<CompleteSetupPhaseResponse> CompleteSetupPhase(
    const CompleteSetupPhaseRequest& request) {
  StartedThreadCpuTimer timer;

  CompleteSetupPhaseResponse response;
  std::string* response_crv = response.mutable_combined_register_vector();
  *response_crv = request.combined_register_vector();

  if (request.has_noise_parameters()) {
    const RegisterNoiseGenerationParameters& noise_parameters =
        request.noise_parameters();
    auto blind_histogram_noise_options = GetBlindHistogramNoiseOptions(
        noise_parameters.dp_params().blind_histogram(),
        noise_parameters.total_sketches_count(),
        noise_parameters.contributors_count());
    auto noise_for_publisher_noise_options = GetNoiseForPublisherNoiseOptions(
        noise_parameters.dp_params().noise_for_publisher_noise(),
        noise_parameters.total_sketches_count(),
        noise_parameters.contributors_count());
    auto global_reach_dp_noise_options = GetGlobalReachDpNoiseOptions(
        noise_parameters.dp_params().global_reach_dp_noise(),
        noise_parameters.contributors_count());
    int64_t total_noise_registers_count =
        noise_for_publisher_noise_options.shift_offset * 2 +
        global_reach_dp_noise_options.shift_offset * 2 +
        blind_histogram_noise_options.shift_offset *
            noise_parameters.total_sketches_count() *
            (noise_parameters.total_sketches_count() + 1);

    // reserve the space to hold all output data.
    response_crv->reserve(request.combined_register_vector().size() +
                          total_noise_registers_count *
                              kBytesPerCipherRegister);

    RETURN_IF_ERROR(ValidateSetupNoiseParameters(noise_parameters));
    ASSIGN_OR_RETURN_ERROR(
        auto protocol_cryptor,
        CreateProtocolCryptorWithKeys(
            noise_parameters.curve_id(), kGenerateWithNewElGamalPublicKey,
            kGenerateWithNewElGamalPrivateKey, kGenerateWithNewPohligHellmanKey,
            std::make_pair(
                noise_parameters.composite_el_gamal_public_key().generator(),
                noise_parameters.composite_el_gamal_public_key().element())),
        "Failed to create the protocol cipher, invalid curveId or keys.");

    // 1. Add blinded histogram noise.
    ASSIGN_OR_RETURN(
        int64_t blinded_histogram_noise_count,
        AddBlindedHistogramNoise(*protocol_cryptor,
                                 noise_parameters.total_sketches_count(),
                                 blind_histogram_noise_options, *response_crv));
    // 2. Add noise for publisher noise.
    ASSIGN_OR_RETURN(int64_t publisher_noise_count,
                     AddNoiseForPublisherNoise(
                         *protocol_cryptor, noise_for_publisher_noise_options,
                         *response_crv));
    // 3. Add reach DP noise.
    ASSIGN_OR_RETURN(
        int64_t reach_dp_noise_count,
        AddGlobalReachDpNoise(*protocol_cryptor, global_reach_dp_noise_options,
                              *response_crv));
    // 4. Add padding noise.
    int64_t padding_noise_count = total_noise_registers_count -
                                  blinded_histogram_noise_count -
                                  publisher_noise_count - reach_dp_noise_count;
    RETURN_IF_ERROR(AddPaddingReachNoise(*protocol_cryptor, padding_noise_count,
                                         *response_crv));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(
      *response.mutable_combined_register_vector()));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseOneResponse> CompleteExecutionPhaseOne(
    const CompleteExecutionPhaseOneRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(request.combined_register_vector(),
                                     kBytesPerCipherRegister));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey,
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  CompleteExecutionPhaseOneResponse response;
  std::string* response_crv = response.mutable_combined_register_vector();
  // The output crv is the same size with the input crv.
  response_crv->reserve(request.combined_register_vector().size());

  for (size_t index_i = 0; index_i < register_count; ++index_i) {
    absl::string_view current_block =
        absl::string_view(request.combined_register_vector())
            .substr(index_i * kBytesPerCipherRegister, kBytesPerCipherRegister);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block,
        {Action::kBlind, Action::kReRandomize, Action::kReRandomize},
        *response_crv));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(*response_crv));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseOneAtAggregatorResponse>
CompleteExecutionPhaseOneAtAggregator(
    const CompleteExecutionPhaseOneAtAggregatorRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(request.combined_register_vector(),
                                     kBytesPerCipherRegister));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey,
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  ASSIGN_OR_RETURN(std::vector<std::string> blinded_register_indexes,
                   GetBlindedRegisterIndexes(request.combined_register_vector(),
                                             *protocol_cryptor));

  // Create a sorting permutation of the blinded register indexes, such that we
  // don't need to modify the sketch data, whose size could be huge. We only
  // need a way to point to registers with a same index.
  std::vector<size_t> permutation(register_count);
  absl::c_iota(permutation, 0);
  absl::c_sort(permutation, [&](size_t a, size_t b) {
    return blinded_register_indexes[a] < blinded_register_indexes[b];
  });

  CompleteExecutionPhaseOneAtAggregatorResponse response;
  std::string* response_data = response.mutable_flag_count_tuples();
  RETURN_IF_ERROR(JoinRegistersByIndexAndMergeCounts(
      *protocol_cryptor, request.combined_register_vector(),
      blinded_register_indexes, permutation, *response_data));

  // Add noise (flag_a, flag_b, flag_c, count) tuples if configured to.
  if (request.has_noise_parameters()) {
    RETURN_IF_ERROR(AddAllFrequencyNoise(
        *protocol_cryptor, *protocol_cryptor, request.curve_id(),
        request.noise_parameters(), *response_data));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerFlagsCountTuple>(*response_data));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseTwoResponse> CompleteExecutionPhaseTwo(
    const CompleteExecutionPhaseTwoRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t tuple_counts,
      GetNumberOfBlocks(request.flag_count_tuples(), kBytesPerFlagsCountTuple));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey,
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  CompleteExecutionPhaseTwoResponse response;
  std::string* response_data = response.mutable_flag_count_tuples();
  // Without noise, the output flag_count_tuples is the same size as the input
  // flag_count_tuples.
  response_data->reserve(request.flag_count_tuples().size());
  for (size_t index = 0; index < tuple_counts; ++index) {
    absl::string_view current_block =
        absl::string_view(request.flag_count_tuples())
            .substr(index * kBytesPerFlagsCountTuple, kBytesPerFlagsCountTuple);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block,
        {Action::kPartialDecrypt, Action::kPartialDecrypt,
         Action::kPartialDecrypt, Action::kReRandomize},
        *response_data));
  }

  // Add noise (flag_a, flag_b, flag_c, count) tuples if configured to.
  if (request.has_noise_parameters()) {
    ASSIGN_OR_RETURN_ERROR(
        auto partial_protocol_cryptor,
        CreateProtocolCryptorWithKeys(
            request.curve_id(), kGenerateWithNewElGamalPublicKey,
            kGenerateWithNewElGamalPrivateKey, kGenerateWithNewPohligHellmanKey,
            std::make_pair(
                request.partial_composite_el_gamal_public_key().generator(),
                request.partial_composite_el_gamal_public_key().element())),
        "Failed to create the protocol cipher, invalid curveId or keys.");
    RETURN_IF_ERROR(AddAllFrequencyNoise(
        *protocol_cryptor, *partial_protocol_cryptor, request.curve_id(),
        request.noise_parameters(), *response_data));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerFlagsCountTuple>(*response_data));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseTwoAtAggregatorResponse>
CompleteExecutionPhaseTwoAtAggregator(
    const CompleteExecutionPhaseTwoAtAggregatorRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t tuple_counts,
      GetNumberOfBlocks(request.flag_count_tuples(), kBytesPerFlagsCountTuple));

  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey,
          std::make_pair(request.composite_el_gamal_public_key().generator(),
                         request.composite_el_gamal_public_key().element())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  ASSIGN_OR_RETURN(std::vector<ElGamalEcPointPair> ska_bases,
                   GetSameKeyAggregatorMatrixBase(*protocol_cryptor,
                                                  request.maximum_frequency()));

  CompleteExecutionPhaseTwoAtAggregatorResponse response;
  std::string* response_ska_matrix =
      response.mutable_same_key_aggregator_matrix();

  int64_t blinded_histogram_noise_count = 0;

  for (size_t index = 0; index < tuple_counts; ++index) {
    absl::string_view current_block =
        absl::string_view(request.flag_count_tuples())
            .substr(index * kBytesPerFlagsCountTuple, kBytesPerFlagsCountTuple);
    std::array<bool, 3> flags = {};
    for (int i = 0; i < 3; ++i) {
      ASSIGN_OR_RETURN(ElGamalCiphertext flag_ciphertext,
                       ExtractElGamalCiphertextFromString(current_block.substr(
                           i * kBytesPerCipherText, kBytesPerCipherText)));
      ASSIGN_OR_RETURN(
          flags[i],
          protocol_cryptor->IsDecryptLocalElGamalResultZero(flag_ciphertext));
    }
    // Add a new row to the SameKeyAggregator Matrix if the register is not
    // destroyed or blinded histogram noise.
    if (flags[0] && !flags[1] && !flags[2]) {
      ASSIGN_OR_RETURN(ElGamalCiphertext current_count_ciphertext,
                       ExtractElGamalCiphertextFromString(current_block.substr(
                           kBytesPerCipherText * 3, kBytesPerCipherText)));
      ASSIGN_OR_RETURN(
          ElGamalEcPointPair current_count_ec_pair,
          protocol_cryptor->ToElGamalEcPoints(current_count_ciphertext));
      for (int i = 0; i < request.maximum_frequency() - 1; ++i) {
        ASSIGN_OR_RETURN(ElGamalEcPointPair diff,
                         protocol_cryptor->CalculateDestructor(
                             ska_bases[i], current_count_ec_pair));
        RETURN_IF_ERROR(AppendEcPointPairToString(diff, *response_ska_matrix));
      }
    }
    if (flags[2]) {
      ++blinded_histogram_noise_count;
    }
  }

  // Estimates reach.
  int64_t non_empty_register_count =
      tuple_counts - blinded_histogram_noise_count;
  if (request.has_reach_dp_noise_baseline()) {
    auto options = GetGlobalReachDpNoiseOptions(
        request.reach_dp_noise_baseline().global_reach_dp_noise(),
        request.reach_dp_noise_baseline().contributors_count());
    int64_t global_reach_dp_noise_baseline = options.shift_offset * options.num;
    non_empty_register_count -= global_reach_dp_noise_baseline;
    // Publisher noise and padding noise each contribute 1 additional destroyed
    // register, which shouldn't be included when estimating reach.
    // Subtracts 2 from the non_empty_register_count if noises exist.
    non_empty_register_count -= 2;
  }
  if (request.has_frequency_noise_parameters()) {
    const FlagCountTupleNoiseGenerationParameters& noise_parameters =
        request.frequency_noise_parameters();
    auto options = GetFrequencyNoiseOptions(
        noise_parameters.dp_params(), noise_parameters.maximum_frequency(),
        noise_parameters.contributors_count());
    int64_t total_noise_tuples_count =
        options.num * options.shift_offset * 2 *
        (noise_parameters.maximum_frequency() + 1);
    // Subtract all frequency noises before estimating reach.
    non_empty_register_count -= total_noise_tuples_count;
  }

  // Ensures that non_empty_register_count is at least 0.
  // non_empty_register_count could be negative if there is too few registers in
  // the sketch and the number of noise registers is smaller than the baseline.
  non_empty_register_count = std::max(non_empty_register_count, 0L);
  ASSIGN_OR_RETURN(
      int64_t reach,
      EstimateReach(request.liquid_legions_parameters().decay_rate(),
                    request.liquid_legions_parameters().size(),
                    non_empty_register_count));
  response.set_reach(reach);

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseThreeResponse> CompleteExecutionPhaseThree(
    const CompleteExecutionPhaseThreeRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t ciphertext_counts,
                   GetNumberOfBlocks(request.same_key_aggregator_matrix(),
                                     kBytesPerCipherText));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey, kGenerateWithNewElGamalPublicKey),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  CompleteExecutionPhaseThreeResponse response;
  // The SKA matrix has the same size in the response as in the request.
  response.mutable_same_key_aggregator_matrix()->reserve(
      request.same_key_aggregator_matrix().size());
  for (size_t index = 0; index < ciphertext_counts; ++index) {
    absl::string_view current_block =
        absl::string_view(request.same_key_aggregator_matrix())
            .substr(index * kBytesPerCipherText, kBytesPerCipherText);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block, {kPartialDecrypt},
        *response.mutable_same_key_aggregator_matrix()));
  }

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteExecutionPhaseThreeAtAggregatorResponse>
CompleteExecutionPhaseThreeAtAggregator(
    const CompleteExecutionPhaseThreeAtAggregatorRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t ciphertext_counts,
                   GetNumberOfBlocks(request.same_key_aggregator_matrix(),
                                     kBytesPerCipherText));

  int maximum_frequency = request.maximum_frequency();
  if (maximum_frequency < 2) {
    return absl::InvalidArgumentError(
        "maximum_frequency should be at least 2.");
  }

  int64_t row_size = maximum_frequency - 1;
  if (ciphertext_counts % row_size != 0) {
    return absl::InvalidArgumentError(
        "The size of the SameKeyAggregator matrix is not divisible by "
        "maximum_frequency-1.");
  }
  int64_t column_size = ciphertext_counts / row_size;

  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey, kGenerateWithNewElGamalPublicKey),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  // histogram[i-1] = the number of times value i (1...maximum_frequency-1)
  // occurs. histogram[maximum_frequency-1] = the number of times all values
  // greater than maximum_frequency-1 occurs.
  std::vector<int> histogram(maximum_frequency);
  histogram[maximum_frequency - 1] = column_size;

  auto same_key_aggregator_matrix =
      absl::string_view(request.same_key_aggregator_matrix());
  for (int column = 0; column < column_size; ++column) {
    for (int row = 0; row < row_size; ++row) {
      size_t offset = (column * row_size + row) * kBytesPerCipherText;
      absl::string_view current_block =
          same_key_aggregator_matrix.substr(offset, kBytesPerCipherText);
      ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                       ExtractElGamalCiphertextFromString(current_block));
      ASSIGN_OR_RETURN(
          bool is_decryption_zero,
          protocol_cryptor->IsDecryptLocalElGamalResultZero(ciphertext));
      if (is_decryption_zero) {
        // This count is equal to row+1.
        ++histogram[row];
        --histogram[maximum_frequency - 1];
        // No need to check other rows of this column.
        break;
      }
    }
  }

  int actual_total = column_size;
  // Adjusts the histogram according the noise baseline.
  if (request.has_global_frequency_dp_noise_per_bucket()) {
    auto options = GetFrequencyNoiseOptions(
        request.global_frequency_dp_noise_per_bucket().dp_params(),
        request.maximum_frequency(),
        request.global_frequency_dp_noise_per_bucket().contributors_count());
    int64_t noise_baseline_per_bucket = options.shift_offset * options.num;
    actual_total = 0;
    for (int i = 0; i < maximum_frequency; ++i) {
      histogram[i] = std::max(0L, histogram[i] - noise_baseline_per_bucket);
      actual_total += histogram[i];
    }
  }

  if (actual_total == 0) {
    return absl::InvalidArgumentError(
        "There is neither actual data nor effective noise in the request.");
  }

  CompleteExecutionPhaseThreeAtAggregatorResponse response;
  google::protobuf::Map<int64_t, double>& distribution =
      *response.mutable_frequency_distribution();
  for (int i = 0; i < maximum_frequency; ++i) {
    if (histogram[i] != 0) {
      distribution[i + 1] = static_cast<double>(histogram[i]) / actual_total;
    }
  }
  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::common::crypto
