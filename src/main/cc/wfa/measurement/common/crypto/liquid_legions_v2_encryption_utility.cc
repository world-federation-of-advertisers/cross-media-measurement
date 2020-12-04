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

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "src/main/cc/estimation/estimators.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
#include "wfa/measurement/common/crypto/started_thread_cpu_timer.h"
#include "wfa/measurement/common/macros.h"
#include "wfa/measurement/common/string_block_sorter.h"

namespace wfa::measurement::common::crypto {

namespace {

// Merge all the counts in each group using the SameKeyAggregation algorithm.
// The calculated (flag_1, flag_2, count) tuple is appended to the response.
// 'sub_permutation' contains the locations of the registers belonging to this
// group, i.e., having the same blinded register index.
absl::Status MergeCountsUsingSameKeyAggregation(
    absl::Span<const size_t> sub_permutation, absl::string_view registers,
    ProtocolCryptor& protocol_cryptor, std::string& response) {
  if (sub_permutation.empty()) {
    return absl::InternalError("Empty sub permutation.");
  }

  ASSIGN_OR_RETURN(ElGamalEcPointPair destroyed_key_constant_ec_pair,
                   protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
                       KDestroyedRegisterKey));

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
  // Append the (flag_a,  flag_b, count) tuple for this group of registers to
  // the final response.
  RETURN_IF_ERROR(AppendEcPointPairToString(flag_a, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(flag_b, response));
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
                                      size_t active_register_count) {
  if (liquid_legions_decay_rate <= 1.0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The decay rate should be > 1, but is ", liquid_legions_decay_rate));
  }
  if (liquid_legions_size <= active_register_count) {
    return absl::InvalidArgumentError(absl::StrCat(
        "liquid legions size (", liquid_legions_size,
        ") should be greater then the number of active registers (",
        active_register_count, ")."));
  }
  return wfa::estimation::EstimateCardinalityLiquidLegions(
      liquid_legions_decay_rate, liquid_legions_size, active_register_count);
}

absl::StatusOr<std::vector<ElGamalEcPointPair>> GetSameKeyAggregatorMatrixBase(
    ProtocolCryptor& protocol_cryptor, int64_t max_frequency) {
  if (max_frequency < 1) {
    return absl::InvalidArgumentError("max_frequency should be positive");
  }
  // Result[i] =  - encrypted_one * (i+1)
  std::vector<ElGamalEcPointPair> result;
  ASSIGN_OR_RETURN(ElGamalEcPointPair one_ec,
                   protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
                       KUnitECPointSeed));
  ASSIGN_OR_RETURN(ElGamalEcPointPair negative_one_ec,
                   InvertEcPointPair(one_ec));
  result.push_back(std::move(negative_one_ec));
  for (size_t i = 1; i < max_frequency; ++i) {
    ASSIGN_OR_RETURN(ElGamalEcPointPair next,
                     AddEcPointPairs(result.back(), result[0]));
    result.push_back(std::move(next));
  }
  return std::move(result);
}

}  // namespace

absl::StatusOr<CompleteSetupPhaseResponse> CompleteSetupPhase(
    const CompleteSetupPhaseRequest& request) {
  StartedThreadCpuTimer timer;

  CompleteSetupPhaseResponse response;
  *response.mutable_combined_register_vector() =
      request.combined_register_vector();

  if (request.has_noise_parameters()) {
    // TODO: add noise registers.
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(
      *response.mutable_combined_register_vector()));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteReachEstimationPhaseResponse>
CompleteReachEstimationPhase(
    const CompleteReachEstimationPhaseRequest& request) {
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

  CompleteReachEstimationPhaseResponse response;
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

absl::StatusOr<CompleteReachEstimationPhaseAtAggregatorResponse>
CompleteReachEstimationPhaseAtAggregator(
    const CompleteReachEstimationPhaseAtAggregatorRequest& request) {
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

  CompleteReachEstimationPhaseAtAggregatorResponse response;
  std::string* response_data = response.mutable_flag_count_tuples();
  RETURN_IF_ERROR(JoinRegistersByIndexAndMergeCounts(
      *protocol_cryptor, request.combined_register_vector(),
      blinded_register_indexes, permutation, *response_data));
  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText * 3>(*response_data));

  // Estimates reach.
  ASSIGN_OR_RETURN(size_t active_register_count,
                   GetNumberOfBlocks(response.flag_count_tuples(),
                                     kBytesPerFlagsCountTuple));
  ASSIGN_OR_RETURN(
      int64_t reach,
      EstimateReach(request.liquid_legions_parameters().decay_rate(),
                    request.liquid_legions_parameters().size(),
                    active_register_count));
  response.set_reach(reach);

  // Add noise (flag_a, flag_b, count) tuples if configured to.
  if (request.has_noise_parameters()) {
    // TODO: add noise
  }

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteFilteringPhaseResponse> CompleteFilteringPhase(
    const CompleteFilteringPhaseRequest& request) {
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

  CompleteFilteringPhaseResponse response;
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
         Action::kReRandomize},
        *response_data));
  }

  // Add noise (flag_a, flag_b, count) tuples if configured to.
  if (request.has_noise_parameters()) {
    // TODO: add noise
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerFlagsCountTuple>(*response_data));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteFilteringPhaseAtAggregatorResponse>
CompleteFilteringPhaseAtAggregator(
    const CompleteFilteringPhaseAtAggregatorRequest& request) {
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

  CompleteFilteringPhaseAtAggregatorResponse response;
  std::string* response_ska_matrix =
      response.mutable_same_key_aggregator_matrix();

  for (size_t index = 0; index < tuple_counts; ++index) {
    absl::string_view current_block =
        absl::string_view(request.flag_count_tuples())
            .substr(index * kBytesPerFlagsCountTuple, kBytesPerFlagsCountTuple);

    ASSIGN_OR_RETURN(ElGamalCiphertext flag_1_ciphertext,
                     ExtractElGamalCiphertextFromString(
                         current_block.substr(0, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(ElGamalCiphertext flag_2_ciphertext,
                     ExtractElGamalCiphertextFromString(current_block.substr(
                         kBytesPerCipherText, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(
        bool flag_1,
        protocol_cryptor->IsDecryptLocalElGamalResultZero(flag_1_ciphertext));
    ASSIGN_OR_RETURN(
        bool flag_2,
        protocol_cryptor->IsDecryptLocalElGamalResultZero(flag_2_ciphertext));

    // Add a new row to the SameKeyAggregator Matrix if the register is not
    // destroyed.
    if (flag_1 && !flag_2) {
      ASSIGN_OR_RETURN(ElGamalCiphertext current_count_ciphertext,
                       ExtractElGamalCiphertextFromString(current_block.substr(
                           kBytesPerCipherText * 2, kBytesPerCipherText)));
      ASSIGN_OR_RETURN(
          ElGamalEcPointPair current_count_ec_pair,
          protocol_cryptor->ToElGamalEcPoints(current_count_ciphertext));
      for (size_t i = 0; i < request.maximum_frequency(); ++i) {
        ASSIGN_OR_RETURN(ElGamalEcPointPair diff,
                         protocol_cryptor->CalculateDestructor(
                             ska_bases[i], current_count_ec_pair));
        RETURN_IF_ERROR(AppendEcPointPairToString(diff, *response_ska_matrix));
      }
    }
  }

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

absl::StatusOr<CompleteFrequencyEstimationPhaseResponse>
CompleteFrequencyEstimationPhase(
    const CompleteFrequencyEstimationPhaseRequest& request) {
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
          kGenerateWithNewPohligHellmanKey, kGenerateWithNewElGamalKey),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  CompleteFrequencyEstimationPhaseResponse response;

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

absl::StatusOr<CompleteFrequencyEstimationPhaseAtAggregatorResponse>
CompleteFrequencyEstimationPhaseAtAggregator(
    const CompleteFrequencyEstimationPhaseAtAggregatorRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(size_t ciphertext_counts,
                   GetNumberOfBlocks(request.same_key_aggregator_matrix(),
                                     kBytesPerCipherText));

  size_t maximum_frequency = request.maximum_frequency();

  if (maximum_frequency < 1) {
    return absl::InvalidArgumentError("maximum_frequency should be positive.");
  }
  if (ciphertext_counts % maximum_frequency != 0) {
    return absl::InvalidArgumentError(
        "The size of the SameKeyAggregator matrix is not divisible by the "
        "maximum_frequency.");
  }

  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().public_key().generator(),
              request.local_el_gamal_key_pair().public_key().element()),
          request.local_el_gamal_key_pair().secret_key(),
          kGenerateWithNewPohligHellmanKey, kGenerateWithNewElGamalKey),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  size_t total_counts = ciphertext_counts / maximum_frequency;

  // histogram[i-1] = the number of times value i (1...maximum_frequency)
  // occurs. histogram[maximum_frequency] = the number of times all values
  // greater than maximum_frequency occurs.
  std::vector<size_t> histogram(maximum_frequency + 1);
  histogram[maximum_frequency] = total_counts;

  absl::string_view same_key_aggregator_matrix =
      absl::string_view(request.same_key_aggregator_matrix());
  for (size_t row = 0; row < total_counts; ++row) {
    for (size_t column = 0; column < maximum_frequency; ++column) {
      size_t offset = (row * maximum_frequency + column) * kBytesPerCipherText;
      absl::string_view current_block =
          same_key_aggregator_matrix.substr(offset, kBytesPerCipherText);
      ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                       ExtractElGamalCiphertextFromString(current_block));
      ASSIGN_OR_RETURN(
          bool is_decryption_zero,
          protocol_cryptor->IsDecryptLocalElGamalResultZero(ciphertext));
      if (is_decryption_zero) {
        // This count is equal to column+1.
        ++histogram[column];
        --histogram[maximum_frequency];
        // No need to check other columns of this row.
        break;
      }
    }
  }

  CompleteFrequencyEstimationPhaseAtAggregatorResponse response;
  google::protobuf::Map<int64_t, double>& distribution =
      *response.mutable_frequency_distribution();
  for (size_t i = 0; i <= maximum_frequency; ++i) {
    if (histogram[i] != 0) {
      distribution[i + 1] = static_cast<double>(histogram[i]) / total_counts;
    }
  }
  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

}  // namespace wfa::measurement::common::crypto
