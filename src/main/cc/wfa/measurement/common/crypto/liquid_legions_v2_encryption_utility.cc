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
#include "absl/types/span.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
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

  ASSIGN_OR_RETURN(
      ElGamalCiphertext flag_zero_base_ciphertext,
      protocol_cryptor.EncryptPlaintextCompositeElGamal(kFlagZeroBase));
  ASSIGN_OR_RETURN(
      ElGamalCiphertext destroyed_key_constant_ciphertext,
      protocol_cryptor.EncryptPlaintextCompositeElGamal(KDestroyedRegisterKey));
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair destroyed_key_constant_ec_pair,
      protocol_cryptor.ToElGamalEcPoints(destroyed_key_constant_ciphertext));

  ASSIGN_OR_RETURN(
      KeyCountPairCipherText key_count_0,
      ExtractKeyCountPairFromRegisters(registers, sub_permutation[0]));
  // Initialize flag_a, flag_b and group_count, which together make a 3-tuple
  // for this register group.
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair flag_a,
      protocol_cryptor.ToElGamalEcPoints(flag_zero_base_ciphertext));
  ASSIGN_OR_RETURN(ElGamalEcPointPair group_count,
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
    // group_count +=  count_i + destructor
    ASSIGN_OR_RETURN(group_count, AddEcPointPairs(group_count, count_i));
    ASSIGN_OR_RETURN(group_count, AddEcPointPairs(group_count, destructor));
  }

  // flag_b = r*(destroyed_key_constant + flag_a - Key[0])
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

}  // namespace

absl::StatusOr<CompleteReachEstimationPhaseResponse>
CompleteReachEstimationPhase(
    const CompleteReachEstimationPhaseRequest& request) {
  absl::Duration startCpuDuration = GetCurrentThreadCpuDuration();

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
          /*local_pohlig_hellman_private_key=*/"",
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

  absl::Duration elaspedDuration =
      GetCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
}

absl::StatusOr<CompleteReachEstimationPhaseAtAggregatorResponse>
CompleteReachEstimationPhaseAtAggregator(
    const CompleteReachEstimationPhaseAtAggregatorRequest& request) {
  absl::Duration startCpuDuration = GetCurrentThreadCpuDuration();

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

  absl::Duration elaspedDuration =
      GetCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
}

}  // namespace wfa::measurement::common::crypto
