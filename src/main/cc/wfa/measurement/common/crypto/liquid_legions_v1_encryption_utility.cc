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

#include "wfa/measurement/common/crypto/liquid_legions_v1_encryption_utility.h"

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/types/span.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfa/measurement/common/crypto/liquid_legions_v1_encryption_methods.pb.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
#include "wfa/measurement/common/crypto/started_thread_cpu_timer.h"
#include "wfa/measurement/common/macros.h"
#include "wfa/measurement/common/string_block_sorter.h"

namespace wfa {
namespace measurement {
namespace common {
namespace crypto {
namespace {

using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ElGamalCiphertext = std::pair<std::string, std::string>;
using FlagCount = DecryptLastLayerFlagAndCountResponse::FlagCount;

ElGamalCiphertext GetPublicKeyStringPair(const ElGamalPublicKey& public_keys) {
  return std::make_pair(public_keys.generator(), public_keys.element());
}

// Create a lookup table mapping the ECPoint strings to real world frequency
// integers.
// During encryption, the way we map frequency to ECPoint is that:
//   1. Pre-determine a seed, i.e., kUnitECPointSeed,  for integer 1.
//   2. Map the seed onto the Elliptical curve. The obtained ECPoint will be the
//      the unit integer, denoted as EC_1.
//   3. For EC_n, it is obtained via calculateing EC_1.mul(n) on the curve.
// This method generates the reverse lookup table for decryption.
absl::StatusOr<absl::flat_hash_map<std::string, int>> CreateCountLookUpTable(
    const int curve_id, const int maximum_frequency) {
  absl::flat_hash_map<std::string, int> count_lookup_table;
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(auto ec_group, ECGroup::Create(curve_id, ctx.get()));

  // The ECPoint corresponding to count value 1 is defined by the mapping of
  // kUnitECPointSeed;
  ASSIGN_OR_RETURN(ECPoint count_1_ec,
                   ec_group.GetPointByHashingToCurveSha256(kUnitECPointSeed));
  ASSIGN_OR_RETURN(std::string count_1, count_1_ec.ToBytesCompressed());
  count_lookup_table[count_1] = 1;

  // All other values are derived from count_1, i.e., count_n = count_1 * n.
  for (int i = 2; i < maximum_frequency; ++i) {
    ASSIGN_OR_RETURN(ECPoint count_i_ec, count_1_ec.Mul(ctx->CreateBigNum(i)));
    ASSIGN_OR_RETURN(std::string count_i, count_i_ec.ToBytesCompressed());
    count_lookup_table[count_i] = i;
  }
  return count_lookup_table;
}

// TODO: delete this method after DecryptLastLayerFlagAndCount() uses the
// ProtocolCryptor.
absl::StatusOr<std::string> GetIsNotDestroyedFlag(const int curve_id) {
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(auto ec_group, ECGroup::Create(curve_id, ctx.get()));
  ASSIGN_OR_RETURN(ECPoint temp_ec_point,
                   ec_group.GetPointByHashingToCurveSha256(kFlagZeroBase));
  return temp_ec_point.ToBytesCompressed();
}

// Merge all the counts in each group using the SameKeyAggregation algorithm.
// The calculated (flag, count) is appended to the response.
// 'sub_permutation' contains the locations of the registers belonging to this
// group, i.e., having the same blinded register index.
absl::Status MergeCountsUsingSameKeyAggregation(
    absl::Span<const size_t> sub_permutation, absl::string_view sketch,
    ProtocolCryptor& protocol_cryptor, std::string& response) {
  if (sub_permutation.empty()) {
    return absl::InternalError("Empty sub permutation.");
  }
  ASSIGN_OR_RETURN(
      KeyCountPairCipherText key_count_0,
      ExtractKeyCountPairFromRegisters(sketch, sub_permutation[0]));
  // Initialize the flag and count.
  ASSIGN_OR_RETURN(ElGamalEcPointPair tuple_flag,
                   protocol_cryptor.EncryptPlaintextToEcPointsCompositeElGamal(
                       kFlagZeroBase));
  ASSIGN_OR_RETURN(ElGamalEcPointPair tuple_count,
                   protocol_cryptor.ToElGamalEcPoints(key_count_0.count));

  // Aggregate other (key, count) pairs if any.
  if (sub_permutation.size() > 1) {
    // calculate the inverse of key_0. i.e., -K0
    ASSIGN_OR_RETURN(ElGamalEcPointPair key_0,
                     protocol_cryptor.ToElGamalEcPoints(key_count_0.key));
    ASSIGN_OR_RETURN(ElGamalEcPointPair key_0_inverse,
                     InvertEcPointPair(key_0));
    // Merge all addition points to the result
    for (size_t i = 1; i < sub_permutation.size(); ++i) {
      ASSIGN_OR_RETURN(
          KeyCountPairCipherText next_key_count,
          ExtractKeyCountPairFromRegisters(sketch, sub_permutation[i]));
      // Get the ECPoints of this (Key, count) pair, 2 for key, and 2 for count.
      ASSIGN_OR_RETURN(
          ElGamalEcPointPair count_i,
          protocol_cryptor.ToElGamalEcPoints(next_key_count.count));
      ASSIGN_OR_RETURN(ElGamalEcPointPair key_i,
                       protocol_cryptor.ToElGamalEcPoints(next_key_count.key));
      ASSIGN_OR_RETURN(
          ElGamalEcPointPair destructor,
          protocol_cryptor.CalculateDestructor(key_0_inverse, key_i));
      // Update the (flag, count) using the destructor.
      ASSIGN_OR_RETURN(tuple_flag, AddEcPointPairs(tuple_flag, destructor));
      ASSIGN_OR_RETURN(tuple_count, AddEcPointPairs(tuple_count, count_i));
      ASSIGN_OR_RETURN(tuple_count, AddEcPointPairs(tuple_count, destructor));
    }
  }
  // Append the (flag, count) result for this group of registers to the final
  // response.
  RETURN_IF_ERROR(AppendEcPointPairToString(tuple_flag, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(tuple_count, response));
  return absl::OkStatus();
}

// Join registers with the same blinded register index as a group, and merge all
// the counts in each group using the SameKeyAggregation algorithm. Then, append
// the (flag, count) result of each group to the response.
absl::Status JoinRegistersByIndexAndMergeCounts(
    ProtocolCryptor& protocol_cryptor, absl::string_view sketch,
    const std::vector<std::string>& blinded_register_indexes,
    absl::Span<const size_t> permutation, std::string& response) {
  const size_t num_registers = sketch.size() / kBytesPerCipherRegister;
  if (num_registers == 0) {
    return absl::OkStatus();
  }

  int start = 0;
  for (size_t i = 0; i < num_registers; ++i) {
    if (blinded_register_indexes[permutation[i]] ==
        blinded_register_indexes[permutation[start]]) {
      // This register has the same index, it belongs to the same group;
      continue;
    } else {
      // This register belongs to a new group. Process the previous group and
      // append the result to the response.
      RETURN_IF_ERROR(MergeCountsUsingSameKeyAggregation(
          permutation.subspan(start, i - start), sketch, protocol_cryptor,
          response));
      // Reset the starting point.
      start = i;
    }
  }
  // Process the last group and append the result to the response.
  RETURN_IF_ERROR(MergeCountsUsingSameKeyAggregation(
      permutation.subspan(start, num_registers - start), sketch,
      protocol_cryptor, response));
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t register_count,
      GetNumberOfBlocks(request.sketch(), kBytesPerCipherRegister));

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

  BlindOneLayerRegisterIndexResponse response;
  std::string* response_sketch = response.mutable_sketch();
  // The output sketch is the same size with the input sketch.
  response_sketch->reserve(request.sketch().size());
  for (size_t index = 0; index < register_count; ++index) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherRegister
    absl::string_view current_block =
        absl::string_view(request.sketch())
            .substr(index * kBytesPerCipherRegister, kBytesPerCipherRegister);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block,
        {Action::kBlind, Action::kReRandomize, Action::kReRandomize},
        *response_sketch));
  }
  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(*response_sketch));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
};

absl::StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t register_count,
      GetNumberOfBlocks(request.sketch(), kBytesPerCipherRegister));
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

  ASSIGN_OR_RETURN(
      std::vector<std::string> blinded_register_indexes,
      GetBlindedRegisterIndexes(request.sketch(), *protocol_cryptor));

  // Create a sorting permutation of the blinded register indexes, such that we
  // don't need to modify the sketch data, whose size could be huge. We only
  // need a way to point to registers with a same index.
  std::vector<size_t> permutation(register_count);
  absl::c_iota(permutation, 0);
  absl::c_sort(permutation, [&](size_t a, size_t b) {
    return blinded_register_indexes[a] < blinded_register_indexes[b];
  });

  BlindLastLayerIndexThenJoinRegistersResponse response;
  std::string* response_data = response.mutable_flag_counts();
  RETURN_IF_ERROR(JoinRegistersByIndexAndMergeCounts(
      *protocol_cryptor, request.sketch(), blinded_register_indexes,
      permutation, *response_data));
  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText * 2>(*response_data));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
};

absl::StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t ciphertext_count,
      GetNumberOfBlocks(request.flag_counts(), kBytesPerCipherText));
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

  DecryptOneLayerFlagAndCountResponse response;
  std::string* response_data = response.mutable_flag_counts();
  // The output sketch is the same size with the input sketch.
  response_data->reserve(request.flag_counts().size());
  for (size_t index = 0; index < ciphertext_count; ++index) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherText
    absl::string_view current_block =
        absl::string_view(request.flag_counts())
            .substr(index * kBytesPerCipherText, kBytesPerCipherText);
    RETURN_IF_ERROR(protocol_cryptor->BatchProcess(
        current_block, {Action::kPartialDecrypt}, *response_data));
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText * 2>(*response_data));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
};

absl::StatusOr<DecryptLastLayerFlagAndCountResponse>
DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request) {
  StartedThreadCpuTimer timer;

  ASSIGN_OR_RETURN(
      size_t flag_count_tuple_count,
      GetNumberOfBlocks(request.flag_counts(), kBytesPerCipherText * 2));
  // Create an ElGamal cipher for decryption.
  ASSIGN_OR_RETURN_ERROR(
      std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          request.curve_id(),
          GetPublicKeyStringPair(
              request.local_el_gamal_key_pair().public_key()),
          request.local_el_gamal_key_pair().secret_key()),
      "Failed to create the local ElGamal cipher, invalid curveId or keys");
  absl::flat_hash_map<std::string, int> count_lookup_table;
  ASSIGN_OR_RETURN(
      count_lookup_table,
      CreateCountLookUpTable(request.curve_id(), request.maximum_frequency()));

  ASSIGN_OR_RETURN(std::string is_not_destroyed,
                   GetIsNotDestroyedFlag(request.curve_id()));

  DecryptLastLayerFlagAndCountResponse response;
  for (size_t index = 0; index < flag_count_tuple_count; ++index) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherText * 2
    absl::string_view current_block =
        absl::string_view(request.flag_counts())
            .substr(index * kBytesPerCipherText * 2, kBytesPerCipherText * 2);
    ASSIGN_OR_RETURN(ElGamalCiphertext flag_ciphertext,
                     ExtractElGamalCiphertextFromString(
                         current_block.substr(0, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(std::string flag_plaintext,
                     el_gamal_cipher->Decrypt(flag_ciphertext));
    // Count value 0 is mapping to ECPoint at Infinity, whose decryption would
    // fail. So we need to check the status before fetching the value.
    ASSIGN_OR_RETURN(ElGamalCiphertext count_ciphertext,
                     ExtractElGamalCiphertextFromString(current_block.substr(
                         kBytesPerCipherText, kBytesPerCipherText)));
    absl::StatusOr<std::string> count_plaintext =
        el_gamal_cipher->Decrypt(count_ciphertext);

    if (count_plaintext.ok()) {
      // Append the result to the response.
      FlagCount* flag_count = response.add_flag_counts();
      flag_count->set_is_not_destroyed(flag_plaintext == is_not_destroyed);
      auto count_value = count_lookup_table.find(count_plaintext.value());
      // If the count_plaintext is not in the lookup table, we map the count
      // value to the maximum_frequency. This can either due to the count value
      // exceeds the maximum_frequency, or due to the count is destoryed due to
      // key collision.
      flag_count->set_frequency(count_value == count_lookup_table.end()
                                    ? request.maximum_frequency()
                                    : count_value->second);
    } else if (count_plaintext.status().message().find("POINT_AT_INFINITY") ==
               std::string::npos) {
      // When the count value is 0 (Point at Infinity), the decryption would
      // fail with message "POINT_AT_INFINITY". In this case, simply skip this
      // value. Otherwise, return the error.
      return count_plaintext.status();
    }
  }

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
};

absl::StatusOr<AddNoiseToSketchResponse> AddNoiseToSketch(
    const AddNoiseToSketchRequest& request) {
  StartedThreadCpuTimer timer;

  AddNoiseToSketchResponse response;
  *response.mutable_sketch() = request.sketch();
  // TODO: actually add noise to the sketch.
  //  For the POC, we only shuffle the registers.
  RETURN_IF_ERROR(
      SortStringByBlock<kBytesPerCipherRegister>(*response.mutable_sketch()));

  response.set_elapsed_cpu_time_millis(timer.ElapsedMillis());
  return response;
}

}  // namespace crypto
}  // namespace common
}  // namespace measurement
}  // namespace wfa
