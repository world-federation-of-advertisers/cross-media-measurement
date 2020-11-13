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

#include "wfa/measurement/common/crypto/protocol_encryption_utility.h"

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/types/span.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/liquid_legions_v1_encryption_methods.pb.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"
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

constexpr int kBytesPerEcPoint = 33;
// A ciphertext contains 2 EcPoints.
constexpr int kBytesPerCipherText = kBytesPerEcPoint * 2;
// A register contains 3 ciphertexts, i.e., (index, key, count)
constexpr int kBytesPerCipherRegister = kBytesPerCipherText * 3;
// The seed for the IsNotDestoryed flag.
constexpr char kIsNotDestroyed[] = "IsNotDestroyed";
constexpr char KUnitECPointSeed[] = "unit_ec_point";

// crypto primitives used for blinding a ciphertext.
struct CompositeCipher {
  std::unique_ptr<CommutativeElGamal> e_g_cipher;
  std::unique_ptr<ECCommutativeCipher> p_h_cipher;
};

struct KeyCountPairCipherText {
  ElGamalCiphertext key;
  ElGamalCiphertext count;
};

ElGamalCiphertext GetPublicKeyStringPair(const ElGamalPublicKey& public_keys) {
  return std::make_pair(public_keys.el_gamal_g(), public_keys.el_gamal_y());
}

absl::Status AppendEcPointPairToString(const ElGamalEcPointPair& ec_point_pair,
                                       std::string& result) {
  std::string temp;
  ASSIGN_OR_RETURN(temp, ec_point_pair.u.ToBytesCompressed());
  result.append(temp);
  ASSIGN_OR_RETURN(temp, ec_point_pair.e.ToBytesCompressed());
  result.append(temp);
  return absl::OkStatus();
}

// Extract an ElGamalCiphertext from a string_view.
absl::StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText) {
    return absl::InternalError("string size doesn't match ciphertext size.");
  }
  return std::make_pair(
      std::string(str.substr(0, kBytesPerEcPoint)),
      std::string(str.substr(kBytesPerEcPoint, kBytesPerEcPoint)));
}

// Extract a KeyCountPairCipherText from a string_view.
absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromSubstring(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText * 2) {
    return absl::InternalError(
        "string size doesn't match keycount pair ciphertext size.");
  }
  KeyCountPairCipherText result;
  ASSIGN_OR_RETURN(result.key, ExtractElGamalCiphertextFromString(
                                   str.substr(0, kBytesPerCipherText)));
  ASSIGN_OR_RETURN(result.count,
                   ExtractElGamalCiphertextFromString(
                       str.substr(kBytesPerCipherText, kBytesPerCipherText)));
  return result;
}

absl::Status ValidateByteSize(absl::string_view data, const int bytes_per_row) {
  if (data.empty()) {
    return absl::InvalidArgumentError("Input data is empty");
  }
  if (data.size() % bytes_per_row) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The size of byte array is not divisible by the row_size: ",
        bytes_per_row));
  }
  return absl::OkStatus();
}

// Blind the last layer of ElGamal Encryption of registers, and return the
// deterministically encrypted results.
absl::StatusOr<std::vector<std::string>> GetBlindedRegisterIndexes(
    absl::string_view sketch, ProtocolCryptor& protocol_cryptor) {
  RETURN_IF_ERROR(ValidateByteSize(sketch, kBytesPerCipherRegister));
  const int num_registers = sketch.size() / kBytesPerCipherRegister;
  std::vector<std::string> blinded_register_indexes;
  blinded_register_indexes.reserve(num_registers);
  for (size_t offset = 0; offset < sketch.size();
       offset += kBytesPerCipherRegister) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherText
    absl::string_view current_block =
        absl::string_view(sketch).substr(offset, kBytesPerCipherText);
    ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                     ExtractElGamalCiphertextFromString(current_block));
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     protocol_cryptor.DecryptLocalElGamal(ciphertext));
    blinded_register_indexes.emplace_back(std::move(decrypted_el_gamal));
  }
  return blinded_register_indexes;
}

// Create a lookup table mapping the ECPoint strings to real world frequency
// integers.
// During encryption, the way we map frequency to ECPoint is that:
//   1. Pre-determine a seed, i.e., KUnitECPointSeed,  for integer 1.
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
  // KUnitECPointSeed;
  ASSIGN_OR_RETURN(ECPoint count_1_ec,
                   ec_group.GetPointByHashingToCurveSha256(KUnitECPointSeed));
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
                   ec_group.GetPointByHashingToCurveSha256(kIsNotDestroyed));
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
  // Create a new ElGamal Encryption of the is_not_destroyed flag.
  ASSIGN_OR_RETURN(std::string is_not_destroyed,
                   protocol_cryptor.MapToCurve(std::string(kIsNotDestroyed)));
  ASSIGN_OR_RETURN(ElGamalCiphertext is_not_destroyed_ciphertext,
                   protocol_cryptor.EncryptCompositeElGamal(is_not_destroyed));
  size_t offset =
      sub_permutation[0] * kBytesPerCipherRegister + kBytesPerCipherText;
  if (offset > sketch.size()) {
    return absl::InternalError("Offset is out of bound");
  }
  ASSIGN_OR_RETURN(KeyCountPairCipherText key_count_0,
                   ExtractKeyCountPairFromSubstring(
                       sketch.substr(offset, kBytesPerCipherText * 2)));
  // Initialize the flag and count.
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair final_flag,
      protocol_cryptor.ToElGamalEcPoints(is_not_destroyed_ciphertext));
  ASSIGN_OR_RETURN(ElGamalEcPointPair final_count,
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
      size_t data_offset =
          sub_permutation[i] * kBytesPerCipherRegister + kBytesPerCipherText;
      if (data_offset > sketch.size()) {
        return absl::InternalError("Offset is out of bound");
      }
      ASSIGN_OR_RETURN(KeyCountPairCipherText next_key_count,
                       ExtractKeyCountPairFromSubstring(sketch.substr(
                           data_offset, kBytesPerCipherText * 2)));
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
      ASSIGN_OR_RETURN(final_flag, AddEcPointPairs(final_flag, destructor));
      ASSIGN_OR_RETURN(final_count, AddEcPointPairs(final_count, count_i));
      ASSIGN_OR_RETURN(final_count, AddEcPointPairs(final_count, destructor));
    }
  }
  // Append the (flag, count) result for this group of registers to the final
  // response.
  RETURN_IF_ERROR(AppendEcPointPairToString(final_flag, response));
  RETURN_IF_ERROR(AppendEcPointPairToString(final_count, response));
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

absl::Duration getCurrentThreadCpuDuration() {
  struct timespec ts;
#ifdef __linux__
  CHECK(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0)
      << "Failed to get the thread cpu time.";
  return absl::DurationFromTimespec(ts);
#else
  return absl::ZeroDuration();
#endif
}

}  // namespace

absl::StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request) {
  absl::Duration startCpuDuration = getCurrentThreadCpuDuration();

  RETURN_IF_ERROR(ValidateByteSize(request.sketch(), kBytesPerCipherRegister));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().el_gamal_pk().el_gamal_g(),
              request.local_el_gamal_key_pair().el_gamal_pk().el_gamal_y()),
          request.local_el_gamal_key_pair().el_gamal_sk(),
          request.local_pohlig_hellman_sk(),
          std::make_pair(request.composite_el_gamal_public_key().el_gamal_g(),
                         request.composite_el_gamal_public_key().el_gamal_y())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  BlindOneLayerRegisterIndexResponse response;
  *response.mutable_local_pohlig_hellman_sk() =
      protocol_cryptor->GetLocalPohligHellmanKey();
  std::string* response_sketch = response.mutable_sketch();
  // The output sketch is the same size with the input sketch.
  response_sketch->reserve(request.sketch().size());
  for (size_t offset = 0; offset < request.sketch().size();
       offset += kBytesPerCipherRegister) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherRegister
    absl::string_view current_block =
        absl::string_view(request.sketch())
            .substr(offset, kBytesPerCipherRegister);
    ASSIGN_OR_RETURN(ElGamalCiphertext index,
                     ExtractElGamalCiphertextFromString(
                         current_block.substr(0, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(ElGamalCiphertext key,
                     ExtractElGamalCiphertextFromString(current_block.substr(
                         kBytesPerCipherText, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(ElGamalCiphertext count,
                     ExtractElGamalCiphertextFromString(current_block.substr(
                         kBytesPerCipherText * 2, kBytesPerCipherText)));

    ASSIGN_OR_RETURN(ElGamalCiphertext blinded_index,
                     protocol_cryptor->Blind(index));
    ASSIGN_OR_RETURN(ElGamalCiphertext re_randomized_key,
                     protocol_cryptor->ReRandomize(key));
    ASSIGN_OR_RETURN(ElGamalCiphertext re_randomized_count,
                     protocol_cryptor->ReRandomize(count));
    // Append the result to the response.
    response_sketch->append(blinded_index.first);
    response_sketch->append(blinded_index.second);
    response_sketch->append(re_randomized_key.first);
    response_sketch->append(re_randomized_key.second);
    response_sketch->append(re_randomized_count.first);
    response_sketch->append(re_randomized_count.second);
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherRegister>(*response_sketch));

  absl::Duration elaspedDuration =
      getCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
};

absl::StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request) {
  absl::Duration startCpuDuration = getCurrentThreadCpuDuration();

  RETURN_IF_ERROR(ValidateByteSize(request.sketch(), kBytesPerCipherRegister));
  ASSIGN_OR_RETURN_ERROR(
      auto protocol_cryptor,
      CreateProtocolCryptorWithKeys(
          request.curve_id(),
          std::make_pair(
              request.local_el_gamal_key_pair().el_gamal_pk().el_gamal_g(),
              request.local_el_gamal_key_pair().el_gamal_pk().el_gamal_y()),
          request.local_el_gamal_key_pair().el_gamal_sk(),
          request.local_pohlig_hellman_sk(),
          std::make_pair(request.composite_el_gamal_public_key().el_gamal_g(),
                         request.composite_el_gamal_public_key().el_gamal_y())),
      "Failed to create the protocol cipher, invalid curveId or keys.");

  ASSIGN_OR_RETURN(
      std::vector<std::string> blinded_register_indexes,
      GetBlindedRegisterIndexes(request.sketch(), *protocol_cryptor));
  const size_t num_registers =
      request.sketch().size() / kBytesPerCipherRegister;

  // Create a sorting permutation of the blinded register indexes, such that we
  // don't need to modify the sketch data, whose size could be huge. We only
  // need a way to point to registers with a same index.
  std::vector<size_t> permutation(num_registers);
  absl::c_iota(permutation, 0);
  absl::c_sort(permutation, [&](size_t a, size_t b) {
    return blinded_register_indexes[a] < blinded_register_indexes[b];
  });

  BlindLastLayerIndexThenJoinRegistersResponse response;
  std::string* response_data = response.mutable_flag_counts();
  *response.mutable_local_pohlig_hellman_sk() =
      protocol_cryptor->GetLocalPohligHellmanKey();
  RETURN_IF_ERROR(JoinRegistersByIndexAndMergeCounts(
      *protocol_cryptor, request.sketch(), blinded_register_indexes,
      permutation, *response_data));
  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText * 2>(*response_data));

  absl::Duration elaspedDuration =
      getCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
};

absl::StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request) {
  absl::Duration startCpuDuration = getCurrentThreadCpuDuration();

  // Each unit contains 2 ciphertexts, e.g., flag and count.
  RETURN_IF_ERROR(
      ValidateByteSize(request.flag_counts(), kBytesPerCipherText * 2));
  // Create an ElGamal cipher for decryption.
  ASSIGN_OR_RETURN_ERROR(
      std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          request.curve_id(),
          GetPublicKeyStringPair(
              request.local_el_gamal_key_pair().el_gamal_pk()),
          request.local_el_gamal_key_pair().el_gamal_sk()),
      "Failed to create the local ElGamal cipher, invalid curveId or keys");

  DecryptOneLayerFlagAndCountResponse response;
  std::string* response_data = response.mutable_flag_counts();
  // The output sketch is the same size with the input sketch.
  response_data->reserve(request.flag_counts().size());
  for (size_t offset = 0; offset < request.flag_counts().size();
       offset += kBytesPerCipherText) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherText
    absl::string_view current_block = absl::string_view(request.flag_counts())
                                          .substr(offset, kBytesPerCipherText);
    ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                     ExtractElGamalCiphertextFromString(current_block));
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     el_gamal_cipher->Decrypt(ciphertext));
    // Append the result to the response.
    // The first part of the ciphertext is the random number which is still
    // required to decrypt the other layers of ElGamal encryptions (at the
    // subsequent duchies. So we keep it.
    response_data->append(ciphertext.first);
    response_data->append(decrypted_el_gamal);
  }

  RETURN_IF_ERROR(SortStringByBlock<kBytesPerCipherText * 2>(*response_data));

  absl::Duration elaspedDuration =
      getCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
};

absl::StatusOr<DecryptLastLayerFlagAndCountResponse>
DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request) {
  absl::Duration startCpuDuration = getCurrentThreadCpuDuration();

  // Each register contains 2 ciphertexts, e.g., flag and count.
  RETURN_IF_ERROR(
      ValidateByteSize(request.flag_counts(), kBytesPerCipherText * 2));
  // Create an ElGamal cipher for decryption.
  ASSIGN_OR_RETURN_ERROR(
      std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          request.curve_id(),
          GetPublicKeyStringPair(
              request.local_el_gamal_key_pair().el_gamal_pk()),
          request.local_el_gamal_key_pair().el_gamal_sk()),
      "Failed to create the local ElGamal cipher, invalid curveId or keys");
  absl::flat_hash_map<std::string, int> count_lookup_table;
  ASSIGN_OR_RETURN(
      count_lookup_table,
      CreateCountLookUpTable(request.curve_id(), request.maximum_frequency()));

  ASSIGN_OR_RETURN(std::string is_not_destroyed,
                   GetIsNotDestroyedFlag(request.curve_id()));

  DecryptLastLayerFlagAndCountResponse response;
  for (size_t offset = 0; offset < request.flag_counts().size();
       offset += kBytesPerCipherText * 2) {
    // The size of current_block is guaranteed to be equal to
    // kBytesPerCipherText * 2
    absl::string_view current_block =
        absl::string_view(request.flag_counts())
            .substr(offset, kBytesPerCipherText * 2);
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

  absl::Duration elaspedDuration =
      getCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
};

absl::StatusOr<AddNoiseToSketchResponse> AddNoiseToSketch(
    const AddNoiseToSketchRequest& request) {
  absl::Duration startCpuDuration = getCurrentThreadCpuDuration();

  AddNoiseToSketchResponse response;
  *response.mutable_sketch() = request.sketch();
  // TODO: actually add noise to the sketch.
  //  For the POC, we only shuffle the registers.
  RETURN_IF_ERROR(
      SortStringByBlock<kBytesPerCipherRegister>(*response.mutable_sketch()));

  absl::Duration elaspedDuration =
      getCurrentThreadCpuDuration() - startCpuDuration;
  response.set_elapsed_cpu_time_millis(
      absl::ToInt64Milliseconds(elaspedDuration));
  return response;
}

}  // namespace crypto
}  // namespace common
}  // namespace measurement
}  // namespace wfa
