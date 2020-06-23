/*
 * Copyright 2020 Google Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "protocol_encryption_utility.h"

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "util/canonical_errors.h"
#include "util/macros.h"
#include "util/string_block_sorter.h"

namespace wfa::measurement::crypto {

namespace {

using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::private_join_and_compute::InternalError;
using ::private_join_and_compute::InvalidArgumentError;
using ::private_join_and_compute::Status;
using ::wfa::measurement::internal::duchy::ElGamalKeys;
using ::wfa::measurement::internal::duchy::ElGamalPublicKeys;
using ElGamalCiphertext = std::pair<std::string, std::string>;
using FlagCount = ::wfa::measurement::internal::duchy::
    DecryptLastLayerFlagAndCountResponse::FlagCount;

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

// A struture containing the two ECPoints of an ElGamal encryption.
struct ElGamalEcPointPair {
  ECPoint u;  // g^r
  ECPoint e;  // m*y^r
};

// crypto primitives used in SameKeyAggregation;
struct SameKeyAggregator {
  // The ElGamal cipher using the same keys used by date providers.
  std::unique_ptr<CommutativeElGamal> client_e_g_cipher;
  // The ECGroup working on.
  std::unique_ptr<ECGroup> ec_group;
  // The Context of the ECGroup.
  std::unique_ptr<Context> ctx;
  // The byte representation of the ECPoint mapped from kIsNotDestroyed,
  std::string is_not_destroyed_ec_string;
};

struct KeyCountPairCipherText {
  ElGamalCiphertext key;
  ElGamalCiphertext count;
};

ElGamalCiphertext GetPublicKeyStringPair(const ElGamalPublicKeys& public_keys) {
  return std::make_pair(public_keys.el_gamal_g(), public_keys.el_gamal_y());
}

StatusOr<ElGamalEcPointPair> GetElGamalEcPoints(
    const ElGamalCiphertext& cipher_text, const ECGroup& ec_group) {
  ASSIGN_OR_RETURN(ECPoint ec_point_u,
                   ec_group.CreateECPoint(cipher_text.first));
  ASSIGN_OR_RETURN(ECPoint ec_point_e,
                   ec_group.CreateECPoint(cipher_text.second));
  ElGamalEcPointPair result = {
      .u = std::move(ec_point_u),
      .e = std::move(ec_point_e),
  };
  return result;
}

StatusOr<ElGamalEcPointPair> GetEcPointPairInverse(
    const ElGamalEcPointPair& ec_point_pair) {
  ASSIGN_OR_RETURN(ECPoint inverse_u, ec_point_pair.u.Inverse());
  ASSIGN_OR_RETURN(ECPoint inverse_e, ec_point_pair.e.Inverse());
  ElGamalEcPointPair result = {
      .u = std::move(inverse_u),
      .e = std::move(inverse_e),
  };
  return result;
}

StatusOr<ElGamalEcPointPair> MultiplyEcPointPairByScalar(
    const ElGamalEcPointPair& ec_point_pair, const BigNum& n) {
  ASSIGN_OR_RETURN(ECPoint result_u, ec_point_pair.u.Mul(n));
  ASSIGN_OR_RETURN(ECPoint result_e, ec_point_pair.e.Mul(n));
  ElGamalEcPointPair result = {
      .u = std::move(result_u),
      .e = std::move(result_e),
  };
  return result;
}

Status AppendEcPointPairToString(const ElGamalEcPointPair& ec_point_pair,
                                 std::string& result) {
  std::string temp;
  ASSIGN_OR_RETURN(temp, ec_point_pair.u.ToBytesCompressed());
  result.append(temp);
  ASSIGN_OR_RETURN(temp, ec_point_pair.e.ToBytesCompressed());
  result.append(temp);
  return Status::OK;
}

StatusOr<ElGamalEcPointPair> AddEcPointPairs(const ElGamalEcPointPair& a,
                                             const ElGamalEcPointPair& b) {
  ASSIGN_OR_RETURN(ECPoint result_u, a.u.Add(b.u));
  ASSIGN_OR_RETURN(ECPoint result_e, a.e.Add(b.e));
  ElGamalEcPointPair result = {
      .u = std::move(result_u),
      .e = std::move(result_e),
  };
  return result;
}

StatusOr<CompositeCipher> CreateCompositeCipher(
    const int curve_id, const ElGamalKeys& el_gamal_keys,
    const std::string& pohlig_hellman_sk) {
  CompositeCipher result;
  // Create the ElGamal cipher using the provided keys.
  ASSIGN_OR_RETURN(
      result.e_g_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          curve_id, GetPublicKeyStringPair(el_gamal_keys.el_gamal_pk()),
          el_gamal_keys.el_gamal_sk()));
  // Create the Pohlig Hellman cipher using the provided key or a random key if
  // no key is provided.
  ASSIGN_OR_RETURN(result.p_h_cipher,
                   pohlig_hellman_sk.empty()
                       ? ECCommutativeCipher::CreateWithNewKey(
                             curve_id, ECCommutativeCipher::HashType::SHA256)
                       : ECCommutativeCipher::CreateFromKey(
                             curve_id, pohlig_hellman_sk,
                             ECCommutativeCipher::HashType::SHA256));
  return result;
}

// Hashing a string to the elliptical curve and return the string representation
// of the obtained ECPoint.
StatusOr<std::string> MapToCurve(const ECGroup& ec_group,
                                 const std::string& str) {
  ASSIGN_OR_RETURN(ECPoint temp_ec_point,
                   ec_group.GetPointByHashingToCurveSha256(str));
  ASSIGN_OR_RETURN(std::string result, temp_ec_point.ToBytesCompressed());
  return result;
}

StatusOr<SameKeyAggregator> CreateSameKeyAggregator(
    const int curve_id, const ElGamalPublicKeys& client_el_gamal_keys) {
  SameKeyAggregator result;
  // Create the client ElGamal cipher using the provided keys.
  ASSIGN_OR_RETURN(result.client_e_g_cipher,
                   CommutativeElGamal::CreateFromPublicKey(
                       curve_id, GetPublicKeyStringPair(client_el_gamal_keys)));
  // Set the ECGroup and Conext.
  result.ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(auto temp_ec_group,
                   ECGroup::Create(curve_id, result.ctx.get()));
  result.ec_group = absl::make_unique<ECGroup>(std::move(temp_ec_group));
  // Calculate the is_not_destroyed ECPoint, so it could be reused in future
  // computation.
  ASSIGN_OR_RETURN(result.is_not_destroyed_ec_string,
                   MapToCurve(*result.ec_group.get(), kIsNotDestroyed));
  return result;
}

// Extract an ElGamalCiphertext from a string_view.
StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText) {
    return InternalError("string size doesn't match ciphertext size.");
  }
  return std::make_pair(
      std::string(str.substr(0, kBytesPerEcPoint)),
      std::string(str.substr(kBytesPerEcPoint, kBytesPerEcPoint)));
}

// Extract a KeyCountPairCipherText from a string_view.
StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromSubstring(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText * 2) {
    return InternalError(
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

Status ReRandomizeCiphertextAndAppendToString(
    const ECGroup& ec_group, const CommutativeElGamal& client_e_g_cipher,
    absl::string_view ciphertext_string, std::string& result) {
  ASSIGN_OR_RETURN(ElGamalCiphertext zero,
                   client_e_g_cipher.EncryptIdentityElement());
  ASSIGN_OR_RETURN(ElGamalEcPointPair zero_ec,
                   GetElGamalEcPoints(zero, ec_group));
  ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                   ExtractElGamalCiphertextFromString(ciphertext_string));
  ASSIGN_OR_RETURN(ElGamalEcPointPair ciphertext_ec,
                   GetElGamalEcPoints(ciphertext, ec_group));
  ASSIGN_OR_RETURN(ElGamalEcPointPair temp,
                   AddEcPointPairs(zero_ec, ciphertext_ec));
  return AppendEcPointPairToString(temp, result);
}

Status ValidateByteSize(absl::string_view data, const int bytes_per_row) {
  if (data.empty()) {
    return InvalidArgumentError("Input data is empty");
  }
  if (data.size() % bytes_per_row) {
    return InvalidArgumentError(absl::StrCat(
        "The size of byte array is not divisible by the row_size: ",
        bytes_per_row));
  }
  return Status::OK;
}

// Blind the last layer of ElGamal Encryption of registers, and return the
// deterministically encrypted results.
StatusOr<std::vector<std::string>> GetBlindedRegisterIndexes(
    absl::string_view sketch, const CompositeCipher& composite_cipher) {
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
                     composite_cipher.e_g_cipher->Decrypt(ciphertext));
    ASSIGN_OR_RETURN(
        std::string re_encryption,
        composite_cipher.p_h_cipher->ReEncrypt(decrypted_el_gamal));
    blinded_register_indexes.emplace_back(std::move(re_encryption));
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
StatusOr<absl::flat_hash_map<std::string, int>> CreateCountLookUpTable(
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

StatusOr<std::string> GetIsNotDestroyedFlag(const int curve_id) {
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(auto ec_group, ECGroup::Create(curve_id, ctx.get()));
  ASSIGN_OR_RETURN(std::string is_not_destroyed,
                   MapToCurve(ec_group, kIsNotDestroyed));
  return is_not_destroyed;
}

// Merge all the counts in each group using the SameKeyAggregation algorithm.
// The calculated (flag, count) is appended to the response.
// 'sub_permutation' contains the locations of the registers belonging to this
// group, i.e., having the same blinded register index.
Status MergeCountsUsingSameKeyAggregation(
    absl::Span<const size_t> sub_permutation, absl::string_view sketch,
    const SameKeyAggregator& same_key_aggregator, std::string& response) {
  if (sub_permutation.empty()) {
    return InternalError("Empty sub permutation.");
  }
  // Create a new ElGamal Encryption of the is_not_destroyed flag.
  ASSIGN_OR_RETURN(ElGamalCiphertext is_not_destroyed_ciphertext,
                   same_key_aggregator.client_e_g_cipher->Encrypt(
                       same_key_aggregator.is_not_destroyed_ec_string));
  size_t offset =
      sub_permutation[0] * kBytesPerCipherRegister + kBytesPerCipherText;
  if (offset > sketch.size()) {
    return InternalError("Offset is out of bound");
  }
  ASSIGN_OR_RETURN(KeyCountPairCipherText key_count_0,
                   ExtractKeyCountPairFromSubstring(
                       sketch.substr(offset, kBytesPerCipherText * 2)));
  // Initialize the flag and count.
  ASSIGN_OR_RETURN(ElGamalEcPointPair final_flag,
                   GetElGamalEcPoints(is_not_destroyed_ciphertext,
                                      *same_key_aggregator.ec_group));
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair final_count,
      GetElGamalEcPoints(key_count_0.count, *same_key_aggregator.ec_group));

  // Aggregate other (key, count) pairs if any.
  if (sub_permutation.size() > 1) {
    // calculate the inverse of key_0. i.e., -K0
    ASSIGN_OR_RETURN(
        ElGamalEcPointPair key_0,
        GetElGamalEcPoints(key_count_0.key, *same_key_aggregator.ec_group));
    ASSIGN_OR_RETURN(ElGamalEcPointPair key_0_inverse,
                     GetEcPointPairInverse(key_0));
    // Merge all addition points to the result
    for (size_t i = 1; i < sub_permutation.size(); ++i) {
      size_t data_offset =
          sub_permutation[i] * kBytesPerCipherRegister + kBytesPerCipherText;
      if (data_offset > sketch.size()) {
        return InternalError("Offset is out of bound");
      }
      ASSIGN_OR_RETURN(KeyCountPairCipherText next_key_count,
                       ExtractKeyCountPairFromSubstring(sketch.substr(
                           data_offset, kBytesPerCipherText * 2)));
      // Get the ECPoints of this (Key, count) pair, 2 for key, and 2 for count.
      ASSIGN_OR_RETURN(ElGamalEcPointPair count_i,
                       GetElGamalEcPoints(next_key_count.count,
                                          *same_key_aggregator.ec_group));
      ASSIGN_OR_RETURN(ElGamalEcPointPair key_i,
                       GetElGamalEcPoints(next_key_count.key,
                                          *same_key_aggregator.ec_group));
      // Calculate the destructor.
      BigNum r = same_key_aggregator.ec_group->GeneratePrivateKey();
      ASSIGN_OR_RETURN(ElGamalEcPointPair key_delta,
                       AddEcPointPairs(key_i, key_0_inverse));
      ASSIGN_OR_RETURN(ElGamalEcPointPair destructor,
                       MultiplyEcPointPairByScalar(key_delta, r));
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
  return Status::OK;
}

// Join registers with the same blinded register index as a group, and merge all
// the counts in each group using the SameKeyAggregation algorithm. Then, append
// the (flag, count) result of each group to the response.
Status JoinRegistersByIndexAndMergeCounts(
    const int curve_id, const ElGamalPublicKeys& public_keys,
    absl::string_view sketch,
    const std::vector<std::string>& blinded_register_indexes,
    absl::Span<const size_t> permutation, std::string& response) {
  // Create an ElGamal cipher using the same combined public key used by the
  // data providers. The cipher is used to calculate destructor in the
  // SameKeyAggregation.
  ASSIGN_OR_RETURN(SameKeyAggregator same_key_aggregator,
                   CreateSameKeyAggregator(curve_id, public_keys));
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
          permutation.subspan(start, i - start), sketch, same_key_aggregator,
          response));
      // Reset the starting point.
      start = i;
    }
  }
  // Process the last group and append the result to the response.
  RETURN_IF_ERROR(MergeCountsUsingSameKeyAggregation(
      permutation.subspan(start, num_registers - start), sketch,
      same_key_aggregator, response));
  return Status::OK;
}

}  // namespace

StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request) {
  RETURN_IF_ERROR(ValidateByteSize(request.sketch(), kBytesPerCipherRegister));
  // Composite cipher used to blind the positions.
  ASSIGN_OR_RETURN(
      CompositeCipher composite_cipher,
      CreateCompositeCipher(request.curve_id(), request.local_el_gamal_keys(),
                            request.local_pohlig_hellman_sk()));
  // ElGamal cipher used to re-randomize the keys and counts.
  ASSIGN_OR_RETURN(
      std::unique_ptr<CommutativeElGamal> client_e_g_cipher,
      CommutativeElGamal::CreateFromPublicKey(
          request.curve_id(),
          GetPublicKeyStringPair(request.composite_el_gamal_keys())));
  // Set the ECGroup and Conext.
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(ECGroup ec_group,
                   ECGroup::Create(request.curve_id(), ctx.get()));
  BlindOneLayerRegisterIndexResponse response;
  *response.mutable_local_pohlig_hellman_sk() =
      composite_cipher.p_h_cipher->GetPrivateKeyBytes();
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
    ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                     ExtractElGamalCiphertextFromString(
                         current_block.substr(0, kBytesPerCipherText)));
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     composite_cipher.e_g_cipher->Decrypt(ciphertext));
    ASSIGN_OR_RETURN(ElGamalCiphertext re_encrypted_p_h,
                     composite_cipher.p_h_cipher->ReEncryptElGamalCiphertext(
                         std::make_pair(ciphertext.first, decrypted_el_gamal)));
    // Append the result to the response.
    // 1. append the blinded register index value.
    response_sketch->append(re_encrypted_p_h.first);
    response_sketch->append(re_encrypted_p_h.second);
    // 2. re-randomize the key and count values and append to the response.
    RETURN_IF_ERROR(ReRandomizeCiphertextAndAppendToString(
        ec_group, *client_e_g_cipher.get(),
        current_block.substr(kBytesPerCipherText, kBytesPerCipherText),
        *response_sketch));
    RETURN_IF_ERROR(ReRandomizeCiphertextAndAppendToString(
        ec_group, *client_e_g_cipher.get(),
        current_block.substr(kBytesPerCipherText * 2, kBytesPerCipherText),
        *response_sketch));
  }
  RETURN_IF_ERROR(
      util::SortStringByBlock<kBytesPerCipherRegister>(*response_sketch));
  return response;
};

StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request) {
  RETURN_IF_ERROR(ValidateByteSize(request.sketch(), kBytesPerCipherRegister));
  // Create a CompositeCipher to blind the register index;
  ASSIGN_OR_RETURN(
      CompositeCipher composite_cipher,
      CreateCompositeCipher(request.curve_id(), request.local_el_gamal_keys(),
                            request.local_pohlig_hellman_sk()));
  ASSIGN_OR_RETURN(
      std::vector<std::string> blinded_register_indexes,
      GetBlindedRegisterIndexes(request.sketch(), composite_cipher));
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
      composite_cipher.p_h_cipher->GetPrivateKeyBytes();
  RETURN_IF_ERROR(JoinRegistersByIndexAndMergeCounts(
      request.curve_id(), request.composite_el_gamal_keys(), request.sketch(),
      blinded_register_indexes, permutation, *response_data));
  RETURN_IF_ERROR(
      util::SortStringByBlock<kBytesPerCipherText * 2>(*response_data));
  return response;
};

StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request) {
  // Each unit contains 2 ciphertexts, e.g., flag and count.
  RETURN_IF_ERROR(
      ValidateByteSize(request.flag_counts(), kBytesPerCipherText * 2));
  // Create an ElGamal cipher for decryption.
  ASSIGN_OR_RETURN(
      std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          request.curve_id(),
          GetPublicKeyStringPair(request.local_el_gamal_keys().el_gamal_pk()),
          request.local_el_gamal_keys().el_gamal_sk()));

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

  RETURN_IF_ERROR(
      util::SortStringByBlock<kBytesPerCipherText * 2>(*response_data));
  return response;
};

StatusOr<DecryptLastLayerFlagAndCountResponse> DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request) {
  // Each register contains 2 ciphertexts, e.g., flag and count.
  RETURN_IF_ERROR(
      ValidateByteSize(request.flag_counts(), kBytesPerCipherText * 2));
  // Create an ElGamal cipher for decryption.
  ASSIGN_OR_RETURN(
      std::unique_ptr<CommutativeElGamal> el_gamal_cipher,
      CommutativeElGamal::CreateFromPublicAndPrivateKeys(
          request.curve_id(),
          GetPublicKeyStringPair(request.local_el_gamal_keys().el_gamal_pk()),
          request.local_el_gamal_keys().el_gamal_sk()));
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
    StatusOr<std::string> count_plaintext =
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
  return response;
};

}  // namespace wfa::measurement::crypto
