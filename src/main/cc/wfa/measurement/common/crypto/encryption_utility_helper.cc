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

#include "wfa/measurement/common/crypto/encryption_utility_helper.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"

namespace wfa::measurement::common::crypto {

absl::StatusOr<size_t> GetNumberOfBlocks(absl::string_view data,
                                         size_t block_size) {
  if (block_size == 0) {
    return absl::InvalidArgumentError("block_size is zero");
  }
  if (data.size() % block_size != 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The size of byte array is not divisible by the block_size: ",
        block_size));
  }
  return data.size() / block_size;
}

absl::StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str) {
  if (str.size() != kBytesPerCipherText) {
    return absl::InternalError("string size doesn't match ciphertext size.");
  }
  return std::make_pair(
      std::string(str.substr(0, kBytesPerEcPoint)),
      std::string(str.substr(kBytesPerEcPoint, kBytesPerEcPoint)));
}

absl::StatusOr<std::vector<std::string>> GetBlindedRegisterIndexes(
    absl::string_view data, ProtocolCryptor& protocol_cryptor) {
  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(data, kBytesPerCipherRegister));
  std::vector<std::string> blinded_register_indexes;
  blinded_register_indexes.reserve(register_count);
  for (size_t index = 0; index < register_count; ++index) {
    // The size of data_block is guaranteed to be equal to
    // kBytesPerCipherText
    absl::string_view data_block =
        data.substr(index * kBytesPerCipherRegister, kBytesPerCipherText);
    ASSIGN_OR_RETURN(ElGamalCiphertext ciphertext,
                     ExtractElGamalCiphertextFromString(data_block));
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     protocol_cryptor.DecryptLocalElGamal(ciphertext));
    blinded_register_indexes.push_back(std::move(decrypted_el_gamal));
  }
  return blinded_register_indexes;
}

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

absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromRegisters(
    absl::string_view registers, size_t register_index) {
  ASSIGN_OR_RETURN(size_t register_count,
                   GetNumberOfBlocks(registers, kBytesPerCipherRegister));
  if (register_index >= register_count) {
    return absl::InternalError("index is out of bound");
  }
  size_t offset =
      register_index * kBytesPerCipherRegister + kBytesPerCipherText;
  return ExtractKeyCountPairFromSubstring(
      registers.substr(offset, kBytesPerCipherText * 2));
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

absl::StatusOr<std::vector<std::string>> GetCountValuesPlaintext(
    int maximum_value, int curve_id) {
  if (maximum_value < 1) {
    return absl::InvalidArgumentError("maximum_value should be at least 1.");
  }
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(ECGroup ec_group, ECGroup::Create(curve_id, ctx.get()));
  ASSIGN_OR_RETURN(ECPoint count_1,
                   ec_group.GetPointByHashingToCurveSha256(kUnitECPointSeed));
  ASSIGN_OR_RETURN(std::string count_1_str, count_1.ToBytesCompressed());

  std::vector<std::string> result;
  result.push_back(count_1_str);
  for (size_t i = 2; i <= maximum_value; ++i) {
    ASSIGN_OR_RETURN(ECPoint count_i, count_1.Mul(ctx->CreateBigNum(i)));
    ASSIGN_OR_RETURN(std::string count_i_str, count_i.ToBytesCompressed());
    result.push_back(count_i_str);
  }
  return result;
}

}  // namespace wfa::measurement::common::crypto
