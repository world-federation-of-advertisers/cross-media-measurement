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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"

namespace wfa::measurement::common::crypto {

using ::wfa::measurement::common::crypto::CompositeType;

// A pair of ciphertexts which store the key and count values of a liquidlegions
// register.
struct KeyCountPairCipherText {
  ElGamalCiphertext key;
  ElGamalCiphertext count;
};

// Gets the number of equal sized blocks the data chunk can be partitioned to.
absl::StatusOr<size_t> GetNumberOfBlocks(absl::string_view data,
                                         size_t block_size);

// Extracts an ElGamalCiphertext from a string_view.
absl::StatusOr<ElGamalCiphertext> ExtractElGamalCiphertextFromString(
    absl::string_view str);

// Extracts a KeyCountPairCipherText from a string_view.
absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromSubstring(
    absl::string_view str);

// Extracts a KeyCountPairCipherText from the registers at the given index.
absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromRegisters(
    absl::string_view registers, size_t register_index);

// Appends the bytes of an EcPoint to a target string.
absl::Status AppendEcPointPairToString(const ElGamalEcPointPair& ec_point_pair,
                                       std::string& result);

// Writes bytes of a pair of EcPoint to a target string at a certain position.
// Bytes are written by replacing content of the string starting at pos. The
// length of bytes written is kBytesPerCipherText = kBytesPerEcPoint * 2.
// Returns a Status with code `INVALID_ARGUMENT` when the result string is not
// long enough.
absl::Status WriteEcPointPairToString(const ElGamalEcPointPair& ec_point_pair,
                                      size_t pos, std::string& result);

// Extract a ElGamalEcPointPair from a string_view.
absl::StatusOr<ElGamalEcPointPair> GetEcPointPairFromString(
    absl::string_view str, int curve_id);

// Returns the vector of ECPoints for count values from 1 to maximum_value.
absl::StatusOr<std::vector<std::string>> GetCountValuesPlaintext(
    int maximum_value, int curve_id);

// Encrypts plaintext and appends bytes of the cipher text to a target string.
// The length of bytes appened is kBytesPerCipherText = kBytesPerEcPoint * 2.
absl::Status EncryptCompositeElGamalAndAppendToString(
    ProtocolCryptor& protocol_cryptor, CompositeType composite_type,
    absl::string_view plaintext_ec, std::string& data);

// Encrypts plaintext and writes bytes of the cipher text to a target string at
// a certain position.
// Bytes are written by replacing content of the string starting at pos. The
// length of bytes written is kBytesPerCipherText = kBytesPerEcPoint * 2.
// Returns a Status with code `INVALID_ARGUMENT` when the result string is not
// long enough.
absl::Status EncryptCompositeElGamalAndWriteToString(
    ProtocolCryptor& protocol_cryptor, CompositeType composite_type,
    absl::string_view plaintext_ec, size_t pos, std::string& result);

}  // namespace wfa::measurement::common::crypto

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
