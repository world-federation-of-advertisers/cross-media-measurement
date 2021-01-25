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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/common/crypto/protocol_cryptor.h"

namespace wfa::measurement::common::crypto {

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

// Blinds the last layer of ElGamal Encryption of register indexes, and return
// the deterministically encrypted results.
absl::StatusOr<std::vector<std::string>> GetBlindedRegisterIndexes(
    absl::string_view data, ProtocolCryptor& protocol_cryptor);

// Extracts a KeyCountPairCipherText from a string_view.
absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromSubstring(
    absl::string_view str);

// Extracts a KeyCountPairCipherText from the registers at the given index.
absl::StatusOr<KeyCountPairCipherText> ExtractKeyCountPairFromRegisters(
    absl::string_view registers, size_t register_index);

// Appends the bytes of an EcPoint to a target string.
absl::Status AppendEcPointPairToString(const ElGamalEcPointPair& ec_point_pair,
                                       std::string& result);

template <typename T>
absl::Status ParseRequestFromString(T& request_proto,
                                    const std::string& serialized_request) {
  return request_proto.ParseFromString(serialized_request)
             ? absl::OkStatus()
             : absl::InternalError(
                   "failed to parse the serialized request proto.");
}

// Returns the vector of ECPoints for count values from 1 to maximum_value.
absl::StatusOr<std::vector<std::string>> GetCountValuesPlaintext(
    int maximum_value, int curve_id);

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_