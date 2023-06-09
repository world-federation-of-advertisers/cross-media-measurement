// Copyright 2021 The Cross-Media Measurement Authors
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

#ifndef SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_DETERMINISTIC_COMMUTATIVE_CIPHER_H_
#define SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_DETERMINISTIC_COMMUTATIVE_CIPHER_H_

#include <google/protobuf/repeated_field.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {

// A cryptosystem that is deterministic and commutative.
//
// This is not reversible, however: inputs to Encrypt are not necessarily
// recoverable by Decrypt. Instead, it is only guaranteed that
// Decrypt(Encrypt(x)) == Decrypt(ReEncrypt(Decrypt(Encrypt(x))
//                     == Decrypt(Decrypt(ReEncrypt(Encrypt(x)).
class DeterministicCommutativeCipher {
 public:
  virtual ~DeterministicCommutativeCipher() = default;

  DeterministicCommutativeCipher(DeterministicCommutativeCipher&& other) =
      delete;
  DeterministicCommutativeCipher& operator=(
      DeterministicCommutativeCipher&& other) = delete;
  DeterministicCommutativeCipher(const DeterministicCommutativeCipher&) =
      delete;
  DeterministicCommutativeCipher& operator=(
      const DeterministicCommutativeCipher&) = delete;

  // Encrypts a plaintext. This cannot be the output of Encrypt, Decrypt, or
  // ReEncrypt.
  virtual absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const = 0;

  // Removes a layer of encryption from ciphertext. See the caveat in the class
  // documentation.
  virtual absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const = 0;

  // Adds a layer of encryption to ciphertext.
  virtual absl::StatusOr<std::string> ReEncrypt(
      absl::string_view ciphertext) const = 0;

 protected:
  DeterministicCommutativeCipher() = default;
};

// Creates a new DeterminsticCommutativeCipher. It is NOT thread-safe.
absl::StatusOr<std::unique_ptr<DeterministicCommutativeCipher>>
NewDeterministicCommutativeCipher(const ::crypto::tink::util::SecretData& key);

}  // namespace wfa::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_DETERMINISTIC_COMMUTATIVE_CIPHER_H_
