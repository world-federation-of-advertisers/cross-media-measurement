/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_AES_WITH_HKDF_H_
#define SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_AES_WITH_HKDF_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::common::crypto {

// Implements HMAC-based Extract-and-Expand Key Derivation Function (HKDF)
// from RFC5869 and an AES encryption
class AesWithHkdf {
 public:
  AesWithHkdf(std::unique_ptr<Hkdf> hkdf, std::unique_ptr<Aes> aes);
  ~AesWithHkdf() = default;

  // Encrypts `input` with a SecretData `key` using an hkdf to generate an
  // aes key and an aes method to encrypt the input with the aes key
  absl::StatusOr<std::string> Encrypt(
      absl::string_view input, const ::crypto::tink::util::SecretData& key,
      const ::crypto::tink::util::SecretData& salt) const;

  // Decrypts `input` with a SecretData `key` using an hkdf to generate an
  // aes key and an aes method to decrypt the input with the aes key
  absl::StatusOr<std::string> Decrypt(
      absl::string_view input, const ::crypto::tink::util::SecretData& key,
      const ::crypto::tink::util::SecretData& salt) const;

 private:
  std::unique_ptr<Hkdf> hkdf_;
  std::unique_ptr<Aes> aes_;
};

}  // namespace wfa::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFA_PANELMATCH_COMMON_CRYPTO_AES_WITH_HKDF_H_
