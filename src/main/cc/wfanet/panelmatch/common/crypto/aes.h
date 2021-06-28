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

#ifndef SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_AES_H_
#define SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_AES_H_

namespace wfanet::panelmatch::common::crypto {

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "tink/util/secret_data.h"

// Implements an AES encryption scheme
class Aes {
 public:
  virtual ~Aes() = default;
  // Encrypts `input` using AES key `key`
  // This will return an error status if the key is the wrong size for the
  // given AES implementation
  virtual absl::StatusOr<std::string> Encrypt(
      absl::string_view input, const crypto::tink::util::SecretData key) = 0;
  // Decrypts `input` using AES key `key`
  // This will return an error status if the key is the wrong size for the
  // given AES implementation
  virtual absl::StatusOr<std::string> Decrypt(
      absl::string_view input, const crypto::tink::util::SecretData key) = 0;
};

}  // namespace wfanet::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_AES_H_
