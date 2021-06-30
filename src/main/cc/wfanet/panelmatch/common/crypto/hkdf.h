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

#ifndef SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_HKDF_H_
#define SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_HKDF_H_

#include "absl/status/statusor.h"
#include "tink/util/secret_data.h"

namespace wfanet::panelmatch::common::crypto {
// Implements HMAC-based Extract-and-Expand Key Derivation Function (HKDF)
// from RFC5869 (https://tools.ietf.org/html/rfc5869)
class Hkdf {
 public:
  virtual ~Hkdf() = default;
  // Generates an encryption key from a SecretData input
  virtual absl::StatusOr<::crypto::tink::util::SecretData> ComputeHkdf(
      const ::crypto::tink::util::SecretData& ikm, int length) const = 0;
};

// Returns an Hkdf that uses SHA-256 as the hash function
const Hkdf& GetSha256Hkdf();

}  // namespace wfanet::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_HKDF_H_
