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

#include "wfanet/panelmatch/common/crypto/hkdf.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "tink/subtle/hkdf.h"

namespace wfanet::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;

// Implements HMAC-based Extract-and-Expand Key Derivation Function (HKDF)
// from RFC5869 (https://tools.ietf.org/html/rfc5869)
class Sha256Hkdf : public Hkdf {
 public:
  Sha256Hkdf() = default;

  // Generates an encryption key from a SecretData input
  absl::StatusOr<SecretData> ComputeHkdf(const SecretData& ikm,
                                         int length) const override {
    if (length < 1 || length > 1024) {
      return absl::InvalidArgumentError(
          "Invalid length: Must be between 1 and 1024");
    } else if (ikm.empty()) {
      return absl::InvalidArgumentError("Invalid ikm: Must not be empty");
    }

    return ::crypto::tink::subtle::Hkdf::ComputeHkdf(
        ::crypto::tink::subtle::SHA256, ikm, "", "", length);
  }
};
}  // namespace

const Hkdf& GetSha256Hkdf() {
  static const auto* const hkdf = new Sha256Hkdf();
  return *hkdf;
}

}  // namespace wfanet::panelmatch::common::crypto
