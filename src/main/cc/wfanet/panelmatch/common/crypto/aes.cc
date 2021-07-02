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

#include "wfanet/panelmatch/common/crypto/aes.h"

#include "absl/status/status.h"

namespace wfanet::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;

// Implements an Aes SIV encryption scheme defined in
// https://datatracker.ietf.org/doc/html/rfc5297
class AesSiv : public Aes {
 public:
  AesSiv() = default;

  absl::StatusOr<std::string> Encrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::UnimplementedError("Not implemented");
  }

  absl::StatusOr<std::string> Decrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::UnimplementedError("Not implemented");
  }
};
}  // namespace

const Aes& GetAesSivCmac512() {
  static const auto* const aes = new AesSiv();
  return *aes;
}
}  // namespace wfanet::panelmatch::common::crypto
