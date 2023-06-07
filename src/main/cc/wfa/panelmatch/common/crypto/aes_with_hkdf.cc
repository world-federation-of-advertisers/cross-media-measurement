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

#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "common_cpp/macros/macros.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::common::crypto {

using ::crypto::tink::util::SecretData;

AesWithHkdf::AesWithHkdf(std::unique_ptr<Hkdf> hkdf, std::unique_ptr<Aes> aes)
    : hkdf_(std::move(hkdf)), aes_(std::move(aes)) {}

absl::StatusOr<std::string> AesWithHkdf::Encrypt(absl::string_view input,
                                                 const SecretData& key,
                                                 const SecretData& salt) const {
  ASSIGN_OR_RETURN(SecretData result,
                   hkdf_->ComputeHkdf(key, aes_->key_size_bytes(), salt));
  return aes_->Encrypt(input, result);
}

absl::StatusOr<std::string> AesWithHkdf::Decrypt(absl::string_view input,
                                                 const SecretData& key,
                                                 const SecretData& salt) const {
  ASSIGN_OR_RETURN(SecretData result,
                   hkdf_->ComputeHkdf(key, aes_->key_size_bytes(), salt));
  return aes_->Decrypt(input, result);
}

}  // namespace wfa::panelmatch::common::crypto
