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

#include "wfa/panelmatch/common/crypto/deterministic_commutative_cipher.h"

#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "private_join_and_compute/crypto/context.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {

namespace {
using ::private_join_and_compute::ECCommutativeCipher;

class EcCommutativeCipherWrapper : public DeterministicCommutativeCipher {
 public:
  explicit EcCommutativeCipherWrapper(
      std::unique_ptr<ECCommutativeCipher> local_ec_cipher)
      : local_ec_cipher_(std::move(local_ec_cipher)) {}

  ~EcCommutativeCipherWrapper() override = default;
  EcCommutativeCipherWrapper(EcCommutativeCipherWrapper&& other) = delete;
  EcCommutativeCipherWrapper& operator=(EcCommutativeCipherWrapper&& other) =
      delete;
  EcCommutativeCipherWrapper(const EcCommutativeCipherWrapper&) = delete;
  EcCommutativeCipherWrapper& operator=(const EcCommutativeCipherWrapper&) =
      delete;

  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const override {
    return local_ec_cipher_->Encrypt(plaintext);
  }

  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const override {
    return local_ec_cipher_->Decrypt(ciphertext);
  }

  absl::StatusOr<std::string> ReEncrypt(
      absl::string_view encrypted_string) const override {
    return local_ec_cipher_->ReEncrypt(encrypted_string);
  }

 private:
  const std::unique_ptr<ECCommutativeCipher> local_ec_cipher_;
};
}  // namespace

absl::StatusOr<std::unique_ptr<DeterministicCommutativeCipher>>
NewDeterministicCommutativeCipher(const ::crypto::tink::util::SecretData& key) {
  ASSIGN_OR_RETURN(auto local_ec_cipher,
                   ECCommutativeCipher::CreateFromKey(
                       NID_X9_62_prime256v1,
                       ::crypto::tink::util::SecretDataAsStringView(key),
                       ECCommutativeCipher::HashType::SHA256));
  return absl::make_unique<EcCommutativeCipherWrapper>(
      std::move(local_ec_cipher));
}

}  // namespace wfa::panelmatch::common::crypto
