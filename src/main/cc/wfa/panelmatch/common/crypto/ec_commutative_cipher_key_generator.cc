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

#include "wfa/panelmatch/common/crypto/ec_commutative_cipher_key_generator.h"

#include <memory>

#include "absl/status/statusor.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;
using ::private_join_and_compute::ECCommutativeCipher;

absl::StatusOr<SecretData> EcCommutativeCipherKeyGenerator::GenerateKey()
    const {
  ASSIGN_OR_RETURN(
      std::unique_ptr<ECCommutativeCipher> cipher,
      ECCommutativeCipher::CreateWithNewKey(
          NID_X9_62_prime256v1, ECCommutativeCipher::HashType::SHA256));

  return SecretDataFromStringView(cipher->GetPrivateKeyBytes());
}

}  // namespace wfa::panelmatch::common::crypto
