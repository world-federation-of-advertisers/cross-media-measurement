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

#include "wfa/panelmatch/protocol/crypto/deterministic_commutative_encryption_utility.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common_cpp/macros/macros.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/deterministic_commutative_cipher.h"
#include "wfa/panelmatch/common/crypto/ec_commutative_cipher_key_generator.h"
#include "wfa/panelmatch/common/crypto/key_loader.h"

namespace wfa::panelmatch::protocol::crypto {
namespace {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::EcCommutativeCipherKeyGenerator;
using ::wfa::panelmatch::common::crypto::LoadKey;
using ::wfa::panelmatch::common::crypto::NewDeterministicCommutativeCipher;
}  // namespace

absl::StatusOr<CryptorGenerateKeyResponse> DeterministicCommutativeGenerateKey(
    const CryptorGenerateKeyRequest& request) {
  EcCommutativeCipherKeyGenerator generator;
  ASSIGN_OR_RETURN(SecretData key, generator.GenerateKey());
  CryptorGenerateKeyResponse response;
  response.set_key(std::string(SecretDataAsStringView(key)).data());
  return response;
}

absl::StatusOr<CryptorEncryptResponse> DeterministicCommutativeEncrypt(
    const CryptorEncryptRequest& request) {
  CryptorEncryptResponse response;

  ASSIGN_OR_RETURN(SecretData key, LoadKey(request.encryption_key()));
  ASSIGN_OR_RETURN(auto cipher, NewDeterministicCommutativeCipher(key));

  for (absl::string_view plaintext : request.plaintexts()) {
    ASSIGN_OR_RETURN(*response.add_ciphertexts(), cipher->Encrypt(plaintext));
  }

  return response;
}

absl::StatusOr<CryptorDecryptResponse> DeterministicCommutativeDecrypt(
    const CryptorDecryptRequest& request) {
  CryptorDecryptResponse response;

  ASSIGN_OR_RETURN(SecretData key, LoadKey(request.encryption_key()));
  ASSIGN_OR_RETURN(auto cipher, NewDeterministicCommutativeCipher(key));

  for (absl::string_view ciphertext : request.ciphertexts()) {
    ASSIGN_OR_RETURN(*response.add_decrypted_texts(),
                     cipher->Decrypt(ciphertext));
  }

  return response;
}

absl::StatusOr<CryptorReEncryptResponse> DeterministicCommutativeReEncrypt(
    const CryptorReEncryptRequest& request) {
  CryptorReEncryptResponse response;

  ASSIGN_OR_RETURN(SecretData key, LoadKey(request.encryption_key()));
  ASSIGN_OR_RETURN(auto cipher, NewDeterministicCommutativeCipher(key));

  for (absl::string_view ciphertext : request.ciphertexts()) {
    ASSIGN_OR_RETURN(*response.add_ciphertexts(),
                     cipher->ReEncrypt(ciphertext));
  }

  return response;
}

}  // namespace wfa::panelmatch::protocol::crypto
