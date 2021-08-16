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

#include <iostream>
#include <memory>
#include <string>

#include "absl/strings/escaping.h"
#include "common_cpp/macros/macros.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/cryptor.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::Action;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::CreateCryptorFromKey;
using ::wfa::panelmatch::common::crypto::Cryptor;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::Hkdf;

// Spot check AesWithHkdf encrypted values from the command line
// Parameters: double-base64-escaped encrypted value, unencrypted identifier,
// cryptokey, hkdf_pepper
int main(int argc, char** argv) {
  if (argc != 5) {
    std::cout << "There must be 4 parameters" << std::endl;
    return 1;
  }

  // TODO(efoxepstein): Look into why this is double-base64-escaped
  std::string temp;
  std::string ciphertext;
  absl::Base64Unescape(argv[1], &temp);
  absl::Base64Unescape(temp, &ciphertext);

  absl::StatusOr<std::unique_ptr<Cryptor>> cryptor =
      CreateCryptorFromKey(argv[3]);
  if (!cryptor.ok()) {
    std::cerr << "Creating a Cryptor failed: " << cryptor.status() << std::endl;
    return 1;
  }

  std::vector<std::string> unencrypted_identifier = {std::string(argv[2])};
  absl::StatusOr<std::vector<std::string>> key =
      (*cryptor)->BatchProcess(unencrypted_identifier, Action::kEncrypt);
  if (!key.ok()) {
    std::cerr << "Creating a key failed: " << key.status() << std::endl;
    return 1;
  }

  if (key->empty()) {
    std::cerr << "Creating a key failed: Empty result" << std::endl;
    return 1;
  }

  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));

  absl::StatusOr<std::string> plaintext =
      aes_hkdf.Decrypt(ciphertext, SecretDataFromStringView((*key)[0]),
                       SecretDataFromStringView(argv[4]));
  if (!plaintext.ok()) {
    std::cerr << "Decryption failed: " << plaintext.status() << std::endl;
    return 1;
  }

  std::cout << "Decrypted value: " << *plaintext << std::endl;
  return 0;
}
