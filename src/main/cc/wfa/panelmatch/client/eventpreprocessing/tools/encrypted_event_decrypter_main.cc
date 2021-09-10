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

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common_cpp/macros/macros.h"
#include "glog/logging.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/deterministic_commutative_cipher.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::DeterministicCommutativeCipher;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::NewDeterministicCommutativeCipher;

ABSL_FLAG(std::string, ciphertext, "",
          "ciphertext to decrypt, encoded twice with base64");
ABSL_FLAG(std::string, crypto_key, "", "deterministic commutative crypto key");
ABSL_FLAG(std::string, identifier, "", "event identifier");
ABSL_FLAG(std::string, hkdf_pepper, "", "pepper for HKDF");

namespace {
void CheckFlagNotEmpty(absl::string_view flag_value,
                       absl::string_view flag_name) {
  CHECK(!flag_value.empty()) << "--" << flag_name << " must not be blank";
}

absl::StatusOr<SecretData> MakeEncryptedIdentifier(
    absl::string_view crypto_key, absl::string_view identifier) {
  ASSIGN_OR_RETURN(
      std::unique_ptr<DeterministicCommutativeCipher> cipher,
      NewDeterministicCommutativeCipher(SecretDataFromStringView(crypto_key)));

  ASSIGN_OR_RETURN(std::string key, cipher->Encrypt(identifier));

  return SecretDataFromStringView(key);
}
}  // namespace

// Spot check AesWithHkdf encrypted values from the command line.
// Example usage:
//
// $ bazel run
// //src/main/cc/wfa/panelmatch/client/eventpreprocessing/tools:encrypted_event_decrypter
// \
//     -- --crypto_key=KEY --identifier=test-id \
//     --ciphertext=M01nQkN4Z01IRit4MnBPSkk2Y0xLc0RueWVMVk5kcFhCMW89 \
//     --hkdf_pepper=PEPPER
int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string crypto_key = absl::GetFlag(FLAGS_crypto_key);
  CheckFlagNotEmpty(crypto_key, "crypto_key");

  std::string identifier = absl::GetFlag(FLAGS_identifier);
  CheckFlagNotEmpty(identifier, "identifier");
  absl::StatusOr<SecretData> encrypted_identifier =
      MakeEncryptedIdentifier(crypto_key, identifier);
  CHECK(encrypted_identifier.ok()) << encrypted_identifier.status();

  SecretData hkdf_pepper =
      SecretDataFromStringView(absl::GetFlag(FLAGS_hkdf_pepper));

  std::string ciphertext_base64_twice = absl::GetFlag(FLAGS_ciphertext);
  CheckFlagNotEmpty(ciphertext_base64_twice, "ciphertext");

  // TODO(efoxepstein): Look into why this is double-base64-escaped
  std::string ciphertext_base64;
  CHECK(absl::Base64Unescape(ciphertext_base64_twice, &ciphertext_base64));

  std::string ciphertext;
  CHECK(absl::Base64Unescape(ciphertext_base64, &ciphertext));

  AesWithHkdf aes_with_hkdf(GetSha256Hkdf(), GetAesSivCmac512());
  absl::StatusOr<std::string> plaintext =
      aes_with_hkdf.Decrypt(ciphertext, *encrypted_identifier, hkdf_pepper);
  CHECK(plaintext.ok());
  std::cout << *plaintext << std::endl;

  return 0;
}
