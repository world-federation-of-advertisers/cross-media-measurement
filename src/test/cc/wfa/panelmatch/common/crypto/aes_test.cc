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

#include "wfa/panelmatch/common/crypto/aes.h"

#include <string>

#include "absl/strings/escaping.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "tink/subtle/aes_siv_boringssl.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::subtle::AesSivBoringSsl;
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;

// Test key used in `tink/cc/subtle/aes_siv_boringssl_test.cc`.
// The first 32 bytes come from RFC3394:
// https://datatracker.ietf.org/doc/html/rfc3394#section-4.3 The last 32 bytes
// is an arbitrary byte pattern.
SecretData GetTestKey() {
  return SecretDataFromStringView(absl::HexStringToBytes(
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
      "00112233445566778899aabbccddeefff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"));
}

// Tests that a value encrypted then decrypted returns that original value
// Key values for these tests are found at
// tink/cc/subtle/aes_siv_boringssl_test.cc
TEST(AesTest, testEncryptDecrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = GetTestKey();
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext, aes->Encrypt(plaintext, key));
  ASSERT_OK_AND_ASSIGN(std::string recovered_plaintext,
                       aes->Decrypt(ciphertext, key));
  EXPECT_EQ(recovered_plaintext, plaintext);
}

// Tests that AesSiv Encrypt returns the same value as AesSivBoringSsl
// EncryptDeterministically
TEST(AesTest, compareEncrypt) {
  std::unique_ptr<Aes> aes_this = GetAesSivCmac512();
  SecretData key = GetTestKey();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<::crypto::tink::DeterministicAead> aes_other,
      AesSivBoringSsl::New(key));
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string result_this,
                       aes_this->Encrypt(plaintext, key));
  ASSERT_OK_AND_ASSIGN(std::string result_other,
                       (*aes_other).EncryptDeterministically(plaintext, ""));
  EXPECT_EQ(result_this, result_other);
}

// Tests that AesSiv Decrypt returns the same value as AesSivBoringSsl
// DecryptDeterministically
TEST(AesTest, compareDecrypt) {
  std::unique_ptr<Aes> aes_this = GetAesSivCmac512();
  SecretData key = GetTestKey();
  ASSERT_OK_AND_ASSIGN(auto aes_other, AesSivBoringSsl::New(key));
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext,
                       aes_this->Encrypt(plaintext, key));
  ASSERT_OK_AND_ASSIGN(std::string result_this,
                       aes_this->Decrypt(ciphertext, key));
  ASSERT_OK_AND_ASSIGN(auto result_other,
                       aes_other->DecryptDeterministically(ciphertext, ""));
  EXPECT_EQ(result_this, result_other);
}

// Tests that AesSiv Encrypt returns an error with the wrong key size
TEST(AesTest, wrongKeySizeEncrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView(absl::HexStringToBytes("01"));
  auto result = aes->Encrypt("input", key);
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that AesSiv Decrypt returns an error with the wrong key size
TEST(AesTest, wrongKeySizeDecrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = GetTestKey();
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext, aes->Encrypt(plaintext, key));
  key = SecretDataFromStringView(absl::HexStringToBytes("01"));
  auto result = aes->Decrypt(ciphertext, key);
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that AesSiv Encrypt returns an error with an empty key
TEST(AesTest, emptyKeyEncrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView(absl::HexStringToBytes(""));
  auto result = aes->Encrypt("input", key);
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that AesSiv Decrypt returns an error with an empty key
TEST(AesTest, emptyKeyDecrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = GetTestKey();
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext, aes->Encrypt(plaintext, key));
  SecretData empty_key = SecretDataFromStringView(absl::HexStringToBytes(""));
  auto result = aes->Decrypt(ciphertext, empty_key);
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that different keys with the same string return different values
TEST(AesTest, differentKeySameStringEncrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key_1 = SecretDataFromStringView(absl::HexStringToBytes(
      "990102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
      "99112233445566778899aabbccddeefff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"));
  SecretData key_2 = GetTestKey();
  ASSERT_NE(key_1, key_2);
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes->Encrypt(plaintext, key_1));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes->Encrypt(plaintext, key_2));
  EXPECT_NE(ciphertext_1, ciphertext_2);
}

// Tests that decrypting with a different key than encryption gives an error
TEST(AesTest, differentKeyDecrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key_1 = SecretDataFromStringView(absl::HexStringToBytes(
      "990102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
      "99112233445566778899aabbccddeefff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"));
  SecretData key_2 = GetTestKey();
  ASSERT_NE(key_1, key_2);
  std::string plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext, aes->Encrypt(plaintext, key_1));
  auto decrypted = aes->Decrypt(ciphertext, key_2);
  EXPECT_THAT(decrypted.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that the same key with different strings return different values for
// Encrypt
TEST(AesTest, sameKeyDifferentStringEncrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = GetTestKey();
  std::string plaintext_1 = "Some data to encrypt.";
  std::string plaintext_2 = "Additional data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes->Encrypt(plaintext_1, key));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes->Encrypt(plaintext_2, key));
  EXPECT_NE(ciphertext_1, ciphertext_2);
}

// Tests that the same key with different strings return different values for
// Decrypt
TEST(AesTest, sameKeyDifferentStringDecrypt) {
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = GetTestKey();
  std::string plaintext_1 = "Some data to encrypt.";
  std::string plaintext_2 = "Additional data to encrypt.";
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes->Encrypt(plaintext_1, key));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes->Encrypt(plaintext_2, key));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_1,
                       aes->Decrypt(ciphertext_1, key));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_2,
                       aes->Decrypt(ciphertext_2, key));
  EXPECT_NE(decrypted_1, decrypted_2);
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
