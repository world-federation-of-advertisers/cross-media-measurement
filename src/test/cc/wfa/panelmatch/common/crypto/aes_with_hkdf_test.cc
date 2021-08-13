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

#include "absl/strings/escaping.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;

// Tests that a value ciphertext then decrypted returns that original value
TEST(AesWithHkdfTest, testEncryptDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext = "Some data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext,
      aes_hkdf.Encrypt(plaintext, key, SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(
      std::string recovered_plaintext,
      aes_hkdf.Decrypt(ciphertext, key, SecretDataFromStringView("test-salt")));
  EXPECT_EQ(recovered_plaintext, plaintext);
}

// Tests that the result of AesWithHkdf Encrypt is the same as the result
// of the functions called within it
TEST(AesWithHkdfTest, compareEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(
      SecretData result,
      hkdf->ComputeHkdf(key, aes->key_size_bytes(),
                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_other,
                       aes->Encrypt(plaintext, result));
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext_this,
      aes_hkdf.Encrypt(plaintext, key, SecretDataFromStringView("test-salt")));
  EXPECT_EQ(ciphertext_this, ciphertext_other);
}

// Tests that the result of AesWithHkdf Decrypt is the same as the result
// of the functions called within it
TEST(AesWithHkdfTest, compareDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext = "Some data to encrypt.";
  ASSERT_OK_AND_ASSIGN(
      SecretData result,
      hkdf->ComputeHkdf(key, aes->key_size_bytes(),
                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_other,
                       aes->Encrypt(plaintext, result));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_other,
                       aes->Decrypt(ciphertext_other, result));
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext_this,
      aes_hkdf.Encrypt(plaintext, key, SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_this,
                       aes_hkdf.Decrypt(ciphertext_this, key,
                                        SecretDataFromStringView("test-salt")));
  EXPECT_EQ(decrypted_this, decrypted_other);
}

// Test with empty key and proper input - Encrypt
TEST(AesWithHkdfTest, emptyKeyEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Encrypt("input", SecretDataFromStringView(""),
                                 SecretDataFromStringView("test-salt"));
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with empty key and proper input - Decrypt
TEST(AesWithHkdfTest, emptyKeyDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Decrypt("input", SecretDataFromStringView(""),
                                 SecretDataFromStringView("test-salt"));
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that different keys with the same string return different values for
// Encrypt
TEST(AesTest, differentKeySameStringEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key_1 = SecretDataFromStringView("key1");
  SecretData key_2 = SecretDataFromStringView("key2");
  ASSERT_NE(key_1, key_2);
  std::string_view plaintext = "Some data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes_hkdf.Encrypt(plaintext, key_1,
                                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes_hkdf.Encrypt(plaintext, key_2,
                                        SecretDataFromStringView("test-salt")));
  EXPECT_NE(ciphertext_1, ciphertext_2);
}

// Tests that decrypting with a different key than encryption gives an error
TEST(AesTest, differentKeyDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key_1 = SecretDataFromStringView("key1");
  SecretData key_2 = SecretDataFromStringView("key2");
  ASSERT_NE(key_1, key_2);
  std::string_view plaintext = "Some data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext,
                       aes_hkdf.Encrypt(plaintext, key_1,
                                        SecretDataFromStringView("test-salt")));
  auto decrypted = aes_hkdf.Decrypt(ciphertext, key_2,
                                    SecretDataFromStringView("test-salt"));
  EXPECT_THAT(decrypted.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Tests that the same key with different strings return different values for
// Encrypt
TEST(AesTest, sameKeyDifferentStringEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext_1 = "Some data to encrypt.";
  std::string_view plaintext_2 = "Additional data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes_hkdf.Encrypt(plaintext_1, key,
                                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes_hkdf.Encrypt(plaintext_2, key,
                                        SecretDataFromStringView("test-salt")));
  EXPECT_NE(ciphertext_1, ciphertext_2);
}

// Tests that the same key with different strings return different values for
// Decrypt
TEST(AesTest, sameKeyDifferentStringDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext_1 = "Some data to encrypt.";
  std::string_view plaintext_2 = "Additional data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_1,
                       aes_hkdf.Encrypt(plaintext_1, key,
                                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string ciphertext_2,
                       aes_hkdf.Encrypt(plaintext_2, key,
                                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_1,
                       aes_hkdf.Decrypt(ciphertext_1, key,
                                        SecretDataFromStringView("test-salt")));
  ASSERT_OK_AND_ASSIGN(std::string decrypted_2,
                       aes_hkdf.Decrypt(ciphertext_2, key,
                                        SecretDataFromStringView("test-salt")));
  EXPECT_NE(decrypted_1, decrypted_2);
}

// Tests that the same key and string with different salts return different
// values for Encrypt
TEST(AesTest, differentSalt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  SecretData key = SecretDataFromStringView("key");
  std::string_view plaintext = "Some data to encrypt.";
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext_1,
      aes_hkdf.Encrypt(plaintext, key, SecretDataFromStringView("test-salt1")));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext_2,
      aes_hkdf.Encrypt(plaintext, key, SecretDataFromStringView("test-salt2")));
  EXPECT_NE(ciphertext_1, ciphertext_2);
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
