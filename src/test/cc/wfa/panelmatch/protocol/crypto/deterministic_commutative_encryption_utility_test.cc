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

#include <google/protobuf/repeated_field.h>

#include <string>

#include "absl/base/port.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "wfa/panelmatch/protocol/crypto/deterministic_commutative_cryptor.pb.h"

namespace wfa::panelmatch {
namespace {
using ::google::protobuf::RepeatedPtrField;
using ::testing::ContainerEq;
using ::testing::Eq;
using ::testing::Ne;
using ::testing::Not;
using ::testing::Pointwise;
using ::wfa::panelmatch::protocol::CryptorDecryptRequest;
using ::wfa::panelmatch::protocol::CryptorDecryptResponse;
using ::wfa::panelmatch::protocol::CryptorEncryptRequest;
using ::wfa::panelmatch::protocol::CryptorEncryptResponse;
using ::wfa::panelmatch::protocol::CryptorGenerateKeyRequest;
using ::wfa::panelmatch::protocol::CryptorGenerateKeyResponse;
using ::wfa::panelmatch::protocol::CryptorReEncryptRequest;
using ::wfa::panelmatch::protocol::CryptorReEncryptResponse;
using ::wfa::panelmatch::protocol::crypto::DeterministicCommutativeDecrypt;
using ::wfa::panelmatch::protocol::crypto::DeterministicCommutativeEncrypt;
using ::wfa::panelmatch::protocol::crypto::DeterministicCommutativeGenerateKey;
using ::wfa::panelmatch::protocol::crypto::DeterministicCommutativeReEncrypt;

TEST(PanelMatchTest, DeterministicCommutativeEncryptionUtility) {
  std::vector<std::string> plaintexts{"some plaintext0", "some plaintext1",
                                      "some plaintext2", "some plaintext3",
                                      "some plaintext4"};
  RepeatedPtrField<std::string> plaintext_batch(plaintexts.begin(),
                                                plaintexts.end());
  CryptorGenerateKeyRequest generate_key_request1;
  ASSERT_OK_AND_ASSIGN(
      CryptorGenerateKeyResponse generate_key_response1,
      DeterministicCommutativeGenerateKey(generate_key_request1));
  std::string random_key_1 = generate_key_response1.key();

  CryptorGenerateKeyRequest generate_key_request2;
  ASSERT_OK_AND_ASSIGN(
      CryptorGenerateKeyResponse generate_key_response2,
      DeterministicCommutativeGenerateKey(generate_key_request2));
  std::string random_key_2 = generate_key_response2.key();

  CryptorEncryptRequest encrypt_request1;
  encrypt_request1.set_encryption_key(random_key_1);
  encrypt_request1.mutable_plaintexts()->CopyFrom(plaintext_batch);
  ASSERT_OK_AND_ASSIGN(CryptorEncryptResponse encrypted_response1,
                       DeterministicCommutativeEncrypt(encrypt_request1));
  const auto& encrypted_texts1 = encrypted_response1.ciphertexts();

  CryptorEncryptRequest encrypt_request2;
  encrypt_request2.set_encryption_key(random_key_2);
  encrypt_request2.mutable_plaintexts()->CopyFrom(plaintext_batch);
  ASSERT_OK_AND_ASSIGN(CryptorEncryptResponse encrypted_response2,
                       DeterministicCommutativeEncrypt(encrypt_request2));
  const auto& encrypted_texts2 = encrypted_response2.ciphertexts();
  EXPECT_THAT(encrypted_texts2, Pointwise(Ne(), encrypted_texts1));

  CryptorReEncryptRequest reencrypt_request1;
  reencrypt_request1.set_encryption_key(random_key_1);
  reencrypt_request1.mutable_ciphertexts()->CopyFrom(encrypted_texts2);
  ASSERT_OK_AND_ASSIGN(CryptorReEncryptResponse double_encrypted_response1,
                       DeterministicCommutativeReEncrypt(reencrypt_request1));
  const auto& double_encrypted_texts1 =
      double_encrypted_response1.ciphertexts();
  EXPECT_THAT(encrypted_texts2, Pointwise(Ne(), double_encrypted_texts1));

  CryptorReEncryptRequest reencrypt_request2;
  reencrypt_request2.set_encryption_key(random_key_2);
  reencrypt_request2.mutable_ciphertexts()->CopyFrom(encrypted_texts1);
  ASSERT_OK_AND_ASSIGN(CryptorReEncryptResponse double_encrypted_response2,
                       DeterministicCommutativeReEncrypt(reencrypt_request2));
  const auto& double_encrypted_texts2 =
      double_encrypted_response2.ciphertexts();
  EXPECT_THAT(encrypted_texts1, Pointwise(Ne(), double_encrypted_texts2));

  CryptorDecryptRequest decrypt_request1;
  decrypt_request1.set_encryption_key(random_key_1);
  decrypt_request1.mutable_ciphertexts()->CopyFrom(double_encrypted_texts1);
  ASSERT_OK_AND_ASSIGN(CryptorDecryptResponse decrypted_response1,
                       DeterministicCommutativeDecrypt(decrypt_request1));
  const auto& decrypted_texts1 = decrypted_response1.decrypted_texts();
  EXPECT_THAT(decrypted_texts1, Pointwise(Eq(), encrypted_texts2));

  CryptorDecryptRequest decrypt_request2;
  decrypt_request2.set_encryption_key(random_key_1);
  decrypt_request2.mutable_ciphertexts()->CopyFrom(double_encrypted_texts2);
  ASSERT_OK_AND_ASSIGN(CryptorDecryptResponse decrypted_response2,
                       DeterministicCommutativeDecrypt(decrypt_request2));
  const auto& decrypted_texts2 = decrypted_response2.decrypted_texts();
  EXPECT_THAT(decrypted_texts2, Pointwise(Eq(), encrypted_texts2));

  CryptorDecryptRequest decrypt_request3;
  decrypt_request3.set_encryption_key(random_key_2);
  decrypt_request3.mutable_ciphertexts()->CopyFrom(double_encrypted_texts1);
  ASSERT_OK_AND_ASSIGN(CryptorDecryptResponse decrypted_response3,
                       DeterministicCommutativeDecrypt(decrypt_request3));
  const auto& decrypted_texts3 = decrypted_response3.decrypted_texts();
  EXPECT_THAT(decrypted_texts3, Pointwise(Eq(), encrypted_texts1));

  CryptorDecryptRequest decrypt_request4;
  decrypt_request4.set_encryption_key(random_key_2);
  decrypt_request4.mutable_ciphertexts()->CopyFrom(double_encrypted_texts2);
  ASSERT_OK_AND_ASSIGN(CryptorDecryptResponse decrypted_response4,
                       DeterministicCommutativeDecrypt(decrypt_request4));
  const auto& decrypted_texts4 = decrypted_response4.decrypted_texts();
  EXPECT_THAT(decrypted_texts4, Pointwise(Eq(), encrypted_texts1));
}

}  // namespace
}  // namespace wfa::panelmatch
