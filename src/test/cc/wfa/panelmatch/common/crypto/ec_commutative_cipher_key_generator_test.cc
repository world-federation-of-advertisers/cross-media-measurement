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

#include <string>

#include "absl/status/statusor.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::private_join_and_compute::ECCommutativeCipher;

// Make sure each call to GenerateKey returns a unique value
TEST(EcCommutativeCipherKeyGeneratorTest, uniqueValues) {
  EcCommutativeCipherKeyGenerator generator;
  ASSERT_OK_AND_ASSIGN(SecretData key1, generator.GenerateKey());
  ASSERT_OK_AND_ASSIGN(SecretData key2, generator.GenerateKey());
  ASSERT_NE(key1, key2);
}

// Make sure GenerateKey returns a uvalue that can be used to create an
// ECCommutativeCipher
TEST(EcCommutativeCipherKeyGeneratorTest, createEcCommutativeCipher) {
  EcCommutativeCipherKeyGenerator generator;
  ASSERT_OK_AND_ASSIGN(SecretData key, generator.GenerateKey());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<ECCommutativeCipher> cipher,
                       ECCommutativeCipher::CreateFromKey(
                           NID_X9_62_prime256v1, SecretDataAsStringView(key),
                           ECCommutativeCipher::HashType::SHA256));
  ASSERT_NE(cipher, nullptr);
  EXPECT_THAT(cipher->Encrypt("plaintext-test").status(), IsOk());
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
