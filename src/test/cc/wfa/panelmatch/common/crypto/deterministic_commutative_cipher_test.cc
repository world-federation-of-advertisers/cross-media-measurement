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

#include "absl/base/port.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/ec_commutative_cipher_key_generator.h"

namespace wfa::panelmatch::common::crypto {
namespace {
using ::crypto::tink::util::SecretDataFromStringView;
using ::private_join_and_compute::ECCommutativeCipher;
using ::testing::ContainerEq;
using ::testing::Not;

TEST(DeterministicCommutativeCipherTest, RoundTrip) {
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<DeterministicCommutativeCipher> cipher1,
                       NewDeterministicCommutativeCipher(
                           SecretDataFromStringView("random-key-1")));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<DeterministicCommutativeCipher> cipher2,
                       NewDeterministicCommutativeCipher(
                           SecretDataFromStringView("random-key-2")));

  std::string plaintext = "some-plaintext";
  ASSERT_OK_AND_ASSIGN(std::string encrypted_with_cipher1,
                       cipher1->Encrypt(plaintext));
  EXPECT_NE(encrypted_with_cipher1, plaintext);

  ASSERT_OK_AND_ASSIGN(std::string encrypted_with_cipher2,
                       cipher2->Encrypt(plaintext));
  EXPECT_NE(encrypted_with_cipher2, encrypted_with_cipher1);

  ASSERT_OK_AND_ASSIGN(std::string encrypted_with_cipher1_then_cipher2,
                       cipher2->ReEncrypt(encrypted_with_cipher1));
  ASSERT_OK_AND_ASSIGN(std::string encrypted_with_cipher2_then_cipher1,
                       cipher1->ReEncrypt(encrypted_with_cipher2));
  EXPECT_EQ(encrypted_with_cipher1_then_cipher2,
            encrypted_with_cipher2_then_cipher1);

  EXPECT_THAT(cipher1->Decrypt(encrypted_with_cipher1_then_cipher2),
              IsOkAndHolds(encrypted_with_cipher2));

  EXPECT_THAT(cipher2->Decrypt(encrypted_with_cipher1_then_cipher2),
              IsOkAndHolds(encrypted_with_cipher1));
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
