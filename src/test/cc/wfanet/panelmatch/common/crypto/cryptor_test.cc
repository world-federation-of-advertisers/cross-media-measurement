// Copyright 2020 The Cross-Media Measurement Authors
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

#include "wfanet/panelmatch/common/crypto/cryptor.h"

#include <string>

#include "absl/base/port.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "crypto/ec_commutative_cipher.h"
#include "gtest/gtest.h"
#include "src/test/cc/testutil/matchers.h"
#include "src/test/cc/testutil/status_macros.h"

namespace wfa::panelmatch {
namespace {
using ::private_join_and_compute::ECCommutativeCipher;
using ::testing::ContainerEq;
using ::testing::Not;
using ::wfanet::IsOk;
using ::wfanet::IsOkAndHolds;
using ::wfanet::panelmatch::common::crypto::Action;
using ::wfanet::panelmatch::common::crypto::CreateCryptorFromKey;
using ::wfanet::panelmatch::common::crypto::CreateCryptorWithNewKey;
using ::wfanet::panelmatch::common::crypto::Cryptor;
TEST(PrivateJoinAndComputeTest, EncryptReEncryptDecrypt) {
  ASSERT_OK_AND_ASSIGN(auto cryptor1, CreateCryptorFromKey("random-key-1"));
  ASSERT_OK_AND_ASSIGN(auto cryptor2, CreateCryptorFromKey("random-key-2"));

  std::vector<std::string> plaintext_batch{"some plaintext0", "some plaintext1",
                                           "some plaintext2", "some plaintext3",
                                           "some plaintext4"};
  absl::StatusOr<std::vector<std::string>> encrypted_batch1 =
      cryptor1->BatchProcess(plaintext_batch, Action::kEncrypt);
  ASSERT_THAT(encrypted_batch1, IsOk());
  ASSERT_THAT(plaintext_batch, Not(ContainerEq(*encrypted_batch1)));
  absl::StatusOr<std::vector<std::string>> encrypted_batch2 =
      cryptor2->BatchProcess(plaintext_batch, Action::kEncrypt);
  ASSERT_THAT(encrypted_batch2, IsOk());
  ASSERT_THAT(*encrypted_batch1, Not(ContainerEq(*encrypted_batch2)));

  absl::StatusOr<std::vector<std::string>> double_encrypted_batch1 =
      cryptor1->BatchProcess(*encrypted_batch2, Action::kReEncrypt);
  ASSERT_THAT(double_encrypted_batch1, IsOk());
  ASSERT_THAT(*encrypted_batch2, Not(ContainerEq(*double_encrypted_batch1)));
  absl::StatusOr<std::vector<std::string>> double_encrypted_batch2 =
      cryptor2->BatchProcess(*encrypted_batch1, Action::kReEncrypt);
  ASSERT_THAT(double_encrypted_batch2, IsOk());

  EXPECT_THAT(
      cryptor1->BatchProcess(*double_encrypted_batch1, Action::kDecrypt),
      IsOkAndHolds(*encrypted_batch2));
  ASSERT_THAT(
      *cryptor1->BatchProcess(*double_encrypted_batch1, Action::kDecrypt),
      ContainerEq(*encrypted_batch2));
  EXPECT_THAT(
      cryptor1->BatchProcess(*double_encrypted_batch2, Action::kDecrypt),
      IsOkAndHolds(*encrypted_batch2));

  EXPECT_THAT(
      cryptor2->BatchProcess(*double_encrypted_batch1, Action::kDecrypt),
      IsOkAndHolds(*encrypted_batch1));
  EXPECT_THAT(
      cryptor2->BatchProcess(*double_encrypted_batch2, Action::kDecrypt),
      IsOkAndHolds(*encrypted_batch1));
}

}  // namespace
}  // namespace wfa::panelmatch
