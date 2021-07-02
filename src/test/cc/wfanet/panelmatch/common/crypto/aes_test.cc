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

#include "wfanet/panelmatch/common/crypto/aes.h"

#include <string>

#include "gtest/gtest.h"
#include "src/test/cc/testutil/matchers.h"

namespace wfanet::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretDataFromStringView;

TEST(AesTest, unimplementedEncrypt) {
  const Aes& aes = GetAesSivCmac512();
  auto result = aes.Encrypt("input", SecretDataFromStringView("key"));
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kUnimplemented, ""));
}

TEST(AesTest, unimplementedDecrypt) {
  const Aes& aes = GetAesSivCmac512();
  auto result = aes.Decrypt("input", SecretDataFromStringView("key"));
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kUnimplemented, ""));
}

// TODO(efreck): Implement the following test cases
//  - Encrypt/Decrypt
//  - Compares the output of AesSiv to AesSivBoringSsl
//  - Wrong key size
//  - Null input
//  - Null key
//  - Different key with same string are different things
//  - Same key with different strings are different things

}  // namespace
}  // namespace wfanet::panelmatch::common::crypto
