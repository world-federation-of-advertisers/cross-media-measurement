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

#include "wfanet/panelmatch/common/crypto/aes_with_hkdf.h"

#include "gtest/gtest.h"
#include "src/test/cc/testutil/matchers.h"
#include "src/test/cc/testutil/status_macros.h"

namespace wfanet::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretDataFromStringView;

// Test with proper key and proper input - Encrypt
// Not currently implemented
TEST(AesWithHkdfTest, unimplementedEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Encrypt("input", SecretDataFromStringView("key"));
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kUnimplemented, ""));
}

// Test with proper key and proper input - Encrypt
// Not currently implemented
TEST(AesWithHkdfTest, unimplementedDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Decrypt("input", SecretDataFromStringView("key"));
  EXPECT_THAT(result.status(), StatusIs(absl::StatusCode::kUnimplemented, ""));
}

// Test with empty key and proper input - Encrypt
TEST(AesWithHkdfTest, emptyKeyEncrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Encrypt("input", SecretDataFromStringView(""));
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with empty key and proper input - Decrypt
TEST(AesWithHkdfTest, emptyKeyDecrypt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  auto result = aes_hkdf.Decrypt("input", SecretDataFromStringView(""));
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// TODO(enfreck): Implement the following cases
//  - Encrypt/Decrypt
//  - Null input
//  - Different key with same string are different things
//  - Same key with different strings are different things

}  // namespace
}  // namespace wfanet::panelmatch::common::crypto
