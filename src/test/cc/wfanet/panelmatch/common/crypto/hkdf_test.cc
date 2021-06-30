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

#include "wfanet/panelmatch/common/crypto/hkdf.h"

#include "gtest/gtest.h"
#include "src/test/cc/testutil/matchers.h"
#include "src/test/cc/testutil/status_macros.h"
#include "tink/util/secret_data.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"
#include "tink/util/test_util.h"

namespace wfanet::panelmatch::common::crypto {
namespace {
using ::crypto::tink::test::HexDecodeOrDie;
using ::crypto::tink::test::HexEncode;
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;

SecretData GetInputKeyMaterial() {
  return SecretDataFromStringView(
      HexDecodeOrDie("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b"));
}

// Test with a proper SecretData value and length value
// Test values come from
// https://datatracker.ietf.org/doc/html/rfc5869#appendix-A.3
TEST(HkdfTest, properCall) {
  const Hkdf& hkdf = GetSha256Hkdf();
  ASSERT_OK_AND_ASSIGN(SecretData result,
                       hkdf.ComputeHkdf(GetInputKeyMaterial(), 42));
  EXPECT_EQ(HexEncode(SecretDataAsStringView(result)),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d"
            "201395faa4b61a96c8");
}

// Test with an empty key and proper length
TEST(HkdfTest, emptyKey) {
  const Hkdf& hkdf = GetSha256Hkdf();
  auto result = hkdf.ComputeHkdf(SecretDataFromStringView(""), 42);
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with a proper key and length < 1
TEST(HkdfTest, lengthTooSmall) {
  const Hkdf& hkdf = GetSha256Hkdf();
  auto result = hkdf.ComputeHkdf(GetInputKeyMaterial(), -6);
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with null key and length > 1024
TEST(HkdfTest, lengthTooBig) {
  const Hkdf& hkdf = GetSha256Hkdf();
  auto result = hkdf.ComputeHkdf(GetInputKeyMaterial(), 10000);
  EXPECT_THAT(result.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, ""));
}
}  // namespace

}  // namespace wfanet::panelmatch::common::crypto
