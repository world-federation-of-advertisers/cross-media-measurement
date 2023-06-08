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

#include "wfa/panelmatch/common/crypto/hkdf.h"

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"
#include "tink/util/status.h"
#include "tink/util/statusor.h"
#include "tink/util/test_util.h"

namespace wfa::panelmatch::common::crypto {
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
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  EXPECT_THAT(hkdf->ComputeHkdf(GetInputKeyMaterial(), 42,
                                SecretDataFromStringView("test-salt"))
                  .status(),
              IsOk());
}

// Test with an empty key and proper length
TEST(HkdfTest, emptyKey) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  auto result = hkdf->ComputeHkdf(SecretDataFromStringView(""), 42,
                                  SecretDataFromStringView("test-salt"));
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with a proper key and length < 1
TEST(HkdfTest, lengthTooSmall) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  auto result = hkdf->ComputeHkdf(GetInputKeyMaterial(), -6,
                                  SecretDataFromStringView("test-salt"));
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

// Test with null key and length > 1024
TEST(HkdfTest, lengthTooBig) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  auto result = hkdf->ComputeHkdf(GetInputKeyMaterial(), 10000,
                                  SecretDataFromStringView("test-salt"));
  EXPECT_THAT(result.status(),
              wfa::StatusIs(absl::StatusCode::kInvalidArgument, ""));
}

TEST(HkdfTest, compareSalt) {
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  auto result1 = hkdf->ComputeHkdf(GetInputKeyMaterial(), 8,
                                   SecretDataFromStringView("test-salt1"));
  auto result2 = hkdf->ComputeHkdf(GetInputKeyMaterial(), 8,
                                   SecretDataFromStringView("test-salt2"));
  EXPECT_NE(result1, result2);
}

}  // namespace

}  // namespace wfa::panelmatch::common::crypto
