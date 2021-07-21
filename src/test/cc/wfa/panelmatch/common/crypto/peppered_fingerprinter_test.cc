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

#include "wfa/panelmatch/common/crypto/peppered_fingerprinter.h"

#include "common_cpp/fingerprinters/fingerprinters.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"

namespace wfa::panelmatch::common::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;

// Test PepperedFingerprinter with Sha256Fingerprinter delegate
// Test values from
// common-cpp/src/test/cc/common_cpp/fingerprinters/fingerprinters_test.cc
TEST(FingerprintersTest, PepperedFingerprinterWithSha256) {
  const Fingerprinter& sha = GetSha256Fingerprinter();
  uint64_t compare = 0x141cfc9842c4b0e3;

  // Test that correct hash value is given
  SecretData pepper = SecretDataFromStringView("");
  std::unique_ptr<Fingerprinter> empty_fingerprinter =
      GetPepperedFingerprinter(&sha, pepper);
  EXPECT_EQ(empty_fingerprinter->Fingerprint(""), compare);

  // Verify that different strings give different results
  pepper = SecretDataFromStringView("-str");
  std::unique_ptr<Fingerprinter> fingerprinter =
      GetPepperedFingerprinter(&sha, pepper);
  EXPECT_NE(fingerprinter->Fingerprint("a-dfferent"), compare);
}

// Test PepperedFingerprinter with FarmHashFingerprinter delegate
// Test values from
// common-cpp/src/test/cc/common_cpp/fingerprinters/fingerprinters_test.cc
TEST(FingerprintersTest, PepperedFingerprinterWithFarm) {
  const Fingerprinter& farm = GetFarmFingerprinter();
  uint64_t compare = 0x9ae16a3b2f90404f;

  // Test that correct hash value is given
  SecretData pepper = SecretDataFromStringView("");
  std::unique_ptr<Fingerprinter> empty_fingerprinter =
      GetPepperedFingerprinter(&farm, pepper);
  EXPECT_EQ(empty_fingerprinter->Fingerprint(""), compare);

  // Verify that different strings give different results
  pepper = SecretDataFromStringView("-str");
  std::unique_ptr<Fingerprinter> fingerprinter =
      GetPepperedFingerprinter(&farm, pepper);
  EXPECT_NE(fingerprinter->Fingerprint("not-an-empty"), compare);
}

// Test PepperedFingerprinter with null delegate
TEST(FingerprintersDeathTest, NullFingerprinter) {
  SecretData pepper = SecretDataFromStringView("");
  ASSERT_DEATH(GetPepperedFingerprinter(nullptr, pepper), "");
}

}  // namespace
}  // namespace wfa::panelmatch::common::crypto
