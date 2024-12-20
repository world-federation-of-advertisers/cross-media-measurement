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

#include "wfa/panelmatch/protocol/crypto/event_data_preprocessor.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "common_cpp/fingerprinters/fingerprinters.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/deterministic_commutative_cipher.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::protocol::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::DeterministicCommutativeCipher;
using ::wfa::panelmatch::common::crypto::Hkdf;
using ::wfa::panelmatch::common::crypto::NewDeterministicCommutativeCipher;

// Fake Hkdf class for testing purposes only
class FakeHkdf : public Hkdf {
 public:
  FakeHkdf() = default;

  absl::StatusOr<SecretData> ComputeHkdf(
      const SecretData& ikm, int length,
      const SecretData& salt) const override {
    return SecretDataFromStringView(absl::StrCat(
        "HKDF(ikm='", SecretDataAsStringView(ikm), "', length=", length,
        ", salt='", SecretDataAsStringView(salt), "')"));
  }
};

// Fake Aes class for testing purposes only
class FakeAes : public Aes {
 public:
  FakeAes() = default;

  absl::StatusOr<std::string> Encrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::StrCat("AesEncrypt(input='", input, "', key='",
                        SecretDataAsStringView(key), "')");
  }

  absl::StatusOr<std::string> Decrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::InternalError("Not implemented");
  }

  int32_t key_size_bytes() const override { return 64; }
};

// Fake Fingerprinter class for testing purposes only
class FakeFingerprinter : public Fingerprinter {
 public:
  ~FakeFingerprinter() override = default;
  FakeFingerprinter() = default;

  uint64_t Fingerprint(absl::Span<const unsigned char> item) const override {
    return item.size();
  }
};

// Fake DeterministicCommutativeCipher class for testing purposes only
class FakeCipher : public DeterministicCommutativeCipher {
 public:
  FakeCipher() = default;
  ~FakeCipher() = default;

  FakeCipher(const FakeCipher&) = delete;
  FakeCipher& operator=(const FakeCipher&) = delete;

  absl::StatusOr<std::string> Encrypt(
      absl::string_view plaintext) const override {
    return absl::StrCat("DeterministicCommutativeCipher::Encrypt(", plaintext,
                        ")");
  }

  absl::StatusOr<std::string> Decrypt(
      absl::string_view ciphertext) const override {
    return absl::UnimplementedError("Decrypt is not implemented");
  }

  absl::StatusOr<std::string> ReEncrypt(
      absl::string_view ciphertext) const override {
    return absl::UnimplementedError("ReEncrypt is not implemented");
  }
};

SecretData TestIdentifierHashPepper() {
  return SecretDataFromStringView("identifier-hash-pepper");
}

SecretData TestHkdfPepper() { return SecretDataFromStringView("hkdf-pepper"); }

std::unique_ptr<DeterministicCommutativeCipher> TestCipher() {
  return absl::make_unique<FakeCipher>();
}

Fingerprinter* TestFingerprinter() {
  static FakeFingerprinter* fingerprinter = new FakeFingerprinter;
  return fingerprinter;
}

AesWithHkdf* TestAesWithHkdf() {
  static AesWithHkdf* aes_with_hkdf = new AesWithHkdf(
      absl::make_unique<FakeHkdf>(), absl::make_unique<FakeAes>());
  return aes_with_hkdf;
}

TEST(EventDataPreprocessorTest, Success) {
  EventDataPreprocessor preprocessor(TestCipher(), TestIdentifierHashPepper(),
                                     TestHkdfPepper(), TestFingerprinter(),
                                     TestAesWithHkdf());

  ASSERT_OK_AND_ASSIGN(ProcessedData processed,
                       preprocessor.Process("some-identifier", "some-event"));

  // Expected length = "some-identifier".size()         [15]
  //                 + TestIdentifierHashPepper.size()  [22]
  //                 + length added by FakeCipher       [41]
  EXPECT_EQ(processed.encrypted_identifier, 78);
  EXPECT_EQ(processed.encrypted_event_data,
            "AesEncrypt(input='some-event', "
            "key='HKDF(ikm='DeterministicCommutativeCipher::Encrypt(some-"
            "identifier)', "
            "length=64, salt='hkdf-pepper')')");
}

TEST(EventDataPreprocessorDeathTest, NullFingerprinter) {
  ASSERT_DEATH(
      EventDataPreprocessor(TestCipher(), TestIdentifierHashPepper(),
                            TestHkdfPepper(), nullptr, TestAesWithHkdf()),
      "");
}

TEST(EventDataPreprocessorDeathTest, NullAesWithHkdf) {
  ASSERT_DEATH(
      EventDataPreprocessor(TestCipher(), TestIdentifierHashPepper(),
                            TestHkdfPepper(), TestFingerprinter(), nullptr),
      "");
}

// Test using actual implementations to ensure nothing crashes
TEST(EventDataPreprocessorTest, Integration) {
  const Fingerprinter& fingerprinter = GetSha256Fingerprinter();
  AesWithHkdf aes_hkdf(wfa::panelmatch::common::crypto::GetSha256Hkdf(),
                       wfa::panelmatch::common::crypto::GetAesSivCmac512());
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DeterministicCommutativeCipher> cipher,
      NewDeterministicCommutativeCipher(SecretDataFromStringView("some-key")));
  EventDataPreprocessor preprocessor(
      std::move(cipher), TestIdentifierHashPepper(), TestHkdfPepper(),
      &fingerprinter, &aes_hkdf);
  EXPECT_THAT(preprocessor.Process("some-identifier", "some-event").status(),
              IsOk());
}

}  // namespace
}  // namespace wfa::panelmatch::protocol::crypto
