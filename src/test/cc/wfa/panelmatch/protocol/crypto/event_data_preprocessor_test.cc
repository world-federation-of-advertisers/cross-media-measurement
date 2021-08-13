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

#include "common_cpp/fingerprinters/fingerprinters.h"
#include "common_cpp/testing/status_macros.h"
#include "include/gtest/gtest.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/cryptor.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::protocol::crypto {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::panelmatch::common::crypto::Action;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::Cryptor;
using ::wfa::panelmatch::common::crypto::Hkdf;

// Fake Hkdf class for testing purposes only
class FakeHkdf : public Hkdf {
 public:
  FakeHkdf() = default;

  absl::StatusOr<SecretData> ComputeHkdf(
      const SecretData& ikm, int length,
      const SecretData& salt) const override {
    return SecretDataFromStringView(absl::StrCat(
        "HKDF with length '", length, "' of '", SecretDataAsStringView(ikm),
        "' and salt '", SecretDataAsStringView(salt), "' "));
  }
};

// Fake Aes class for testing purposes only
class FakeAes : public Aes {
 public:
  FakeAes() = default;

  absl::StatusOr<std::string> Encrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::StrCat("Encrypted '", input, "' with key '",
                        SecretDataAsStringView(key), "'");
  }

  absl::StatusOr<std::string> Decrypt(absl::string_view input,
                                      const SecretData& key) const override {
    return absl::StrCat("Decrypted '", input, "' with key '",
                        SecretDataAsStringView(key), "'");
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

// Fake Cryptor class for testing purposes only
class FakeCryptor : public Cryptor {
 public:
  FakeCryptor() = default;
  ~FakeCryptor() = default;
  FakeCryptor(FakeCryptor&& other) = delete;
  FakeCryptor& operator=(FakeCryptor&& other) = delete;
  FakeCryptor(const FakeCryptor&) = delete;
  FakeCryptor& operator=(const FakeCryptor&) = delete;

  absl::StatusOr<std::vector<std::string>> BatchProcess(
      const std::vector<std::string>& plaintexts_or_ciphertexts,
      Action action) override {
    return plaintexts_or_ciphertexts;
  }

  absl::StatusOr<google::protobuf::RepeatedPtrField<std::string>> BatchProcess(
      const google::protobuf::RepeatedPtrField<std::string>&
          plaintexts_or_ciphertexts,
      Action action) override {
    return plaintexts_or_ciphertexts;
  }
};

std::unique_ptr<FakeCryptor> CreateFakeCryptor() {
  return absl::make_unique<FakeCryptor>();
}

// Test using fake classes to ensure proper return values
TEST(EventDataPreprocessorTests, properImplementation) {
  const FakeFingerprinter fingerprinter;
  std::unique_ptr<Hkdf> hkdf = absl::make_unique<FakeHkdf>();
  std::unique_ptr<Aes> aes = absl::make_unique<FakeAes>();
  const AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  std::unique_ptr<FakeCryptor> cryptor = CreateFakeCryptor();
  SecretData salt = SecretDataFromStringView("salt");
  EventDataPreprocessor preprocessor(std::move(cryptor),
                                     SecretDataFromStringView("pepper"), salt,
                                     &fingerprinter, &aes_hkdf);
  ASSERT_OK_AND_ASSIGN(ProcessedData processed,
                       preprocessor.Process("some-identifier", "some-event"));
  EXPECT_EQ(processed.encrypted_identifier, 21);
  EXPECT_EQ(processed.encrypted_event_data,
            "Encrypted 'some-event' with key 'HKDF with length '64' of "
            "'some-identifier' and salt 'salt' '");
}

// Tests EventDataPreprocessor with null Fingerprinter
TEST(EventDataPreprocessorTests, nullFingerpritner) {
  std::unique_ptr<Hkdf> hkdf = absl::make_unique<FakeHkdf>();
  std::unique_ptr<Aes> aes = absl::make_unique<FakeAes>();
  const AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  std::unique_ptr<FakeCryptor> cryptor = CreateFakeCryptor();
  ASSERT_DEATH(EventDataPreprocessor preprocessor(
                   std::move(cryptor), SecretDataFromStringView("pepper"),
                   SecretDataFromStringView("salt"), nullptr, &aes_hkdf),
               "");
}

// Tests EventDataPreprocessor with null AesWithHkdf
TEST(EventDataPreprocessorTests, nullAesWithHkdf) {
  const FakeFingerprinter fingerprinter;
  std::unique_ptr<Hkdf> hkdf = absl::make_unique<FakeHkdf>();
  std::unique_ptr<Aes> aes = absl::make_unique<FakeAes>();
  const AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  std::unique_ptr<FakeCryptor> cryptor = CreateFakeCryptor();
  ASSERT_DEATH(EventDataPreprocessor preprocessor(
                   std::move(cryptor), SecretDataFromStringView("pepper"),
                   SecretDataFromStringView("salt"), &fingerprinter, nullptr),
               "");
}

// Test using actual implementations to ensure nothing crashes
TEST(EventDataPreprocessorTests, actualValues) {
  const Fingerprinter& sha = GetSha256Fingerprinter();
  std::unique_ptr<Hkdf> hkdf = common::crypto::GetSha256Hkdf();
  std::unique_ptr<Aes> aes = common::crypto::GetAesSivCmac512();
  const AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Cryptor> cryptor,
                       common::crypto::CreateCryptorWithNewKey());
  EventDataPreprocessor preprocessor(
      std::move(cryptor), SecretDataFromStringView("pepper"),
      SecretDataFromStringView("salt"), &sha, &aes_hkdf);
  ASSERT_OK_AND_ASSIGN(ProcessedData processed,
                       preprocessor.Process("some-identifier", "some-event"));
}

}  // namespace
}  // namespace wfa::panelmatch::protocol::crypto
