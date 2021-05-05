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
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "crypto/context.h"
#include "crypto/ec_commutative_cipher.h"

namespace wfanet::panelmatch::common::crypto {

namespace {
using ::private_join_and_compute::ECCommutativeCipher;

class CryptorImpl : public Cryptor {
 public:
  explicit CryptorImpl(std::unique_ptr<ECCommutativeCipher> local_ec_cipher);
  ~CryptorImpl() override = default;
  CryptorImpl(CryptorImpl&& other) = delete;
  CryptorImpl& operator=(CryptorImpl&& other) = delete;
  CryptorImpl(const CryptorImpl&) = delete;
  CryptorImpl& operator=(const CryptorImpl&) = delete;

  absl::StatusOr<std::vector<std::string>> BatchProcess(
      const std::vector<std::string>& plaintexts_or_ciphertexts,
      Action action) override;
  absl::StatusOr<google::protobuf::RepeatedPtrField<std::string>> BatchProcess(
      const google::protobuf::RepeatedPtrField<std::string>&
          plaintexts_or_ciphertexts,
      Action action) override;

 private:
  absl::StatusOr<std::string> Decrypt(absl::string_view encrypted_string)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return local_ec_cipher_->Decrypt(encrypted_string);
  }

  absl::StatusOr<std::string> Encrypt(absl::string_view plaintext)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return local_ec_cipher_->Encrypt(plaintext);
  }

  absl::StatusOr<std::string> ReEncrypt(absl::string_view encrypted_string)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return local_ec_cipher_->ReEncrypt(encrypted_string);
  }

  absl::StatusOr<std::string> ActionHelper(std::string text, Action action)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    switch (action) {
      case Action::kEncrypt:
        return Encrypt(text);
      case Action::kReEncrypt:
        return ReEncrypt(text);
      case Action::kDecrypt:
        return Decrypt(text);
      default:
        return absl::InvalidArgumentError("Unknown action.");
    }
  }

  const std::unique_ptr<ECCommutativeCipher> local_ec_cipher_
      GUARDED_BY(mutex_);

  // Since the underlying private-join-and-computer::ECCommuativeCipher is NOT
  // thread safe, we use mutex to enforce thread safety in this class.
  absl::Mutex mutex_;  // protects local_ec_cipher_
};

CryptorImpl::CryptorImpl(std::unique_ptr<ECCommutativeCipher> local_ec_cipher)
    : local_ec_cipher_(std::move(local_ec_cipher)) {}

absl::StatusOr<std::vector<std::string>> CryptorImpl::BatchProcess(
    const std::vector<std::string>& plaintexts_or_ciphertexts, Action action)
    LOCKS_EXCLUDED(mutex_) {
  absl::WriterMutexLock l(&mutex_);
  std::vector<std::string> results;
  const int num_texts = plaintexts_or_ciphertexts.size();
  results.reserve(num_texts);

  for (auto& text : plaintexts_or_ciphertexts) {
    ASSIGN_OR_RETURN(auto result, ActionHelper(text, action));
    results.push_back(std::move(result));
  }
  return std::move(results);
}

absl::StatusOr<google::protobuf::RepeatedPtrField<std::string>>
CryptorImpl::BatchProcess(const google::protobuf::RepeatedPtrField<std::string>&
                              plaintexts_or_ciphertexts,
                          Action action) LOCKS_EXCLUDED(mutex_) {
  absl::WriterMutexLock l(&mutex_);
  google::protobuf::RepeatedPtrField<std::string> results;
  const int num_texts = plaintexts_or_ciphertexts.size();
  results.Reserve(num_texts);

  for (auto& text : plaintexts_or_ciphertexts) {
    ASSIGN_OR_RETURN(auto result, ActionHelper(text, action));
    results.Add(std::move(result));
  }
  return std::move(results);
}

}  // namespace

// We probably want to pass in a crypto key in the future.
// This is just a placeholder.
absl::StatusOr<std::unique_ptr<Cryptor>> CreateCryptorWithNewKey(void) {
  ASSIGN_OR_RETURN(
      auto local_ec_cipher,
      ECCommutativeCipher::CreateWithNewKey(
          NID_X9_62_prime256v1, ECCommutativeCipher::HashType::SHA256));
  return absl::make_unique<CryptorImpl>(std::move(local_ec_cipher));
}

absl::StatusOr<std::unique_ptr<Cryptor>> CreateCryptorFromKey(
    absl::string_view key_bytes) {
  ASSIGN_OR_RETURN(auto local_ec_cipher,
                   ECCommutativeCipher::CreateFromKey(
                       NID_X9_62_prime256v1, key_bytes,
                       ECCommutativeCipher::HashType::SHA256));
  return absl::make_unique<CryptorImpl>(std::move(local_ec_cipher));
}

}  // namespace wfanet::panelmatch::common::crypto
