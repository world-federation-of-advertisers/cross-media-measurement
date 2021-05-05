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

#ifndef SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_CRYPTOR_H_
#define SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_CRYPTOR_H_

#include <google/protobuf/repeated_field.h>

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace wfanet::panelmatch::common::crypto {
enum class Action { kEncrypt, kReEncrypt, kDecrypt };
// A cryptor dealing with basic operations needed for panel match
class Cryptor {
 public:
  virtual ~Cryptor() = default;

  Cryptor(Cryptor&& other) = delete;
  Cryptor& operator=(Cryptor&& other) = delete;
  Cryptor(const Cryptor&) = delete;
  Cryptor& operator=(const Cryptor&) = delete;

  virtual absl::StatusOr<std::vector<std::string>> BatchProcess(
      const std::vector<std::string>& plaintexts_or_ciphertexts,
      Action action) = 0;

  virtual absl::StatusOr<google::protobuf::RepeatedPtrField<std::string>>
  BatchProcess(const google::protobuf::RepeatedPtrField<std::string>&
                   plaintexts_or_ciphertexts,
               Action action) = 0;

 protected:
  Cryptor() = default;
};

// Create a Cryptor.
absl::StatusOr<std::unique_ptr<Cryptor>> CreateCryptorWithNewKey(void);
absl::StatusOr<std::unique_ptr<Cryptor>> CreateCryptorFromKey(
    absl::string_view key_bytes);

}  // namespace wfanet::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_CRYPTOR_H_
