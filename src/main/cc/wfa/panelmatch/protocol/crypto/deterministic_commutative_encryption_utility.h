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

#ifndef SRC_MAIN_CC_WFA_PANELMATCH_PROTOCOL_CRYPTO_DETERMINISTIC_COMMUTATIVE_ENCRYPTION_UTILITY_H_
#define SRC_MAIN_CC_WFA_PANELMATCH_PROTOCOL_CRYPTO_DETERMINISTIC_COMMUTATIVE_ENCRYPTION_UTILITY_H_

#include "absl/status/statusor.h"
#include "wfa/panelmatch/protocol/crypto/deterministic_commutative_cryptor.pb.h"

namespace wfa::panelmatch::protocol::crypto {

absl::StatusOr<CryptorGenerateKeyResponse> DeterministicCommutativeGenerateKey(
    const CryptorGenerateKeyRequest& request);

absl::StatusOr<wfa::panelmatch::protocol::CryptorEncryptResponse>
DeterministicCommutativeEncrypt(
    const wfa::panelmatch::protocol::CryptorEncryptRequest& request);

absl::StatusOr<wfa::panelmatch::protocol::CryptorReEncryptResponse>
DeterministicCommutativeReEncrypt(
    const wfa::panelmatch::protocol::CryptorReEncryptRequest& request);

absl::StatusOr<wfa::panelmatch::protocol::CryptorDecryptResponse>
DeterministicCommutativeDecrypt(
    const wfa::panelmatch::protocol::CryptorDecryptRequest& request);

}  // namespace wfa::panelmatch::protocol::crypto

#endif  // SRC_MAIN_CC_WFA_PANELMATCH_PROTOCOL_CRYPTO_DETERMINISTIC_COMMUTATIVE_ENCRYPTION_UTILITY_H_
