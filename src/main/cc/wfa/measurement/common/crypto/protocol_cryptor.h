// Copyright 2020 The Measurement System Authors
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

#ifndef WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_
#define WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_

#include <memory>

#include "absl/strings/string_view.h"
#include "util/statusor.h"

namespace wfa::measurement::common::crypto {

// TODO: use absl::StatusOr.
using ::private_join_and_compute::StatusOr;

// Each ElGamalCiphertext is a two tuple (u, e), where u=g^r and e=m*y^r.
using ElGamalCiphertext = std::pair<std::string, std::string>;

// A cryptor dealing with basic operations in various MPC protocols.
class ProtocolCryptor {
 public:
  virtual ~ProtocolCryptor() = default;

  ProtocolCryptor(ProtocolCryptor&& other) = delete;
  ProtocolCryptor& operator=(ProtocolCryptor&& other) = delete;
  ProtocolCryptor(const ProtocolCryptor&) = delete;
  ProtocolCryptor& operator=(const ProtocolCryptor&) = delete;

  // Blinds a ciphertext, i.e., decrypts one layer of ElGamal encryption and
  // encrypts another layer of deterministic Pohlig Hellman encryption.
  virtual StatusOr<ElGamalCiphertext> Blind(
      const ElGamalCiphertext& ciphertext) = 0;
  // Decrypts one layer of ElGamal encryption.
  virtual StatusOr<std::string> Decrypt(
      const ElGamalCiphertext& ciphertext) = 0;
  // ReRandomizes the ciphertext by adding an encrypted Zero to it.
  virtual StatusOr<ElGamalCiphertext> ReRandomize(
      const ElGamalCiphertext& ciphertext) = 0;

 protected:
  ProtocolCryptor() = default;
};

// Create a ProtocolCryptor using keys required for internal ciphers.
StatusOr<std::unique_ptr<ProtocolCryptor>> CreateProtocolCryptorWithKeys(
    int curve_id, const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key);

}  // namespace wfa::measurement::common::crypto

#endif  // WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_
