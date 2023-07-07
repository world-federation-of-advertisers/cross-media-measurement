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

#ifndef SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_
#define SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"

namespace wfa::measurement::common::crypto {

enum Action {
  kBlind,
  kPartialDecrypt,
  kPartialDecryptAndReRandomize,
  kDecrypt,
  kReRandomize,
  kNoop
};

enum CompositeType { kFull, kPartial };

// A cryptor dealing with basic operations in various MPC protocols.
//
// The implementation of [ProtocolCrytpor] is not thread-safe.
class ProtocolCryptor {
 public:
  virtual ~ProtocolCryptor() = default;

  ProtocolCryptor(ProtocolCryptor&& other) = delete;
  ProtocolCryptor& operator=(ProtocolCryptor&& other) = delete;
  ProtocolCryptor(const ProtocolCryptor&) = delete;
  ProtocolCryptor& operator=(const ProtocolCryptor&) = delete;

  // Blinds a ciphertext, i.e., decrypts one layer of ElGamal encryption and
  // encrypts another layer of deterministic Pohlig Hellman encryption.
  virtual absl::StatusOr<ElGamalCiphertext> Blind(
      const ElGamalCiphertext& ciphertext) = 0;
  // Decrypts one layer of ElGamal encryption.
  virtual absl::StatusOr<std::string> DecryptLocalElGamal(
      const ElGamalCiphertext& ciphertext) = 0;
  // Maps a plaintext onto the curve and then encrypts the EcPoint with the full
  // or partial composite ElGamal Key.
  virtual absl::StatusOr<ElGamalCiphertext> EncryptPlaintextCompositeElGamal(
      absl::string_view plaintext, CompositeType composite_type) = 0;
  // Maps a plaintext onto the curve and then encrypts the EcPoint with the full
  // or partial composite ElGamal Key, returns the result as an
  // ElGamalEcPointPair.
  virtual absl::StatusOr<ElGamalEcPointPair>
  EncryptPlaintextToEcPointsCompositeElGamal(absl::string_view plaintext,
                                             CompositeType composite_type) = 0;
  // Encrypts the plain EcPoint using the full or partial composite ElGamal Key.
  virtual absl::StatusOr<ElGamalCiphertext> EncryptCompositeElGamal(
      absl::string_view plain_ec_point, CompositeType composite_type) = 0;
  // Encrypts the Identity Element using the full or partial composite ElGamal
  // Key, returns the result as an ElGamalEcPointPair.
  virtual absl::StatusOr<ElGamalEcPointPair>
  EncryptIdentityElementToEcPointsCompositeElGamal(
      CompositeType composite_type) = 0;
  // ReRandomizes the ciphertext by adding an encrypted Zero to it. The
  // encryption is done by the full or partial composite ElGamal Cipher.
  virtual absl::StatusOr<ElGamalCiphertext> ReRandomize(
      const ElGamalCiphertext& ciphertext, CompositeType composite_type) = 0;
  // Calculates the SameKeyAggregation destructor using the provided base and
  // key
  virtual absl::StatusOr<ElGamalEcPointPair> CalculateDestructor(
      const ElGamalEcPointPair& base, const ElGamalEcPointPair& key) = 0;
  // Hashes a string to the elliptical curve and return the string
  // representation of the obtained ECPoint.
  virtual absl::StatusOr<std::string> MapToCurve(absl::string_view str) = 0;
  // Hashes an integer to the elliptical curve and return the string
  // representation of the obtained ECPoint.
  virtual absl::StatusOr<std::string> MapToCurve(int64_t x) = 0;
  // Gets the equivalent ECPoint depiction of a ElGamalCiphertext
  virtual absl::StatusOr<ElGamalEcPointPair> ToElGamalEcPoints(
      const ElGamalCiphertext& cipher_text) = 0;
  // Returns the key of the local PohligHellman cipher.
  virtual std::string GetLocalPohligHellmanKey() = 0;
  // Batch processes the ciphertexts in the data, and write the output into
  // the certain position of the result string.
  virtual absl::Status BatchProcess(absl::string_view data,
                                    absl::Span<const Action> actions,
                                    size_t pos, std::string& result) = 0;
  // Returns true if the result of DecryptLocalElGamal() is zero, i.e., Point at
  // infinity.
  virtual absl::StatusOr<bool> IsDecryptLocalElGamalResultZero(
      const ElGamalCiphertext& ciphertext) = 0;
  // Returns a random BigNum.
  virtual private_join_and_compute::BigNum NextRandomBigNum() = 0;
  // Returns a random BigNum as string.
  virtual std::string NextRandomBigNumAsString() = 0;

 protected:
  ProtocolCryptor() = default;
};

struct ProtocolCryptorOptions {
  int curve_id;
  ElGamalCiphertext local_el_gamal_public_key;
  std::string local_el_gamal_private_key;
  std::string local_pohlig_hellman_private_key;
  ElGamalCiphertext composite_el_gamal_public_key;
  ElGamalCiphertext partial_composite_el_gamal_public_key;
};

// Create a ProtocolCryptor using keys required for internal ciphers.
absl::StatusOr<std::unique_ptr<ProtocolCryptor>> CreateProtocolCryptor(
    const ProtocolCryptorOptions& options);

// Create a vector of identical [ProtocolCryptor]s.
//
// [ProtocolCryptor] is not thread-safe due to the underlying crypto library
// thus requires multiple identical [ProtocolCryptor]s to execute in parallel.
// The mutex in [ProtocolCrytpor] is a precaution and will dramatically impact
// the parallelism.
// Per advise from team `private_join_and_compute`, using a vector of
// [ProtocolCryptor] is a suggested way. Note that, ProtocolCryptors are not
// exactly identical. Each of them has a different context that is initialized
// with a different random generator. But they should work consistently
// regardless the minor differences.
absl::StatusOr<std::vector<std::unique_ptr<ProtocolCryptor>>>
CreateIdenticalProtocolCrypors(int num, const ProtocolCryptorOptions& options);

}  // namespace wfa::measurement::common::crypto

#endif  // SRC_MAIN_CC_WFA_MEASUREMENT_COMMON_CRYPTO_PROTOCOL_CRYPTOR_H_
