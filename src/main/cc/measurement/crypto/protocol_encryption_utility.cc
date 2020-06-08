/*
 * Copyright 2020 Google Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "protocol_encryption_utility.h"

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "util/canonical_errors.h"

namespace wfa::measurement::crypto {

namespace {

using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::private_join_and_compute::InternalError;
using ::private_join_and_compute::InvalidArgumentError;
using ::wfa::measurement::internal::duchy::ElGamalKeys;
using ElGamalCipherText = std::pair<std::string, std::string>;

constexpr int kBytesPerEcPoint = 33;
// A ciphertext contains 2 EcPoints.
constexpr int kBytesPerCipherText = kBytesPerEcPoint * 2;
// A register contains 3 ciphertexts, i.e., (index, key, count)
constexpr int kBytesPerCipherRegister = kBytesPerCipherText * 3;

struct CompositeCipher {
  std::unique_ptr<CommutativeElGamal> e_g_cipher;
  std::unique_ptr<ECCommutativeCipher> p_h_cipher;
};

StatusOr<CompositeCipher> CreateCompositeCipher(
    int curve_id, const ElGamalKeys& el_gamal_keys,
    const std::string& pohlig_hellman_sk) {
  CompositeCipher result;
  // Create the ElGamal cipher using the provided keys.
  ASSIGN_OR_RETURN(result.e_g_cipher,
                   CommutativeElGamal::CreateFromPublicAndPrivateKeys(
                       curve_id,
                       std::make_pair(el_gamal_keys.el_gamal_g(),
                                      el_gamal_keys.el_gamal_y()),
                       el_gamal_keys.el_gamal_sk()));
  // Create the Pohlig Hellman cipher using the provided key or a random key if
  // no key is provided.
  ASSIGN_OR_RETURN(result.p_h_cipher,
                   pohlig_hellman_sk.empty()
                       ? ECCommutativeCipher::CreateWithNewKey(
                             curve_id, ECCommutativeCipher::HashType::SHA256)
                       : ECCommutativeCipher::CreateFromKey(
                             curve_id, pohlig_hellman_sk,
                             ECCommutativeCipher::HashType::SHA256));
  return result;
}

template <typename T>
std::pair<std::string, std::string> ExtractCipherTextFromSubstring(T it) {
  return std::make_pair(
      std::string(it, it + kBytesPerEcPoint),
      std::string(it + kBytesPerEcPoint, it + kBytesPerEcPoint * 2));
}

}  // namespace

StatusOr<BlindOneLayerRegisterIndexResponse> BlindOneLayerRegisterIndex(
    const BlindOneLayerRegisterIndexRequest& request) {
  if (request.sketch().size() % (kBytesPerCipherText * 3)) {
    return InvalidArgumentError(
        "The size of sketch is not divisible by the register_size: " +
        std::to_string(kBytesPerCipherText * 3));
  }
  ASSIGN_OR_RETURN(
      CompositeCipher composite_cipher,
      CreateCompositeCipher(request.curve_id(), request.local_el_gamal_keys(),
                            request.local_pohlig_hellman_sk()));
  BlindOneLayerRegisterIndexResponse response;
  *response.mutable_local_pohlig_hellman_sk() =
      composite_cipher.p_h_cipher->GetPrivateKeyBytes();
  std::string* response_sketch = response.mutable_sketch();
  // The output sketch is the same size with the input sketch.
  response_sketch->reserve(request.sketch().size());
  for (auto it = request.sketch().begin(); it < request.sketch().end();
       it += kBytesPerCipherRegister) {
    std::pair<std::string, std::string> ciphertext =
        ExtractCipherTextFromSubstring(it);
    ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                     composite_cipher.e_g_cipher->Decrypt(ciphertext));
    ASSIGN_OR_RETURN(ElGamalCipherText re_encrypted_p_h,
                     composite_cipher.p_h_cipher->ReEncryptElGamalCiphertext(
                         std::make_pair(ciphertext.first, decrypted_el_gamal)));
    // Append the result to the response.
    // 1. append the blinded register index value.
    response_sketch->append(re_encrypted_p_h.first);
    response_sketch->append(re_encrypted_p_h.second);
    // 2. keep the original key and count values
    // Skip the index and append the key and count values.
    response_sketch->append(it + kBytesPerCipherText,
                            it + kBytesPerCipherRegister);
  }
  return response;
};

StatusOr<BlindLastLayerIndexThenJoinRegistersResponse>
BlindLastLayerIndexThenJoinRegisters(
    const BlindLastLayerIndexThenJoinRegistersRequest& request) {
  return InternalError("unimplemented.");
};

StatusOr<DecryptOneLayerFlagAndCountResponse> DecryptOneLayerFlagAndCount(
    const DecryptOneLayerFlagAndCountRequest& request) {
  return InternalError("unimplemented.");
};

StatusOr<DecryptLastLayerFlagAndCountResponse> DecryptLastLayerFlagAndCount(
    const DecryptLastLayerFlagAndCountRequest& request) {
  return InternalError("unimplemented.");
};

}  // namespace wfa::measurement::crypto
