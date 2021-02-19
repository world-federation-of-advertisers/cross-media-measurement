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

#include "wfa/measurement/common/crypto/protocol_cryptor.h"

#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/context.h"
#include "crypto/ec_commutative_cipher.h"
#include "crypto/ec_group.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"

namespace wfa::measurement::common::crypto {

namespace {
using ::private_join_and_compute::BigNum;
using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;

class ProtocolCryptorImpl : public ProtocolCryptor {
 public:
  ProtocolCryptorImpl(
      std::unique_ptr<CommutativeElGamal> local_el_gamal_cipher,
      std::unique_ptr<CommutativeElGamal> client_el_gamal_cipher,
      std::unique_ptr<CommutativeElGamal> partial_composite_el_gamal_cipher,
      std::unique_ptr<ECCommutativeCipher> local_pohlig_hellman_cipher,
      std::unique_ptr<Context> ctx, ECGroup ec_group);
  ~ProtocolCryptorImpl() override = default;
  ProtocolCryptorImpl(ProtocolCryptorImpl&& other) = delete;
  ProtocolCryptorImpl& operator=(ProtocolCryptorImpl&& other) = delete;
  ProtocolCryptorImpl(const ProtocolCryptorImpl&) = delete;
  ProtocolCryptorImpl& operator=(const ProtocolCryptorImpl&) = delete;

  absl::StatusOr<ElGamalCiphertext> Blind(
      const ElGamalCiphertext& ciphertext) override;
  absl::StatusOr<std::string> DecryptLocalElGamal(
      const ElGamalCiphertext& ciphertext) override;
  absl::StatusOr<ElGamalCiphertext> EncryptPlaintextCompositeElGamal(
      absl::string_view plaintext, CompositeType composite_type) override;
  absl::StatusOr<ElGamalEcPointPair> EncryptPlaintextToEcPointsCompositeElGamal(
      absl::string_view plaintext, CompositeType composite_type) override;
  absl::StatusOr<ElGamalEcPointPair>
  EncryptIdentityElementToEcPointsCompositeElGamal(
      CompositeType composite_type) override;
  absl::StatusOr<ElGamalCiphertext> EncryptCompositeElGamal(
      absl::string_view plain_ec_point, CompositeType composite_type) override;
  absl::StatusOr<ElGamalCiphertext> ReRandomize(
      const ElGamalCiphertext& ciphertext,
      CompositeType composite_type) override;
  absl::StatusOr<ElGamalEcPointPair> CalculateDestructor(
      const ElGamalEcPointPair& base, const ElGamalEcPointPair& key) override;
  absl::StatusOr<std::string> MapToCurve(absl::string_view str) override;
  absl::StatusOr<std::string> MapToCurve(int64_t x) override;
  absl::StatusOr<ElGamalEcPointPair> ToElGamalEcPoints(
      const ElGamalCiphertext& cipher_text) override;
  std::string GetLocalPohligHellmanKey() override;
  absl::Status BatchProcess(absl::string_view data,
                            absl::Span<const Action> actions,
                            std::string& result) override;
  absl::StatusOr<bool> IsDecryptLocalElGamalResultZero(
      const ElGamalCiphertext& ciphertext) override;
  BigNum NextRandomBigNum() override;
  std::string NextRandomBigNumAsString() override;

 private:
  // A CommutativeElGamal cipher created using local ElGamal Keys, used for
  // encrypting/decrypting local layer of ElGamal encryption.
  const std::unique_ptr<CommutativeElGamal> local_el_gamal_cipher_;
  // A CommutativeElGamal cipher created using the combined public key, used
  // for re-randomizing ciphertext and sameKeyAggregation, etc.
  const std::unique_ptr<CommutativeElGamal> composite_el_gamal_cipher_;
  // A CommutativeElGamal cipher created using the partially combined public
  // key, used for re-randomizing partially decrypted ciphertexts.
  const std::unique_ptr<CommutativeElGamal> partial_composite_el_gamal_cipher_;
  // An ECCommutativeCipher used for blinding a ciphertext.
  const std::unique_ptr<ECCommutativeCipher> local_pohlig_hellman_cipher_;

  // Context used for storing temporary values to be reused across openssl
  // function calls for better performance.
  const std::unique_ptr<Context> ctx_;
  // The EC Group representing the curve definition.
  const ECGroup ec_group_;

  // Since the underlying private-join-and-computer::CommutativeElGamal is NOT
  // thread safe, we use mutex to enforce thread safety in this class.
  absl::Mutex mutex_;
};

ProtocolCryptorImpl::ProtocolCryptorImpl(
    std::unique_ptr<CommutativeElGamal> local_el_gamal_cipher,
    std::unique_ptr<CommutativeElGamal> client_el_gamal_cipher,
    std::unique_ptr<CommutativeElGamal> partial_composite_el_gamal_cipher,
    std::unique_ptr<ECCommutativeCipher> local_pohlig_hellman_cipher,
    std::unique_ptr<Context> ctx, ECGroup ec_group)
    : local_el_gamal_cipher_(std::move(local_el_gamal_cipher)),
      composite_el_gamal_cipher_(std::move(client_el_gamal_cipher)),
      partial_composite_el_gamal_cipher_(
          std::move(partial_composite_el_gamal_cipher)),
      local_pohlig_hellman_cipher_(std::move(local_pohlig_hellman_cipher)),
      ctx_(std::move(ctx)),
      ec_group_(std::move(ec_group)) {}

absl::StatusOr<ElGamalCiphertext> ProtocolCryptorImpl::Blind(
    const ElGamalCiphertext& ciphertext) {
  absl::WriterMutexLock l(&mutex_);
  ASSIGN_OR_RETURN(std::string decrypted_el_gamal,
                   local_el_gamal_cipher_->Decrypt(ciphertext));
  ASSIGN_OR_RETURN(ElGamalCiphertext re_encrypted_p_h,
                   local_pohlig_hellman_cipher_->ReEncryptElGamalCiphertext(
                       std::make_pair(ciphertext.first, decrypted_el_gamal)));
  return {std::move(re_encrypted_p_h)};
}

absl::StatusOr<std::string> ProtocolCryptorImpl::DecryptLocalElGamal(
    const ElGamalCiphertext& ciphertext) {
  absl::WriterMutexLock l(&mutex_);
  return local_el_gamal_cipher_->Decrypt(ciphertext);
}

absl::StatusOr<ElGamalCiphertext>
ProtocolCryptorImpl::EncryptPlaintextCompositeElGamal(
    absl::string_view plaintext, CompositeType composite_type) {
  ASSIGN_OR_RETURN(std::string ec_point, MapToCurve(plaintext));
  return EncryptCompositeElGamal(ec_point, composite_type);
}

absl::StatusOr<ElGamalEcPointPair>
ProtocolCryptorImpl::EncryptPlaintextToEcPointsCompositeElGamal(
    absl::string_view plaintext, CompositeType composite_type) {
  ASSIGN_OR_RETURN(ElGamalCiphertext temp,
                   EncryptPlaintextCompositeElGamal(plaintext, composite_type));
  return ToElGamalEcPoints(temp);
}

absl::StatusOr<ElGamalEcPointPair>
ProtocolCryptorImpl::EncryptIdentityElementToEcPointsCompositeElGamal(
    CompositeType composite_type) {
  ASSIGN_OR_RETURN(
      ElGamalCiphertext temp,
      composite_type == CompositeType::kFull
          ? composite_el_gamal_cipher_->EncryptIdentityElement()
          : partial_composite_el_gamal_cipher_->EncryptIdentityElement());
  return ToElGamalEcPoints(temp);
}

absl::StatusOr<ElGamalCiphertext> ProtocolCryptorImpl::EncryptCompositeElGamal(
    absl::string_view plain_ec_point, CompositeType composite_type) {
  absl::WriterMutexLock l(&mutex_);
  return composite_type == CompositeType::kFull
             ? composite_el_gamal_cipher_->Encrypt(plain_ec_point)
             : partial_composite_el_gamal_cipher_->Encrypt(plain_ec_point);
}

absl::StatusOr<ElGamalCiphertext> ProtocolCryptorImpl::ReRandomize(
    const ElGamalCiphertext& ciphertext, CompositeType composite_type) {
  ASSIGN_OR_RETURN(
      ElGamalEcPointPair zero_ec,
      EncryptIdentityElementToEcPointsCompositeElGamal(composite_type));
  ASSIGN_OR_RETURN(ElGamalEcPointPair ciphertext_ec,
                   GetElGamalEcPoints(ciphertext, ec_group_));
  ASSIGN_OR_RETURN(ElGamalEcPointPair result_ec,
                   AddEcPointPairs(zero_ec, ciphertext_ec));

  ElGamalCiphertext result_ciphertext;
  ASSIGN_OR_RETURN(result_ciphertext.first, result_ec.u.ToBytesCompressed());
  ASSIGN_OR_RETURN(result_ciphertext.second, result_ec.e.ToBytesCompressed());
  return {std::move(result_ciphertext)};
}

absl::StatusOr<ElGamalEcPointPair> ProtocolCryptorImpl::CalculateDestructor(
    const ElGamalEcPointPair& base_inverse, const ElGamalEcPointPair& key) {
  absl::WriterMutexLock l(&mutex_);
  BigNum r = ec_group_.GeneratePrivateKey();
  ASSIGN_OR_RETURN(ElGamalEcPointPair key_delta,
                   AddEcPointPairs(key, base_inverse));
  return MultiplyEcPointPairByScalar(key_delta, r);
}

absl::StatusOr<std::string> ProtocolCryptorImpl::MapToCurve(
    absl::string_view str) {
  absl::WriterMutexLock l(&mutex_);
  ASSIGN_OR_RETURN(ECPoint temp_ec_point,
                   ec_group_.GetPointByHashingToCurveSha256(str));
  return temp_ec_point.ToBytesCompressed();
}

absl::StatusOr<std::string> ProtocolCryptorImpl::MapToCurve(int64_t x) {
  return MapToCurve(std::to_string(x));
}

absl::StatusOr<ElGamalEcPointPair> ProtocolCryptorImpl::ToElGamalEcPoints(
    const ElGamalCiphertext& cipher_text) {
  absl::WriterMutexLock l(&mutex_);
  return GetElGamalEcPoints(cipher_text, ec_group_);
}

std::string ProtocolCryptorImpl::GetLocalPohligHellmanKey() {
  absl::WriterMutexLock l(&mutex_);
  return local_pohlig_hellman_cipher_->GetPrivateKeyBytes();
}

absl::Status ProtocolCryptorImpl::BatchProcess(absl::string_view data,
                                               absl::Span<const Action> actions,
                                               std::string& result) {
  size_t num_of_ciphertext = actions.size();
  if (data.size() != num_of_ciphertext * kBytesPerCipherText) {
    return absl::InvalidArgumentError(
        "The input data can not be partitioned to ciphertexts.");
  }
  for (size_t index = 0; index < num_of_ciphertext; ++index) {
    ElGamalCiphertext ciphertext = std::make_pair(
        std::string(data.substr(index * kBytesPerCipherText, kBytesPerEcPoint)),
        std::string(data.substr(index * kBytesPerCipherText + kBytesPerEcPoint,
                                kBytesPerEcPoint)));
    switch (actions[index]) {
      case Action::kBlind: {
        ASSIGN_OR_RETURN(ElGamalCiphertext temp, Blind(ciphertext));
        result.append(temp.first);
        result.append(temp.second);
        break;
      }
      case Action::kPartialDecrypt: {
        ASSIGN_OR_RETURN(std::string temp, DecryptLocalElGamal(ciphertext));
        // The first part of the ciphertext is the random number which is still
        // required to decrypt the other layers of ElGamal encryptions (at the
        // subsequent duchies. So we keep it.
        result.append(ciphertext.first);
        result.append(temp);
        break;
      }
      case Action::kPartialDecryptAndReRandomize: {
        ASSIGN_OR_RETURN(std::string decrypted,
                         DecryptLocalElGamal(ciphertext));
        // Rerandomize the decrypted ciphertext such that it couldn't be
        // distinguished by the first element.
        ASSIGN_OR_RETURN(
            ElGamalCiphertext temp,
            ReRandomize(std::make_pair(ciphertext.first, decrypted),
                        CompositeType::kPartial));
        result.append(temp.first);
        result.append(temp.second);
        break;
      }
      case Action::kDecrypt: {
        ASSIGN_OR_RETURN(std::string temp, DecryptLocalElGamal(ciphertext));
        result.append(temp);
        break;
      }
      case Action::kReRandomize: {
        ASSIGN_OR_RETURN(ElGamalCiphertext temp,
                         ReRandomize(ciphertext, CompositeType::kFull));
        result.append(temp.first);
        result.append(temp.second);
        break;
      }
      case Action::kNoop: {
        result.append(ciphertext.first);
        result.append(ciphertext.second);
        break;
      }
      default:
        return absl::InvalidArgumentError("Unknown action.");
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ProtocolCryptorImpl::IsDecryptLocalElGamalResultZero(
    const ElGamalCiphertext& ciphertext) {
  absl::StatusOr<std::string> decryption =
      local_el_gamal_cipher_->Decrypt(ciphertext);
  if (decryption.ok()) {
    return false;
  } else if (absl::IsInternal(decryption.status()) &&
             decryption.status().message().find("POINT_AT_INFINITY") !=
                 std::string::npos) {
    // When the value is 0 (Point at Infinity), the decryption would
    // fail with message "POINT_AT_INFINITY".
    return true;
  } else {
    return decryption.status();
  }
}

BigNum ProtocolCryptorImpl::NextRandomBigNum() {
  return ec_group_.GeneratePrivateKey();
}

std::string ProtocolCryptorImpl::NextRandomBigNumAsString() {
  return NextRandomBigNum().ToDecimalString();
}

}  // namespace

absl::StatusOr<std::unique_ptr<ProtocolCryptor>> CreateProtocolCryptorWithKeys(
    int curve_id, const ElGamalCiphertext& local_el_gamal_public_key,
    absl::string_view local_el_gamal_private_key,
    absl::string_view local_pohlig_hellman_private_key,
    const ElGamalCiphertext& composite_el_gamal_public_key,
    const ElGamalCiphertext& partial_composite_el_gamal_public_key) {
  auto ctx = absl::make_unique<Context>();
  ASSIGN_OR_RETURN(ECGroup ec_group, ECGroup::Create(curve_id, ctx.get()));
  ASSIGN_OR_RETURN(
      auto local_el_gamal_cipher,
      local_el_gamal_public_key.first.empty()
          ? CommutativeElGamal::CreateWithNewKeyPair(curve_id)
          : (local_el_gamal_private_key.empty()
                 ? CommutativeElGamal::CreateFromPublicKey(
                       curve_id, local_el_gamal_public_key)
                 : CommutativeElGamal::CreateFromPublicAndPrivateKeys(
                       curve_id, local_el_gamal_public_key,
                       local_el_gamal_private_key)));
  ASSIGN_OR_RETURN(auto client_el_gamal_cipher,
                   composite_el_gamal_public_key.first.empty()
                       ? CommutativeElGamal::CreateWithNewKeyPair(curve_id)
                       : CommutativeElGamal::CreateFromPublicKey(
                             curve_id, composite_el_gamal_public_key));
  ASSIGN_OR_RETURN(auto partial_composite_el_gamal_cipher,
                   partial_composite_el_gamal_public_key.first.empty()
                       ? CommutativeElGamal::CreateWithNewKeyPair(curve_id)
                       : CommutativeElGamal::CreateFromPublicKey(
                             curve_id, partial_composite_el_gamal_public_key));
  ASSIGN_OR_RETURN(auto local_pohlig_hellman_cipher,
                   local_pohlig_hellman_private_key.empty()
                       ? ECCommutativeCipher::CreateWithNewKey(
                             curve_id, ECCommutativeCipher::HashType::SHA256)
                       : ECCommutativeCipher::CreateFromKey(
                             curve_id, local_pohlig_hellman_private_key,
                             ECCommutativeCipher::HashType::SHA256));

  std::unique_ptr<ProtocolCryptor> result =
      absl::make_unique<ProtocolCryptorImpl>(
          std::move(local_el_gamal_cipher), std::move(client_el_gamal_cipher),
          std::move(partial_composite_el_gamal_cipher),
          std::move(local_pohlig_hellman_cipher), std::move(ctx),
          std::move(ec_group));
  return {std::move(result)};
}

}  // namespace wfa::measurement::common::crypto
