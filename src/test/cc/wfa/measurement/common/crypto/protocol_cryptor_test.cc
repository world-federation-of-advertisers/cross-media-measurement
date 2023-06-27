// Copyright 2023 The Cross-Media Measurement Authors
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

#include "any_sketch/crypto/sketch_encrypter.h"
#include "common_cpp/testing/status_macros.h"
#include "gtest/gtest.h"
#include "private_join_and_compute/crypto/commutative_elgamal.h"
#include "private_join_and_compute/crypto/context.h"
#include "private_join_and_compute/crypto/ec_commutative_cipher.h"
#include "private_join_and_compute/crypto/ec_group.h"
#include "wfa/measurement/common/crypto/constants.h"
#include "wfa/measurement/common/crypto/ec_point_util.h"
#include "wfa/measurement/internal/duchy/crypto.pb.h"

namespace src::test::cc::wfa::measurement::common::crypto {
namespace {

using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::wfa::measurement::common::crypto::CompositeType;
using ::wfa::measurement::common::crypto::CreateIdenticalProtocolCrypors;
using ::wfa::measurement::common::crypto::ElGamalCiphertext;
using ::wfa::measurement::common::crypto::kBytesPerEcPoint;
using ::wfa::measurement::common::crypto::kGenerateNewCompositeCipher;
using ::wfa::measurement::common::crypto::kGenerateNewParitialCompositeCipher;
using ::wfa::measurement::common::crypto::ProtocolCryptorOptions;
using ::wfa::measurement::internal::duchy::ElGamalKeyPair;
using ::wfa::measurement::internal::duchy::ElGamalPublicKey;

constexpr int kTestCurveId = NID_X9_62_prime256v1;

absl::StatusOr<ElGamalKeyPair> CreateElGamalKeyPair(int curve_id) {
  ASSIGN_OR_RETURN(std::unique_ptr<CommutativeElGamal> cipher,
                   CommutativeElGamal::CreateWithNewKeyPair(curve_id));
  ASSIGN_OR_RETURN(ElGamalCiphertext public_key, cipher->GetPublicKeyBytes());
  ASSIGN_OR_RETURN(std::string private_key, cipher->GetPrivateKeyBytes());

  ElGamalKeyPair key_pair;
  key_pair.mutable_public_key()->set_generator(public_key.first);
  key_pair.mutable_public_key()->set_element(public_key.second);
  key_pair.set_secret_key(private_key);

  return key_pair;
}

TEST(MultipleCryptors, CryptorsCanEncryptAndDescrypt) {
  ASSERT_OK_AND_ASSIGN(ElGamalKeyPair key_pair,
                       CreateElGamalKeyPair(kTestCurveId));
  auto cryptor_options = ProtocolCryptorOptions{
      .curve_id = kTestCurveId,
      .local_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .local_el_gamal_private_key = key_pair.secret_key(),
      .composite_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};
  ASSERT_OK_AND_ASSIGN(auto cryptors,
                       CreateIdenticalProtocolCrypors(2, cryptor_options));
  auto &encryption_cryptor = *cryptors[0];
  auto &decryption_cryptor = *cryptors[1];

  auto ctx = absl::make_unique<Context>();
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group,
                       ECGroup::Create(kTestCurveId, ctx.get()));
  auto plaintext = ctx->GenerateRandomBytes(kBytesPerEcPoint);
  ASSERT_OK_AND_ASSIGN(ECPoint plaintext_ec,
                       ec_group.GetPointByHashingToCurveSha256(plaintext));
  ASSERT_OK_AND_ASSIGN(std::string plaintext_bytes,
                       plaintext_ec.ToBytesCompressed());

  ASSERT_OK_AND_ASSIGN(ElGamalCiphertext cipher_text,
                       encryption_cryptor.EncryptCompositeElGamal(
                           plaintext_bytes, CompositeType::kFull));
  ASSERT_OK_AND_ASSIGN(std::string result,
                       decryption_cryptor.DecryptLocalElGamal(cipher_text));
  EXPECT_EQ(result, plaintext_bytes);
}

TEST(MultipleCryptors, CryptorsHaveSameBlindResult) {
  ASSERT_OK_AND_ASSIGN(ElGamalKeyPair key_pair,
                       CreateElGamalKeyPair(kTestCurveId));

  auto cryptor_options = ProtocolCryptorOptions{
      .curve_id = kTestCurveId,
      .local_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .local_el_gamal_private_key = key_pair.secret_key(),
      .composite_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};
  ASSERT_OK_AND_ASSIGN(
      auto encryption_cryptor,
      CommutativeElGamal::CreateFromPublicKey(
          kTestCurveId, std::make_pair(key_pair.public_key().generator(),
                                       key_pair.public_key().element())));
  ASSERT_OK_AND_ASSIGN(auto cryptors,
                       CreateIdenticalProtocolCrypors(2, cryptor_options));

  auto ctx = absl::make_unique<Context>();
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group,
                       ECGroup::Create(kTestCurveId, ctx.get()));
  auto plaintext = ctx->GenerateRandomBytes(kBytesPerEcPoint);
  ASSERT_OK_AND_ASSIGN(ECPoint plaintext_ec,
                       ec_group.GetPointByHashingToCurveSha256(plaintext));
  ASSERT_OK_AND_ASSIGN(std::string plaintext_bytes,
                       plaintext_ec.ToBytesCompressed());

  ASSERT_OK_AND_ASSIGN(ElGamalCiphertext cipher_text,
                       encryption_cryptor->Encrypt(plaintext_bytes));
  ASSERT_OK_AND_ASSIGN(ElGamalCiphertext blind_result_1,
                       cryptors[0]->Blind(cipher_text));
  ASSERT_OK_AND_ASSIGN(ElGamalCiphertext blind_result_2,
                       cryptors[1]->Blind(cipher_text));

  ASSERT_EQ(blind_result_1, blind_result_2);
}

TEST(MultipleCryptors, CryptorsReRandomizeHaveMatchedResult) {
  ASSERT_OK_AND_ASSIGN(ElGamalKeyPair key_pair,
                       CreateElGamalKeyPair(kTestCurveId));

  auto cryptor_options = ProtocolCryptorOptions{
      .curve_id = kTestCurveId,
      .local_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .local_el_gamal_private_key = key_pair.secret_key(),
      .composite_el_gamal_public_key = std::make_pair(
          key_pair.public_key().generator(), key_pair.public_key().element()),
      .partial_composite_el_gamal_public_key =
          kGenerateNewParitialCompositeCipher};
  ASSERT_OK_AND_ASSIGN(
      auto encryption_cryptor,
      CommutativeElGamal::CreateFromPublicKey(
          kTestCurveId, std::make_pair(key_pair.public_key().generator(),
                                       key_pair.public_key().element())));
  ASSERT_OK_AND_ASSIGN(auto cryptors,
                       CreateIdenticalProtocolCrypors(2, cryptor_options));

  auto ctx = absl::make_unique<Context>();
  ASSERT_OK_AND_ASSIGN(ECGroup ec_group,
                       ECGroup::Create(kTestCurveId, ctx.get()));
  auto plaintext = ctx->GenerateRandomBytes(kBytesPerEcPoint);
  ASSERT_OK_AND_ASSIGN(ECPoint plaintext_ec,
                       ec_group.GetPointByHashingToCurveSha256(plaintext));
  ASSERT_OK_AND_ASSIGN(std::string plaintext_bytes,
                       plaintext_ec.ToBytesCompressed());

  ASSERT_OK_AND_ASSIGN(ElGamalCiphertext cipher_text,
                       encryption_cryptor->Encrypt(plaintext_bytes));
  ASSERT_OK_AND_ASSIGN(
      ElGamalCiphertext re_randomize_result_1,
      cryptors[0]->ReRandomize(cipher_text, CompositeType::kFull));
  ASSERT_OK_AND_ASSIGN(
      ElGamalCiphertext re_randomize_result_2,
      cryptors[1]->ReRandomize(cipher_text, CompositeType::kFull));
  ASSERT_OK_AND_ASSIGN(std::string decryption_result_1,
                       cryptors[0]->DecryptLocalElGamal(re_randomize_result_1));
  ASSERT_OK_AND_ASSIGN(std::string decryption_result_2,
                       cryptors[1]->DecryptLocalElGamal(re_randomize_result_2));

  EXPECT_EQ(decryption_result_1, plaintext_bytes);
  EXPECT_EQ(decryption_result_2, plaintext_bytes);
}

}  // namespace
}  // namespace src::test::cc::wfa::measurement::common::crypto
