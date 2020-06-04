#include "protocol_encryption_utility.h"

#include <openssl/obj_mac.h>
#include <wfa/measurement/internal/duchy/protocol_encryption_methods.pb.h>

#include "absl/strings/string_view.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "gmock/gmock.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "src/main/cc/any_sketch/sketch_encrypter.h"
#include "wfa/measurement/api/v1alpha/sketch.pb.h"

namespace wfa::measurement::crypto {
namespace {

using ::private_join_and_compute::CommutativeElGamal;
using ::private_join_and_compute::Context;
using ::private_join_and_compute::ECCommutativeCipher;
using ::private_join_and_compute::ECGroup;
using ::private_join_and_compute::ECPoint;
using ::private_join_and_compute::Status;
using ::private_join_and_compute::StatusCode;
using ::wfa::measurement::api::v1alpha::Sketch;
using ::wfa::measurement::api::v1alpha::SketchConfig;
using ::wfa::measurement::internal::duchy::ElGamalKeys;
using FlagCount = ::wfa::measurement::internal::duchy::
    DecryptLastLayerFlagAndCountResponse::FlagCount;

constexpr int kMaxFrequency = 10;
constexpr int kTestCurveId = NID_X9_62_prime256v1;
constexpr int kBytesPerEcPoint = 33;
constexpr int kBytesCipherText = kBytesPerEcPoint * 2;
constexpr int kBytesPerEncryptedRegister = kBytesCipherText * 3;

MATCHER_P2(StatusIs, code, message, "") {
  return ExplainMatchResult(
      AllOf(testing::Property(&Status::code, code),
            testing::Property(&Status::message, testing::HasSubstr(message))),
      arg, result_listener);
}

// Returns true if two proto messages are equal when ignoring the order of
// repeated fields.
MATCHER_P(EqualsProto, expected, "") {
  ::google::protobuf::util::MessageDifferencer differencer;
  differencer.set_repeated_field_comparison(
      ::google::protobuf::util::MessageDifferencer::AS_SET);
  return differencer.Compare(arg, expected);
}

ElGamalKeys GenerateRandomElGamalKeys(const int curve_id) {
  ElGamalKeys el_gamal_keys;
  auto el_gamal_cipher =
      CommutativeElGamal::CreateWithNewKeyPair(curve_id).value();
  *el_gamal_keys.mutable_el_gamal_sk() =
      el_gamal_cipher.get()->GetPrivateKeyBytes().value();
  *el_gamal_keys.mutable_el_gamal_g() =
      el_gamal_cipher.get()->GetPublicKeyBytes().value().first;
  *el_gamal_keys.mutable_el_gamal_y() =
      el_gamal_cipher.get()->GetPublicKeyBytes().value().second;
  return el_gamal_keys;
}

std::string GenerateRandomPhKey(const int curve_id) {
  auto p_h_cipher = ECCommutativeCipher::CreateWithNewKey(
                        curve_id, ECCommutativeCipher::HashType::SHA256)
                        .value();
  return p_h_cipher.get()->GetPrivateKeyBytes();
}

void AddRegister(Sketch* sketch, const int index, const int key,
                 const int count) {
  auto register_ptr = sketch->add_registers();
  register_ptr->set_index(index);
  register_ptr->add_values(key);
  register_ptr->add_values(count);
}

Sketch CreateEmptyClceSketch() {
  Sketch plain_sketch;
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::UNIQUE);
  plain_sketch.mutable_config()->add_values()->set_aggregator(
      SketchConfig::ValueSpec::SUM);
  return plain_sketch;
}

FlagCount CreateFlagCount(const bool is_not_destroyed, const int frequency) {
  FlagCount result;
  result.set_is_not_destroyed(is_not_destroyed);
  result.set_frequency(frequency);
  return result;
}

// The TestData generates cipher keys for 3 duchies, and the combined public
// key for the data providers.
class TestData {
 public:
  ElGamalKeys duchy_1_el_gamal_keys_;
  std::string duchy_1_p_h_key_;
  ElGamalKeys duchy_2_el_gamal_keys_;
  std::string duchy_2_p_h_key_;
  ElGamalKeys duchy_3_el_gamal_keys_;
  std::string duchy_3_p_h_key_;
  ElGamalKeys client_el_gamal_keys_;  // combined from 3 duchy keys;
  std::unique_ptr<any_sketch::SketchEncrypter> sketch_encrypter;

  TestData() {
    duchy_1_el_gamal_keys_ = GenerateRandomElGamalKeys(kTestCurveId);
    duchy_2_el_gamal_keys_ = GenerateRandomElGamalKeys(kTestCurveId);
    duchy_3_el_gamal_keys_ = GenerateRandomElGamalKeys(kTestCurveId);
    duchy_1_p_h_key_ = GenerateRandomPhKey(kTestCurveId);
    duchy_2_p_h_key_ = GenerateRandomPhKey(kTestCurveId);
    duchy_3_p_h_key_ = GenerateRandomPhKey(kTestCurveId);

    // Combine the el_gamal keys from all duchies to generate the data provider
    // el_gamal key.
    Context ctx;
    ECGroup ec_group = ECGroup::Create(kTestCurveId, &ctx).value();
    ECPoint duchy_1_public_el_gamal_y_ec =
        ec_group.CreateECPoint(duchy_1_el_gamal_keys_.el_gamal_y()).value();
    ECPoint duchy_2_public_el_gamal_y_ec =
        ec_group.CreateECPoint(duchy_2_el_gamal_keys_.el_gamal_y()).value();
    ECPoint duchy_3_public_el_gamal_y_ec =
        ec_group.CreateECPoint(duchy_3_el_gamal_keys_.el_gamal_y()).value();
    ECPoint client_public_el_gamal_y_ec =
        duchy_1_public_el_gamal_y_ec.Add(duchy_2_public_el_gamal_y_ec)
            .value()
            .Add(duchy_3_public_el_gamal_y_ec)
            .value();
    client_el_gamal_keys_.set_el_gamal_g(duchy_1_el_gamal_keys_.el_gamal_g());
    client_el_gamal_keys_.set_el_gamal_y(
        client_public_el_gamal_y_ec.ToBytesCompressed().value());

    any_sketch::CiphertextString client_public_key = {
        .u = client_el_gamal_keys_.el_gamal_g(),
        .e = client_el_gamal_keys_.el_gamal_y(),
    };

    // Create a sketch_encryter for encrypting plaintext any_sketch data.
    sketch_encrypter = any_sketch::CreateWithPublicKey(
                           kTestCurveId, kMaxFrequency, client_public_key)
                           .value();
  }

  // Helper function to go through the entire MPC protocol using the input data.
  // The final (flag, count) lists are returned.
  DecryptLastLayerFlagAndCountResponse GoThroughEntireMpcProtocol(
      const std::vector<unsigned char>& encrypted_sketch) {
    // Blind register indexes at duchy 1
    BlindOneLayerRegisterIndexRequest blind_one_layer_register_index_request_1;
    *blind_one_layer_register_index_request_1
         .mutable_local_pohlig_hellman_sk() = duchy_1_p_h_key_;
    *blind_one_layer_register_index_request_1.mutable_local_el_gamal_keys() =
        duchy_1_el_gamal_keys_;
    blind_one_layer_register_index_request_1.set_curve_id(kTestCurveId);
    blind_one_layer_register_index_request_1.mutable_sketch()->append(
        encrypted_sketch.begin(), encrypted_sketch.end());
    BlindOneLayerRegisterIndexResponse
        blind_one_layer_register_index_response_1 =
            BlindOneLayerRegisterIndex(blind_one_layer_register_index_request_1)
                .value();

    // Blind register indexes at duchy 2
    BlindOneLayerRegisterIndexRequest blind_one_layer_register_index_request_2;
    *blind_one_layer_register_index_request_2
         .mutable_local_pohlig_hellman_sk() = duchy_2_p_h_key_;
    *blind_one_layer_register_index_request_2.mutable_local_el_gamal_keys() =
        duchy_2_el_gamal_keys_;
    blind_one_layer_register_index_request_2.set_curve_id(kTestCurveId);
    blind_one_layer_register_index_request_2.mutable_sketch()->append(
        blind_one_layer_register_index_response_1.sketch().begin(),
        blind_one_layer_register_index_response_1.sketch().end());
    BlindOneLayerRegisterIndexResponse
        blind_one_layer_register_index_response_2 =
            BlindOneLayerRegisterIndex(blind_one_layer_register_index_request_2)
                .value();

    // Blind register indexes and join registers at duchy 3 (primary duchy)
    BlindLastLayerIndexThenJoinRegistersRequest
        blind_last_layer_index_then_join_registers_request;
    *blind_last_layer_index_then_join_registers_request
         .mutable_local_pohlig_hellman_sk() = duchy_3_p_h_key_;
    *blind_last_layer_index_then_join_registers_request
         .mutable_local_el_gamal_keys() = duchy_3_el_gamal_keys_;
    *blind_last_layer_index_then_join_registers_request
         .mutable_composite_el_gamal_keys() = client_el_gamal_keys_;
    blind_last_layer_index_then_join_registers_request.set_curve_id(
        kTestCurveId);
    blind_last_layer_index_then_join_registers_request.mutable_sketch()->append(
        blind_one_layer_register_index_response_2.sketch().begin(),
        blind_one_layer_register_index_response_2.sketch().end());
    BlindLastLayerIndexThenJoinRegistersResponse
        blind_last_layer_index_then_join_registers_response =
            BlindLastLayerIndexThenJoinRegisters(
                blind_last_layer_index_then_join_registers_request)
                .value();

    // Decrypt flags and counts at duchy 1
    DecryptOneLayerFlagAndCountRequest
        decrypt_one_layer_flag_and_count_request_1;
    *decrypt_one_layer_flag_and_count_request_1.mutable_local_el_gamal_keys() =
        duchy_1_el_gamal_keys_;
    decrypt_one_layer_flag_and_count_request_1.set_curve_id(kTestCurveId);
    decrypt_one_layer_flag_and_count_request_1.mutable_flag_counts()->append(
        blind_last_layer_index_then_join_registers_response.flag_counts()
            .begin(),
        blind_last_layer_index_then_join_registers_response.flag_counts()
            .end());
    DecryptOneLayerFlagAndCountResponse
        decrypt_one_layer_flag_and_count_response_1 =
            DecryptOneLayerFlagAndCount(
                decrypt_one_layer_flag_and_count_request_1)
                .value();

    // Decrypt flags and counts at duchy 2
    DecryptOneLayerFlagAndCountRequest
        decrypt_one_layer_flag_and_count_request_2;
    *decrypt_one_layer_flag_and_count_request_2.mutable_local_el_gamal_keys() =
        duchy_2_el_gamal_keys_;
    decrypt_one_layer_flag_and_count_request_2.set_curve_id(kTestCurveId);
    decrypt_one_layer_flag_and_count_request_2.mutable_flag_counts()->append(
        decrypt_one_layer_flag_and_count_response_1.flag_counts().begin(),
        decrypt_one_layer_flag_and_count_response_1.flag_counts().end());
    DecryptOneLayerFlagAndCountResponse
        decrypt_one_layer_flag_and_count_response_2 =
            DecryptOneLayerFlagAndCount(
                decrypt_one_layer_flag_and_count_request_2)
                .value();

    // Decrypt flags and counts at duchy 3 (primary duchy).
    DecryptLastLayerFlagAndCountRequest
        decrypt_last_layer_flag_and_count_request;
    *decrypt_last_layer_flag_and_count_request.mutable_local_el_gamal_keys() =
        duchy_3_el_gamal_keys_;
    decrypt_last_layer_flag_and_count_request.set_curve_id(kTestCurveId);
    decrypt_last_layer_flag_and_count_request.set_maximum_frequency(
        kMaxFrequency);
    decrypt_last_layer_flag_and_count_request.mutable_flag_counts()->append(
        decrypt_one_layer_flag_and_count_response_2.flag_counts().begin(),
        decrypt_one_layer_flag_and_count_response_2.flag_counts().end());
    DecryptLastLayerFlagAndCountResponse final_response =
        DecryptLastLayerFlagAndCount(decrypt_last_layer_flag_and_count_request)
            .value();
    return final_response;
  }
};

TEST(BlindOneLayerRegisterIndex, wrongInputSketchSizeShouldThrow) {
  BlindOneLayerRegisterIndexRequest request;
  auto result = BlindOneLayerRegisterIndex(request);

  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(), StatusIs(StatusCode::kInvalidArgument, "empty"));

  *request.mutable_sketch() = "1234";
  result = BlindOneLayerRegisterIndex(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(StatusCode::kInvalidArgument, "not divisible"));
}

TEST(BlindLastLayerIndexThenJoinRegisters, wrongInputSketchSizeShouldThrow) {
  BlindLastLayerIndexThenJoinRegistersRequest request;
  auto result = BlindLastLayerIndexThenJoinRegisters(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(), StatusIs(StatusCode::kInvalidArgument, "empty"));

  *request.mutable_sketch() = "1234";
  result = BlindLastLayerIndexThenJoinRegisters(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(StatusCode::kInvalidArgument, "not divisible"));
}

TEST(DecryptOneLayerFlagAndCount, wrongInputSketchSizeShouldThrow) {
  DecryptOneLayerFlagAndCountRequest request;
  auto result = DecryptOneLayerFlagAndCount(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(), StatusIs(StatusCode::kInvalidArgument, "empty"));

  *request.mutable_flag_counts() = "1234";
  result = DecryptOneLayerFlagAndCount(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(StatusCode::kInvalidArgument, "not divisible"));
}

TEST(DecryptLastLayerFlagAndCount, wrongInputSketchSizeShouldThrow) {
  DecryptLastLayerFlagAndCountRequest request;
  auto result = DecryptLastLayerFlagAndCount(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(), StatusIs(StatusCode::kInvalidArgument, "empty"));

  *request.mutable_flag_counts() = "1234";
  result = DecryptLastLayerFlagAndCount(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(StatusCode::kInvalidArgument, "not divisible"));
}

TEST(EndToEnd, sumOfCountsShouldBeCorrect) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 4);
  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  DecryptLastLayerFlagAndCountResponse final_response =
      test_data.GoThroughEntireMpcProtocol(encrypted_sketch);

  DecryptLastLayerFlagAndCountResponse expected;
  *expected.add_flag_counts() = CreateFlagCount(true, 9);

  EXPECT_THAT(final_response, EqualsProto(expected));
}

TEST(EndToEnd, sumOfCoutsShouldBeCappedByMaximumFrequency) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111,
              /* count = */ kMaxFrequency - 2);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 3);
  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  DecryptLastLayerFlagAndCountResponse final_response =
      test_data.GoThroughEntireMpcProtocol(encrypted_sketch);

  DecryptLastLayerFlagAndCountResponse expected;
  *expected.add_flag_counts() = CreateFlagCount(true, kMaxFrequency);

  EXPECT_THAT(final_response, EqualsProto(expected));
}

TEST(EndToEnd, keyCollisionShouldDestroyCount) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 222, /* count = */ 2);
  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  DecryptLastLayerFlagAndCountResponse final_response =
      test_data.GoThroughEntireMpcProtocol(encrypted_sketch);

  DecryptLastLayerFlagAndCountResponse expected;
  *expected.add_flag_counts() = CreateFlagCount(false, kMaxFrequency);

  EXPECT_THAT(final_response, EqualsProto(expected));
}

TEST(EndToEnd, zeroCountShouldBeSkipped) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 222, /* count = */ 0);
  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  DecryptLastLayerFlagAndCountResponse final_response =
      test_data.GoThroughEntireMpcProtocol(encrypted_sketch);

  DecryptLastLayerFlagAndCountResponse expected;

  EXPECT_THAT(final_response, EqualsProto(expected));
}

TEST(EndToEnd, combinedCases) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();

  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 100, /* count = */ 1);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 200, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 300, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 4, /* key = */ 400, /* count = */ 4);
  AddRegister(&plain_sketch, /* index = */ 5, /* key = */ 500, /* count = */ 0);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 101, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 102, /* count = */ 1);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 200, /* count = */ 3);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 300, /* count = */ 8);
  AddRegister(&plain_sketch, /* index = */ 4, /* key = */ 400, /* count = */ 0);

  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  DecryptLastLayerFlagAndCountResponse final_response =
      test_data.GoThroughEntireMpcProtocol(encrypted_sketch);

  DecryptLastLayerFlagAndCountResponse expected;
  // For index 1, key collisions, the counter is destroyed ( decrypted as
  // kMaxFrequency)
  *expected.add_flag_counts() = CreateFlagCount(false, kMaxFrequency);
  // For index 2, no key collision, count sum = 2 + 3 = 5;
  *expected.add_flag_counts() = CreateFlagCount(true, 5);
  // For index 3, no key collision, count sum = 3 + 8 = 11 > 10 (max_frequency)
  *expected.add_flag_counts() = CreateFlagCount(true, kMaxFrequency);
  // For index 4, no key collision, count sum = 4 + 0 = 4;
  *expected.add_flag_counts() = CreateFlagCount(true, 4);
  // For index 5, no key collision, count sum = 0; ignored in the result.

  EXPECT_THAT(final_response, EqualsProto(expected));
}

}  // namespace
}  // namespace wfa::measurement::crypto