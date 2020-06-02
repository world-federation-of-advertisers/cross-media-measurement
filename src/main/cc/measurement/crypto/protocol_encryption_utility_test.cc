#include "protocol_encryption_utility.h"

#include <openssl/obj_mac.h>
#include <wfa/measurement/internal/duchy/protocol_encryption_methods.pb.h>

#include "absl/strings/string_view.h"
#include "crypto/commutative_elgamal.h"
#include "crypto/ec_commutative_cipher.h"
#include "gmock/gmock.h"
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

static ElGamalKeys GenerateRandomElGamalKeys(int curve_id) {
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

std::string GenerateRandomPhKey(int curve_id) {
  auto p_h_cipher = ECCommutativeCipher::CreateWithNewKey(
                        curve_id, ECCommutativeCipher::HashType::SHA256)
                        .value();
  return p_h_cipher.get()->GetPrivateKeyBytes();
}

void AddRegister(Sketch* sketch, int index, int key, int count) {
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

// The TestData generates cipher keys for 3 duchies, and the combined public
// key for the client.
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
    client_el_gamal_keys_.set_el_gamal_y(
        client_public_el_gamal_y_ec.ToBytesCompressed().value());
    client_el_gamal_keys_.set_el_gamal_g(duchy_1_el_gamal_keys_.el_gamal_y());

    any_sketch::CiphertextString client_public_key = {
        .u = duchy_1_el_gamal_keys_.el_gamal_y(),
        .e = client_public_el_gamal_y_ec.ToBytesCompressed().value(),
    };
    sketch_encrypter = any_sketch::CreateWithPublicKey(
                           kTestCurveId, kMaxFrequency, client_public_key)
                           .value();
  }
};

TEST(BlindOneLayerRegisterIndex, wrongInputSketchSizeShouldThrow) {
  BlindOneLayerRegisterIndexRequest request;
  *request.mutable_sketch() = "1234";
  auto result = BlindOneLayerRegisterIndex(request);
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status(),
              StatusIs(StatusCode::kInvalidArgument, "size of sketch"));
}

TEST(BlindOneLayerRegisterIndex, keyCountValuesShouldNotChange) {
  TestData test_data;
  Sketch plain_sketch = CreateEmptyClceSketch();
  AddRegister(&plain_sketch, /* index = */ 1, /* key = */ 111, /* count = */ 2);
  AddRegister(&plain_sketch, /* index = */ 2, /* key = */ 222, /* count = */ 1);
  AddRegister(&plain_sketch, /* index = */ 3, /* key = */ 333, /* count = */ 1);
  std::vector<unsigned char> encrypted_sketch =
      test_data.sketch_encrypter->Encrypt(plain_sketch).value();

  BlindOneLayerRegisterIndexRequest request;
  *request.mutable_local_pohlig_hellman_sk() = test_data.duchy_1_p_h_key_;
  *request.mutable_local_el_gamal_keys() = test_data.duchy_1_el_gamal_keys_;
  request.set_curve_id(kTestCurveId);
  request.mutable_sketch()->append(encrypted_sketch.begin(),
                                   encrypted_sketch.end());

  BlindOneLayerRegisterIndexResponse response =
      BlindOneLayerRegisterIndex(request).value();

  absl::string_view input_sketch = request.sketch();
  absl::string_view output_sketch = response.sketch();
  ASSERT_EQ(output_sketch.size(), kBytesPerEncryptedRegister * 3);
  for (int i = 0; i < 3; i++) {
    const int register_head = i * kBytesPerEncryptedRegister;
    // register index value should change.
    EXPECT_NE(input_sketch.substr(register_head, kBytesCipherText),
              output_sketch.substr(register_head, kBytesCipherText));
    // key and count should not change.
    EXPECT_EQ(input_sketch.substr(register_head + kBytesCipherText,
                                  kBytesCipherText * 2),
              output_sketch.substr(register_head + kBytesCipherText,
                                   kBytesCipherText * 2));
  }
}

}  // namespace
}  // namespace wfa::measurement::crypto