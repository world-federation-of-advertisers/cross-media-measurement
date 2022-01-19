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

#include <google/protobuf/repeated_field.h>

#include <string>

#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/client/exchangetasks/join_key.pb.h"
#include "wfa/panelmatch/client/privatemembership/decrypt_event_data.pb.h"
#include "wfa/panelmatch/client/privatemembership/event_data_decryptor.h"
#include "wfa/panelmatch/client/privatemembership/event_data_decryptor_wrapper.h"
#include "wfa/panelmatch/client/privatemembership/query.pb.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::client::privatemembership {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;
using ::wfa::EqualsProto;
using ::wfa::panelmatch::client::exchangetasks::JoinKey;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::Hkdf;

TEST(DecryptEventData, DecryptEventDataTest) {
  std::string hkdf_pepper = "some-pepper";
  std::string key = "some-single-blinded-JoinKey";
  JoinKey lookup_key;
  lookup_key.set_key(key);
  std::string plaintext = "Some data to encrypt.";

  // We first generate a valid ciphertext
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext,
      aes_hkdf.Encrypt(plaintext, SecretDataFromStringView(key),
                       SecretDataFromStringView(hkdf_pepper)));

  DecryptEventDataRequest test_request;
  test_request.set_hkdf_pepper(hkdf_pepper);
  test_request.mutable_lookup_key()->set_key(key);
  test_request.mutable_encrypted_event_data_set()
      ->mutable_encrypted_event_data()
      ->add_ciphertexts(ciphertext);
  test_request.mutable_encrypted_event_data_set()->mutable_query_id()->set_id(
      1);

  absl::StatusOr<DecryptedEventDataSet> test_response =
      DecryptEventData(test_request);
  DecryptedEventDataSet expected_response;
  expected_response.mutable_query_id()->set_id(1);
  Plaintext *expected_event_data = expected_response.add_decrypted_event_data();
  expected_event_data->set_payload(plaintext);
  EXPECT_THAT(test_response, IsOkAndHolds(EqualsProto(expected_response)));

  std::string valid_serialized_request;
  test_request.SerializeToString(&valid_serialized_request);
  absl::StatusOr<std::string> wrapper_test_response1 =
      DecryptEventDataWrapper(valid_serialized_request);
  EXPECT_THAT(wrapper_test_response1.status(), IsOk());

  absl::StatusOr<std::string> wrapper_test_response2 =
      DecryptEventDataWrapper("some-invalid-serialized-request");
  EXPECT_THAT(wrapper_test_response2.status(),
              StatusIs(absl::StatusCode::kInternal, ""));
}

}  // namespace
}  // namespace wfa::panelmatch::client::privatemembership
