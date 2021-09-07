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

#include "wfa/panelmatch/client/privatemembership/decrypt_query_results.h"

#include <google/protobuf/repeated_field.h>

#include <string>

#include "common_cpp/testing/common_matchers.h"
#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "private_membership/rlwe/batch/cpp/client/client.h"
#include "private_membership/rlwe/batch/cpp/client/client.pb.h"
#include "private_membership/rlwe/batch/cpp/client/client_helper.h"
#include "private_membership/rlwe/batch/cpp/shared.h"
#include "private_membership/rlwe/batch/cpp/shared.pb.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/client/privatemembership/decrypt_event_data.pb.h"
#include "wfa/panelmatch/client/privatemembership/decrypt_query_results_wrapper.h"
#include "wfa/panelmatch/client/privatemembership/testing/private_membership_helper.h"
#include "wfa/panelmatch/common/crypto/aes.h"
#include "wfa/panelmatch/common/crypto/aes_with_hkdf.h"
#include "wfa/panelmatch/common/crypto/hkdf.h"

namespace wfa::panelmatch::client::privatemembership {
namespace {

using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataFromStringView;
using ::google::protobuf::RepeatedPtrField;
using ::testing::Eq;
using ::testing::Pointwise;
using ::wfa::EqualsProto;
using ::wfa::panelmatch::common::crypto::Aes;
using ::wfa::panelmatch::common::crypto::AesWithHkdf;
using ::wfa::panelmatch::common::crypto::GetAesSivCmac512;
using ::wfa::panelmatch::common::crypto::GetSha256Hkdf;
using ::wfa::panelmatch::common::crypto::Hkdf;
using ClientDecryptQueriesRequest =
    ::private_membership::batch::DecryptQueriesRequest;
using ClientEncryptedQueryResult =
    ::private_membership::batch::EncryptedQueryResult;
using ::wfa::panelmatch::client::privatemembership::testing::
    CreateTestDecryptQueriesRequest;

TEST(DecryptQueryResults, DecryptQueryResultsTest) {
  std::string hkdf_pepper = "some-pepper";
  std::string key = "some-single-blinded-joinkey";
  JoinKey single_blinded_joinkey;
  single_blinded_joinkey.set_key(key);
  std::string plaintext1 = "Some data to encrypt 1.";
  std::string plaintext2 = "Some data to encrypt 2.";
  std::string plaintext3 = "Some data to encrypt 3.";

  // We first generate a valid ciphertext
  std::unique_ptr<Hkdf> hkdf = GetSha256Hkdf();
  std::unique_ptr<Aes> aes = GetAesSivCmac512();
  AesWithHkdf aes_hkdf(std::move(hkdf), std::move(aes));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext1,
      aes_hkdf.Encrypt(plaintext1, SecretDataFromStringView(key),
                       SecretDataFromStringView(hkdf_pepper)));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext2,
      aes_hkdf.Encrypt(plaintext2, SecretDataFromStringView(key),
                       SecretDataFromStringView(hkdf_pepper)));
  ASSERT_OK_AND_ASSIGN(
      std::string ciphertext3,
      aes_hkdf.Encrypt(plaintext3, SecretDataFromStringView(key),
                       SecretDataFromStringView(hkdf_pepper)));

  std::array<absl::string_view, 3> kTestBuckets = {ciphertext1, ciphertext2,
                                                   ciphertext3};
  ASSERT_OK_AND_ASSIGN(ClientDecryptQueriesRequest request,
                       CreateTestDecryptQueriesRequest(kTestBuckets));

  DecryptQueryResultsRequest test_request;
  test_request.set_hkdf_pepper(hkdf_pepper);
  test_request.mutable_single_blinded_joinkey()->set_key(key);
  test_request.set_serialized_private_key(
      request.private_key().SerializeAsString());
  test_request.set_serialized_public_key(
      request.public_key().SerializeAsString());
  test_request.set_serialized_parameters(
      request.parameters().SerializeAsString());
  for (const ClientEncryptedQueryResult &client_encrypted_query_result :
       request.encrypted_queries()) {
    EncryptedQueryResult *encrypted_query_result =
        test_request.add_encrypted_query_results();
    for (const auto &ciphertext : client_encrypted_query_result.ciphertexts()) {
      encrypted_query_result->add_ciphertexts(ciphertext.SerializeAsString());
    }
    encrypted_query_result->mutable_query_id()->set_id(
        client_encrypted_query_result.query_metadata().query_id());
    encrypted_query_result->mutable_shard_id()->set_id(
        client_encrypted_query_result.query_metadata().shard_id());
  }

  absl::StatusOr<DecryptQueryResultsResponse> test_response =
      DecryptQueryResults(test_request);
  DecryptQueryResultsResponse expected_response;
  DecryptedEventData *expected_decrypted_event_data1 =
      expected_response.add_decrypted_event_data();
  expected_decrypted_event_data1->set_plaintext(plaintext1);
  expected_decrypted_event_data1->mutable_query_id()->set_id(0);
  expected_decrypted_event_data1->mutable_shard_id()->set_id(8);
  DecryptedEventData *expected_decrypted_event_data2 =
      expected_response.add_decrypted_event_data();
  expected_decrypted_event_data2->set_plaintext(plaintext2);
  expected_decrypted_event_data2->mutable_query_id()->set_id(1);
  expected_decrypted_event_data2->mutable_shard_id()->set_id(3);
  DecryptedEventData *expected_decrypted_event_data3 =
      expected_response.add_decrypted_event_data();
  expected_decrypted_event_data3->set_plaintext(plaintext3);
  expected_decrypted_event_data3->mutable_query_id()->set_id(2);
  expected_decrypted_event_data3->mutable_shard_id()->set_id(11);
  EXPECT_THAT(test_response, IsOkAndHolds(EqualsProto(expected_response)));
}

}  // namespace
}  // namespace wfa::panelmatch::client::privatemembership
