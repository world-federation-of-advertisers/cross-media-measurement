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

#include "wfa/panelmatch/client/batchlookup/oblivious_query.h"

#include <string>

#include "common_cpp/testing/status_macros.h"
#include "common_cpp/testing/status_matchers.h"
#include "wfa/panelmatch/client/batchlookup/oblivious_query.pb.h"
#include "wfa/panelmatch/client/batchlookup/oblivious_query_wrapper.h"

namespace wfa::panelmatch::client::batchlookup {
namespace {

TEST(ObliviousQuery, GenerateKeysTest) {
  GenerateKeysRequest test_request;
  auto test_response = GenerateKeys(test_request);
  EXPECT_THAT(test_response.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  std::string valid_serialized_request;
  test_request.SerializeToString(&valid_serialized_request);
  auto wrapper_test_response1 = GenerateKeysWrapper(valid_serialized_request);
  EXPECT_THAT(wrapper_test_response1.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  auto wrapper_test_response2 =
      GenerateKeysWrapper("some-invalid-serialized-request");
  EXPECT_THAT(wrapper_test_response2.status(),
              StatusIs(absl::StatusCode::kInternal, ""));
}

TEST(ObliviousQuery, EncryptQueriesTest) {
  EncryptQueriesRequest test_request;
  test_request.set_public_key("some-public-key");
  test_request.set_private_key("some-private-key");
  UnencryptedQuery* unencrypted_query = test_request.add_unencrypted_query();
  unencrypted_query->set_shard_id(1);
  unencrypted_query->set_query_id(2);
  unencrypted_query->set_bucket_id(3);
  auto test_response = EncryptQueries(test_request);
  EXPECT_THAT(test_response.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  std::string valid_serialized_request;
  test_request.SerializeToString(&valid_serialized_request);
  auto wrapper_test_response1 = EncryptQueriesWrapper(valid_serialized_request);
  EXPECT_THAT(wrapper_test_response1.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  auto wrapper_test_response2 =
      EncryptQueriesWrapper("some-invalid-serialized-request");
  EXPECT_THAT(wrapper_test_response2.status(),
              StatusIs(absl::StatusCode::kInternal, ""));
}

TEST(ObliviousQuery, DecryptQueriesTest) {
  DecryptQueriesRequest test_request;
  test_request.set_public_key("some-public-key");
  test_request.set_private_key("some-private-key");
  test_request.add_encrypted_query_results("some-encrypted-query-result");
  auto test_response = DecryptQueries(test_request);
  EXPECT_THAT(test_response.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  std::string valid_serialized_request;
  test_request.SerializeToString(&valid_serialized_request);
  auto wrapper_test_response1 = DecryptQueriesWrapper(valid_serialized_request);
  EXPECT_THAT(wrapper_test_response1.status(),
              StatusIs(absl::StatusCode::kUnimplemented, ""));

  auto wrapper_test_response2 =
      DecryptQueriesWrapper("some-invalid-serialized-request");
  EXPECT_THAT(wrapper_test_response2.status(),
              StatusIs(absl::StatusCode::kInternal, ""));
}
}  // namespace
}  // namespace wfa::panelmatch::client::batchlookup
