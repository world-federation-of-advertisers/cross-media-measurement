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

#include "wfa/panelmatch/client/privatemembership/testing/private_membership_helper.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "common_cpp/macros/macros.h"
#include "private_membership/rlwe/batch/cpp/client/client.h"
#include "private_membership/rlwe/batch/cpp/server/server.h"
#include "private_membership/rlwe/batch/proto/client.pb.h"
#include "private_membership/rlwe/batch/proto/server.pb.h"
#include "private_membership/rlwe/batch/proto/shared.pb.h"

namespace wfa::panelmatch::client::privatemembership {
namespace {
using ::private_membership::batch::ApplyQueriesRequest;
using ::private_membership::batch::ApplyQueriesResponse;
using ::private_membership::batch::DecryptedQueryResult;
using ::private_membership::batch::DecryptQueriesRequest;
using ::private_membership::batch::DecryptQueriesResponse;
using ::private_membership::batch::EncryptedQueryResult;
using ::private_membership::batch::EncryptQueries;
using ::private_membership::batch::EncryptQueriesRequest;
using ::private_membership::batch::EncryptQueriesResponse;
using ::private_membership::batch::GenerateKeysRequest;
using ::private_membership::batch::GenerateKeysResponse;
using ::private_membership::batch::Parameters;
using ::private_membership::batch::PlaintextQuery;
using ::private_membership::batch::PrivateKey;
using ::private_membership::batch::PublicKey;
using ::private_membership::batch::QueryMetadata;
using ::private_membership::batch::RawDatabaseShard;

constexpr std::array<int, 3> kTestQueryIds = {0, 1, 2};
constexpr std::array<int, 3> kTestShardIds = {2, 3, 1};
constexpr std::array<int, 3> kTestBucketIds = {41, 14, 22};

std::vector<PlaintextQuery> CreateTestPlaintextQueries() {
  std::vector<PlaintextQuery> test_plaintext_queries(kTestQueryIds.size());

  for (int i = 0; i < kTestQueryIds.size(); ++i) {
    // Set query metadata.
    QueryMetadata* query_metadata =
        test_plaintext_queries[i].mutable_query_metadata();
    query_metadata->set_query_id(kTestQueryIds[i]);
    query_metadata->set_shard_id(kTestShardIds[i]);

    // Set bucket ID.
    test_plaintext_queries[i].set_bucket_id(kTestBucketIds[i]);
  }

  return test_plaintext_queries;
}

Parameters CreateTestParameters() {
  Parameters parameters;

  Parameters::ShardParameters* shard_parameters =
      parameters.mutable_shard_parameters();
  shard_parameters->set_number_of_shards(4);
  shard_parameters->set_number_of_buckets_per_shard(50);

  // Example test parameters for the underlying RLWE cryptosystem. The security
  // of these parameters may be calculating using the code found at:
  // https://bitbucket.org/malb/lwe-estimator/src/master/
  Parameters::CryptoParameters* crypto_parameters =
      parameters.mutable_crypto_parameters();
  crypto_parameters->add_request_modulus(18446744073708380161ULL);
  crypto_parameters->add_request_modulus(137438953471ULL);
  crypto_parameters->add_response_modulus(2056193ULL);
  crypto_parameters->set_log_degree(12);
  crypto_parameters->set_log_t(1);
  crypto_parameters->set_variance(8);
  crypto_parameters->set_levels_of_recursion(2);
  crypto_parameters->set_log_compression_factor(4);
  crypto_parameters->set_log_decomposition_modulus(10);

  return parameters;
}
}  // namespace

absl::StatusOr<DecryptQueriesRequest> CreateTestDecryptQueriesRequest(
    std::array<absl::string_view, 3> test_buckets) {
  Parameters parameters = CreateTestParameters();

  GenerateKeysRequest keys_request;
  *keys_request.mutable_parameters() = CreateTestParameters();
  ASSIGN_OR_RETURN(GenerateKeysResponse keys_response,
                   GenerateKeys(keys_request));
  const PrivateKey& private_key = keys_response.private_key();
  const PublicKey& public_key = keys_response.public_key();

  EncryptQueriesRequest encrypt_queries_request;
  *encrypt_queries_request.mutable_parameters() = parameters;
  *encrypt_queries_request.mutable_public_key() = public_key;
  *encrypt_queries_request.mutable_private_key() = private_key;

  std::vector<PlaintextQuery> test_plaintext_queries =
      CreateTestPlaintextQueries();
  *encrypt_queries_request.mutable_plaintext_queries() = {
      test_plaintext_queries.begin(), test_plaintext_queries.end()};

  ASSIGN_OR_RETURN(EncryptQueriesResponse encrypt_queries_response,
                   EncryptQueries(encrypt_queries_request));

  ApplyQueriesRequest apply_queries_request;
  *apply_queries_request.mutable_parameters() = parameters;
  *apply_queries_request.mutable_public_key() = public_key;
  *apply_queries_request.add_queries() =
      encrypt_queries_response.encrypted_queries();
  apply_queries_request.set_finalize_results(true);

  for (int i = 0; i < test_buckets.size(); ++i) {
    RawDatabaseShard* shard =
        apply_queries_request.mutable_raw_database()->add_shards();
    shard->set_shard_index(kTestShardIds[i]);
    RawDatabaseShard::Bucket* bucket = shard->add_buckets();
    bucket->set_bucket_id(kTestBucketIds[i]);
    *bucket->mutable_bucket_contents() = test_buckets[i];
  }

  ASSIGN_OR_RETURN(ApplyQueriesResponse apply_queries_response,
                   ApplyQueries(apply_queries_request));

  DecryptQueriesRequest request;
  *request.mutable_parameters() = parameters;
  *request.mutable_private_key() = private_key;
  *request.mutable_public_key() = public_key;
  *request.mutable_encrypted_queries() = apply_queries_response.query_results();
  return request;
}

}  // namespace wfa::panelmatch::client::privatemembership
