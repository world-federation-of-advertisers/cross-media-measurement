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

// Copied from
// https://github.com/google/private-membership/blob/main/private_membership/rlwe/batch/cpp/client/client_test.cc
// TODO(stevenwarejones) This file really should be deleted. However, currently
// there are not client libraries in github.com/google/private-membership that
// we can use for testing.
#include "wfa/panelmatch/client/privatemembership/testing/private_membership_helper.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "./galois_key.h"
#include "./oblivious_expand.h"
#include "./polynomial.h"
#include "./serialization.pb.h"
#include "./symmetric_encryption.h"
#include "./symmetric_encryption_with_prng.h"
#include "./transcription.h"
#include "private_membership/rlwe/batch/cpp/client/client.h"
#include "private_membership/rlwe/batch/cpp/client/client.pb.h"
#include "private_membership/rlwe/batch/cpp/client/client_helper.h"
#include "private_membership/rlwe/batch/cpp/shared.h"
#include "private_membership/rlwe/batch/cpp/shared.pb.h"

namespace wfa::panelmatch::client::privatemembership::testing {

using ::private_membership::batch::Context;
using ::private_membership::batch::CreatePrng;
using ::private_membership::batch::DecryptedQueryResult;
using ::private_membership::batch::DecryptQueriesRequest;
using ::private_membership::batch::DecryptQueriesResponse;
using ::private_membership::batch::EncryptedQueryResult;
using ::private_membership::batch::GenerateKeysRequest;
using ::private_membership::batch::GenerateKeysResponse;
using ::private_membership::batch::Int;
using ::private_membership::batch::ModularInt;
using ::private_membership::batch::Parameters;
using ::private_membership::batch::PlaintextQuery;
using ::private_membership::batch::PrependLength;
using ::private_membership::batch::QueryMetadata;

constexpr int kNumberOfShards = 16;
constexpr int kNumberOfBucketsPerShard = 200;
constexpr int kLevelsOfRecursion = 2;

constexpr std::array<int, 3> kTestQueryIds = {0, 1, 2};
constexpr std::array<int, 3> kTestShardIds = {8, 3, 11};
constexpr std::array<int, 3> kTestBucketIds = {173, 14, 82};

absl::StatusOr<Int> CreateModulus(
    const google::protobuf::RepeatedField<uint64_t>& serialized_modulus) {
  if (serialized_modulus.empty()) {
    return absl::InvalidArgumentError("No modulus serialized.");
  } else if (serialized_modulus.size() > 2) {
    return absl::InvalidArgumentError("Modulus does not fit into uint128.");
  } else if (serialized_modulus.size() == 2) {
    return static_cast<rlwe::Uint128>(
        absl::MakeUint128(serialized_modulus.at(1), serialized_modulus.at(0)));
  } else {
    return static_cast<rlwe::Uint128>(
        absl::MakeUint128(0, serialized_modulus.at(0)));
  }
}

std::vector<PlaintextQuery> CreateTestPlaintextQueries() {
  std::vector<PlaintextQuery> test_plaintext_queries(kTestQueryIds.size());

  for (int i = 0; i < kTestQueryIds.size(); ++i) {
    // Set query metadata.
    QueryMetadata* query_metadata =
        test_plaintext_queries[i].mutable_query_metadata();
    query_metadata->set_query_id(kTestQueryIds.at(i));
    query_metadata->set_shard_id(kTestShardIds.at(i));

    // Set bucket ID.
    test_plaintext_queries[i].set_bucket_id(kTestBucketIds.at(i));
  }

  return test_plaintext_queries;
}

absl::StatusOr<rlwe::SymmetricRlweKey<ModularInt>> DeserializePrivateKey(
    const rlwe::SerializedNttPolynomial& serialization,
    const Context& context) {
  return rlwe::SymmetricRlweKey<ModularInt>::Deserialize(
      context.GetVariance(), context.GetLogT(), serialization,
      context.GetModulusParams(), context.GetNttParams());
}

Parameters CreateTestParameters() {
  Parameters parameters;

  Parameters::ShardParameters* shard_parameters =
      parameters.mutable_shard_parameters();
  shard_parameters->set_number_of_shards(kNumberOfShards);
  shard_parameters->set_number_of_buckets_per_shard(kNumberOfBucketsPerShard);

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
  crypto_parameters->set_levels_of_recursion(kLevelsOfRecursion);
  crypto_parameters->set_log_compression_factor(4);
  crypto_parameters->set_log_decomposition_modulus(10);

  return parameters;
}

GenerateKeysRequest CreateTestGenerateKeysRequest() {
  GenerateKeysRequest request;
  *(request.mutable_parameters()) = CreateTestParameters();
  return request;
}

absl::StatusOr<DecryptQueriesRequest> CreateTestDecryptQueriesRequest(
    std::array<absl::string_view, 3> kTestBuckets) {
  GenerateKeysRequest keys_request = CreateTestGenerateKeysRequest();

  absl::StatusOr<GenerateKeysResponse> keys_response =
      GenerateKeys(keys_request);
  if (!keys_response.ok()) {
    return keys_response.status();
  }

  DecryptQueriesRequest request;
  *request.mutable_parameters() = CreateTestParameters();
  *request.mutable_private_key() = keys_response->private_key();
  *request.mutable_public_key() = keys_response->public_key();

  auto response_context = CreateRlweResponseContext(request.parameters());
  if (!response_context.ok()) {
    return response_context.status();
  }
  auto private_key = DeserializePrivateKey(request.private_key().response_key(),
                                           **response_context);
  if (!private_key.ok()) {
    return private_key.status();
  }

  std::vector<PlaintextQuery> test_plaintext_queries =
      CreateTestPlaintextQueries();
  for (int i = 0; i < test_plaintext_queries.size(); ++i) {
    EncryptedQueryResult* encrypted_result = request.add_encrypted_queries();
    *encrypted_result->mutable_query_metadata() =
        test_plaintext_queries[i].query_metadata();
    int bytes_per_ciphertext =
        ((*response_context)->GetN() * (*response_context)->GetLogT()) / 8;
    std::string length_prepended_bucket = PrependLength(kTestBuckets[i]);
    std::vector<uint8_t> padded_plaintext(bytes_per_ciphertext, '\0');
    std::copy(length_prepended_bucket.begin(), length_prepended_bucket.end(),
              padded_plaintext.begin());
    absl::StatusOr<std::vector<ModularInt::Int>> transcribed =
        rlwe::TranscribeBits<uint8_t, ModularInt::Int>(
            padded_plaintext, padded_plaintext.size() * 8, 8,
            (*response_context)->GetLogT());
    if (!transcribed.ok()) {
      return transcribed.status();
    }
    std::vector<ModularInt> transcribed_coeffs;
    transcribed_coeffs.reserve(transcribed->size());
    for (const ModularInt::Int& coeff : *transcribed) {
      absl::StatusOr<ModularInt> coeff_modular_int =
          ModularInt::ImportInt(coeff, (*response_context)->GetModulusParams());
      if (!coeff_modular_int.ok()) {
        return coeff_modular_int.status();
      }
      transcribed_coeffs.push_back(*std::move(coeff_modular_int));
    }
    rlwe::Polynomial<ModularInt> ntt_polynomial =
        rlwe::Polynomial<ModularInt>::ConvertToNtt(
            transcribed_coeffs, (*response_context)->GetNttParams(),
            (*response_context)->GetModulusParams());
    auto prng = CreatePrng();
    if (!prng.ok()) {
      return prng.status();
    }
    absl::StatusOr<rlwe::SymmetricRlweCiphertext<ModularInt>> encryption =
        rlwe::Encrypt(*private_key, ntt_polynomial,
                      (*response_context)->GetErrorParams(), prng->get());
    if (!encryption.ok()) {
      return encryption.status();
    }
    absl::StatusOr<rlwe::SerializedSymmetricRlweCiphertext>
        serialized_encryption = encryption->Serialize();
    if (!serialized_encryption.ok()) {
      return serialized_encryption.status();
    }
    *encrypted_result->add_ciphertexts() = *std::move(serialized_encryption);
  }

  return request;
}

}  // namespace wfa::panelmatch::client::privatemembership::testing
