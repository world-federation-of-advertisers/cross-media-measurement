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

#include <memory>
#include <string>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common_cpp/macros/macros.h"
#include "private_membership/rlwe/batch/cpp/client/client.h"
#include "private_membership/rlwe/batch/proto/client.pb.h"
#include "wfa/panelmatch/client/privatemembership/event_data_decryptor.h"

namespace wfa::panelmatch::client::privatemembership {

namespace {
using ClientEncryptedQueryResult =
    ::private_membership::batch::EncryptedQueryResult;
using ClientDecryptedQueryResult =
    ::private_membership::batch::DecryptedQueryResult;
using ClientDecryptQueriesRequest =
    ::private_membership::batch::DecryptQueriesRequest;
using ClientDecryptQueriesResponse =
    ::private_membership::batch::DecryptQueriesResponse;
using ::private_membership::batch::DecryptQueries;
absl::StatusOr<ClientDecryptQueriesResponse> RemoveRlwe(
    const DecryptQueryResultsRequest& request) {
  ClientDecryptQueriesRequest client_decrypt_queries_request;
  client_decrypt_queries_request.mutable_parameters()->ParseFromString(
      request.serialized_parameters());
  client_decrypt_queries_request.mutable_private_key()->ParseFromString(
      request.serialized_private_key());
  client_decrypt_queries_request.mutable_public_key()->ParseFromString(
      request.serialized_public_key());
  for (const EncryptedQueryResult& encrypted_query_result :
       request.encrypted_query_results()) {
    ClientEncryptedQueryResult client_encrypted_query_result;
    client_encrypted_query_result.ParseFromString(
        encrypted_query_result.serialized_encrypted_query_result());
    client_decrypt_queries_request.add_encrypted_queries()->Swap(
        &client_encrypted_query_result);
  }
  ASSIGN_OR_RETURN(ClientDecryptQueriesResponse client_decrypt_queries_response,
                   DecryptQueries(client_decrypt_queries_request));
  return client_decrypt_queries_response;
}

absl::StatusOr<DecryptedEventDataSet> RemoveAesFromDecryptedQueryResult(
    const ClientDecryptedQueryResult& client_decrypted_query_result,
    const std::string& single_blinded_joinkey, const std::string& hkdf_pepper) {
  DecryptEventDataRequest decrypt_event_data_request;
  decrypt_event_data_request.set_hkdf_pepper(hkdf_pepper);
  decrypt_event_data_request.mutable_single_blinded_joinkey()->set_key(
      single_blinded_joinkey);
  EncryptedEventData encrypted_event_data;
  encrypted_event_data.ParseFromString(client_decrypted_query_result.result());
  decrypt_event_data_request.mutable_encrypted_event_data_set()
      ->mutable_query_id()
      ->set_id(client_decrypted_query_result.query_metadata().query_id());
  *decrypt_event_data_request.mutable_encrypted_event_data_set()
       ->mutable_encrypted_event_data()
       ->mutable_ciphertexts() =
      *std::move(encrypted_event_data.mutable_ciphertexts());
  return DecryptEventData(decrypt_event_data_request);
}

absl::StatusOr<DecryptQueryResultsResponse> RemoveAes(
    const DecryptQueryResultsRequest& request,
    const ClientDecryptQueriesResponse& client_decrypt_queries_response) {
  DecryptQueryResultsResponse result;
  for (const ClientDecryptedQueryResult& client_decrypted_query_result :
       client_decrypt_queries_response.result()) {
    ASSIGN_OR_RETURN(
        DecryptedEventDataSet decrypt_event_data_response,
        RemoveAesFromDecryptedQueryResult(
            client_decrypted_query_result,
            request.single_blinded_joinkey().key(), request.hkdf_pepper()));
    result.add_event_data_sets()->Swap(&decrypt_event_data_response);
  }
  return result;
}
}  // namespace

absl::StatusOr<DecryptQueryResultsResponse> DecryptQueryResults(
    const DecryptQueryResultsRequest& request) {
  // Step 1: Decrypt the encrypted query response
  ASSIGN_OR_RETURN(ClientDecryptQueriesResponse client_decrypt_queries_response,
                   RemoveRlwe(request));
  // Step 2: Decrypt the encrypted event data
  ASSIGN_OR_RETURN(DecryptQueryResultsResponse result,
                   RemoveAes(request, client_decrypt_queries_response));
  return result;
}
}  // namespace wfa::panelmatch::client::privatemembership
