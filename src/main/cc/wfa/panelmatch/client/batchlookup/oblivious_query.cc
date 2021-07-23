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
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "wfa/panelmatch/client/batchlookup/oblivious_query.pb.h"

namespace wfa::panelmatch::client::batchlookup {

absl::StatusOr<GenerateKeysResponse> GenerateKeys(
    const GenerateKeysRequest& request) {
  return absl::UnimplementedError("Not implemented");
}

absl::StatusOr<EncryptQueriesResponse> EncryptQueries(
    const EncryptQueriesRequest& request) {
  return absl::UnimplementedError("Not implemented");
}

absl::StatusOr<DecryptQueriesResponse> DecryptQueries(
    const DecryptQueriesRequest& request) {
  return absl::UnimplementedError("Not implemented");
}

}  // namespace wfa::panelmatch::client::batchlookup
