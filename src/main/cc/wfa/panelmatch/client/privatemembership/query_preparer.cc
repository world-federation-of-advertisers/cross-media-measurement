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

#include "wfa/panelmatch/client/privatemembership/query_preparer.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "common_cpp/fingerprinters/fingerprinters.h"
#include "common_cpp/macros/macros.h"
#include "tink/util/secret_data.h"
#include "wfa/panelmatch/common/crypto/peppered_fingerprinter.h"

namespace wfa::panelmatch::client::privatemembership {
using ::crypto::tink::util::SecretData;
using ::crypto::tink::util::SecretDataAsStringView;
using ::crypto::tink::util::SecretDataFromStringView;

absl::StatusOr<PrepareQueryResponse> PrepareQuery(
    const PrepareQueryRequest& request) {
  if (request.identifier_hash_pepper().empty()) {
    return absl::InvalidArgumentError("Empty Identifier Hash Pepper");
  }
  const Fingerprinter& fingerprinter = GetSha256Fingerprinter();
  std::unique_ptr<Fingerprinter> peppered_fingerprinter =
      wfa::panelmatch::common::crypto::GetPepperedFingerprinter(
          &fingerprinter,
          SecretDataFromStringView(request.identifier_hash_pepper()));
  PrepareQueryResponse prepared_query;
  for (const std::string& single_blinded_key : request.single_blinded_keys()) {
    prepared_query.add_hashed_single_blinded_keys(
        peppered_fingerprinter->Fingerprint(single_blinded_key));
  }
  return prepared_query;
}
}  // namespace wfa::panelmatch::client::privatemembership
