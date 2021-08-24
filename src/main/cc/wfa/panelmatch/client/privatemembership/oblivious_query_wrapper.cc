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

#include "wfa/panelmatch/client/privatemembership/oblivious_query_wrapper.h"

#include <string>

#include "absl/status/statusor.h"
#include "common_cpp/jni/jni_wrap.h"
#include "private_membership/rlwe/batch/cpp/client/client.h"
#include "private_membership/rlwe/batch/cpp/client/client.pb.h"

namespace wfa::panelmatch::client::privatemembership {
using ::private_membership::batch::DecryptQueries;
using ::private_membership::batch::EncryptQueries;
using ::private_membership::batch::GenerateKeys;

absl::StatusOr<std::string> GenerateKeysWrapper(
    const std::string& serialized_request) {
  return JniWrap(serialized_request, GenerateKeys);
}

absl::StatusOr<std::string> EncryptQueriesWrapper(
    const std::string& serialized_request) {
  return JniWrap(serialized_request, EncryptQueries);
}

absl::StatusOr<std::string> DecryptQueriesWrapper(
    const std::string& serialized_request) {
  return JniWrap(serialized_request, DecryptQueries);
}

}  // namespace wfa::panelmatch::client::privatemembership
