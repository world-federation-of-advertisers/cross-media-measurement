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
#include "wfanet/panelmatch/client/eventpreprocessing/preprocess_events_wrapper.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "util/status_macros.h"
#include "wfa/measurement/common/crypto/encryption_utility_helper.h"
#include "wfanet/panelmatch/client/eventpreprocessing/preprocess_events.h"
#include "wfanet/panelmatch/client/eventpreprocessing/preprocess_events.pb.h"
#include "wfanet/panelmatch/common/crypto/encryption_utility_helper.h"

namespace wfanet::panelmatch::client {
absl::StatusOr<std::string> PreprocessEvents(
    const std::string& serialized_request) {
  wfanet::panelmatch::client::PreprocessEventsRequest request_proto;

  RETURN_IF_ERROR(wfanet::panelmatch::common::crypto::ParseRequestFromString(
      serialized_request, request_proto));
  ASSIGN_OR_RETURN(PreprocessEventsResponse result,
                   PreprocessEvents(request_proto));
  return result.SerializeAsString();
}
}  // namespace wfanet::panelmatch::client
