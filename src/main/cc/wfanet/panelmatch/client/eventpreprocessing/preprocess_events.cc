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

#include "wfanet/panelmatch/client/eventpreprocessing/preprocess_events.h"

#include <string>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "wfanet/panelmatch/client/eventpreprocessing/preprocess_events.pb.h"
#include "wfanet/panelmatch/common/crypto/encryption_utility_helper.h"

namespace wfanet::panelmatch::client {
absl::StatusOr<PreprocessEventsResponse> PreprocessEvents(
    const PreprocessEventsRequest& request) {
  // TODO(juliamorrissey): call Erin's library
  return absl::UnimplementedError("Not implemented");
}
}  // namespace wfanet::panelmatch::client
