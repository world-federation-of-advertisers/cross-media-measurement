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

#include "wfa/panelmatch/client/privatemembership/query_evaluator_wrapper.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "common_cpp/jni/jni_wrap.h"
#include "wfa/panelmatch/client/privatemembership/query_evaluator.h"
#include "wfa/panelmatch/client/privatemembership/query_evaluator.pb.h"

namespace wfa::panelmatch::client::privatemembership {
absl::StatusOr<std::string> ExecuteQueriesWrapper(
    const std::string& serialized_request) {
  return JniWrap(serialized_request, ExecuteQueries);
}
absl::StatusOr<std::string> CombineResultsWrapper(
    const std::string& serialized_request) {
  return JniWrap(serialized_request, CombineResults);
}
}  // namespace wfa::panelmatch::client::privatemembership
