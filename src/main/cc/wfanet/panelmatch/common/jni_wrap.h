/*
 * Copyright 2021 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_JNI_WRAP_H_
#define SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_JNI_WRAP_H_

#include <string>

#include "absl/status/statusor.h"
#include "util/status_macros.h"
#include "wfanet/panelmatch/common/deserialize_proto.h"

namespace wfanet::panelmatch::common {

// Deserializes `serialized_request`, then calls `inner_function` on the
// resulting proto, then serializes and returns the result.
template <typename Request, typename Response>
absl::StatusOr<std::string> JniWrap(
    const std::string& serialized_request,
    absl::StatusOr<Response> (*inner_function)(const Request&)) {
  ASSIGN_OR_RETURN(auto request, DeserializeProto<Request>(serialized_request));
  ASSIGN_OR_RETURN(Response response, inner_function(request));
  return response.SerializeAsString();
}

}  // namespace wfanet::panelmatch::common

#endif  // SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_JNI_WRAP_H_
