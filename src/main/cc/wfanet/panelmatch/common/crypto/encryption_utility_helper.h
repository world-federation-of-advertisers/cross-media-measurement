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

#ifndef SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
#define SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace wfanet::panelmatch::common::crypto {

template <typename T>
absl::Status ParseRequestFromString(const std::string& serialized_request,
                                    T& request_proto) {
  return request_proto.ParseFromString(serialized_request)
             ? absl::OkStatus()
             : absl::InternalError(
                   "failed to parse the serialized request proto.");
}

}  // namespace wfanet::panelmatch::common::crypto

#endif  // SRC_MAIN_CC_WFANET_PANELMATCH_COMMON_CRYPTO_ENCRYPTION_UTILITY_HELPER_H_
