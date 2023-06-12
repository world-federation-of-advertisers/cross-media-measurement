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

#ifndef SRC_MAIN_CC_WFA_PANELMATCH_CLIENT_BATCHLOOKUP_QUERY_EVALUATOR_WRAPPER_H_
#define SRC_MAIN_CC_WFA_PANELMATCH_CLIENT_BATCHLOOKUP_QUERY_EVALUATOR_WRAPPER_H_

#include <string>

#include "absl/status/statusor.h"

namespace wfa::panelmatch::client::batchlookup {

absl::StatusOr<std::string> ExecuteQueriesWrapper(
    const std::string& serialized_request);
absl::StatusOr<std::string> CombineResultsWrapper(
    const std::string& serialized_request);

}  // namespace wfa::panelmatch::client::batchlookup

#endif  // SRC_MAIN_CC_WFA_PANELMATCH_CLIENT_BATCHLOOKUP_QUERY_EVALUATOR_WRAPPER_H_
