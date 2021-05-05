// Copyright 2020 The Cross-Media Measurement Authors
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

#ifndef SRC_TEST_CC_TESTUTIL_MATCHERS_H_
#define SRC_TEST_CC_TESTUTIL_MATCHERS_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace wfanet {

MATCHER(IsOk, "") {
  return testing::ExplainMatchResult(true, arg.ok(), result_listener);
}

MATCHER(IsNotOk, "") {
  return testing::ExplainMatchResult(testing::Not(IsOk()), arg,
                                     result_listener);
}

MATCHER_P(IsOkAndHolds, value, "") {
  if (arg.ok()) {
    return testing::ExplainMatchResult(value, arg.value(), result_listener);
  }

  *result_listener << "expected OK status instead of error code "
                   << absl::StatusCodeToString(arg.status().code())
                   << " and message " << arg.status();
  return false;
}

MATCHER_P2(StatusIs, code, message, "") {
  if (arg.code() != code) {
    *result_listener << "Expected code: " << code << " but got code "
                     << arg.code();
    return false;
  }
  return testing::ExplainMatchResult(
      testing::HasSubstr(message), std::string(arg.message()), result_listener);
}

}  // namespace wfanet

#endif  // SRC_TEST_CC_TESTUTIL_MATCHERS_H_
