// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

class PrivacyBudgetManagementInternalException : Exception {
  val code: Code

  constructor(code: Code) : super() {
    this.code = code
  }

  constructor(code: Code, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  enum class Code {
    /** Input bad. */
    BAD_CEL_EXPRESSION,
    /** Input bad. */
    INVALID_FILTER_FOR_PRIVACY,
  }
}
