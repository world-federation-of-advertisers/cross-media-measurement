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

package org.wfanet.measurement.eventdataprovider.eventfiltration.validation

class EventFilterValidationException(val code: Code, message: String) : Exception(message) {

  enum class Code(val description: String) {
    INVALID_CEL_EXPRESSION(
      "An expression that cannot be interpreted as a valid CEL due to syntax or typing."
    ),
    UNSUPPORTED_OPERATION(
      "Expression uses a CEL operation that currently is not supported for Halo."
    ),
    INVALID_VALUE_TYPE("A CEL value has a type that is invalid for Halo."),
    FIELD_COMPARISON_OUTSIDE_LEAF("Field comparison is not done within a leaf node."),
    EXPRESSION_IS_NOT_CONDITIONAL("The expression is a single value, not a condition."),
    INVALID_OPERATION_OUTSIDE_LEAF("Leaf-only operator used outside leaf."),
  }
}
