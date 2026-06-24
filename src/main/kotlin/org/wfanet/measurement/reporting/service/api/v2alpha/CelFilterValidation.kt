/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.projectnessie.cel.Env
import org.projectnessie.cel.checker.Decls
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException

/**
 * Validates that a CEL filter string compiles against the given [Env] and returns a boolean.
 *
 * An empty [filter] is a no-op.
 *
 * @throws InvalidFieldValueException when [filter] fails to compile, references unknown fields, or
 *   does not return a boolean.
 */
fun validateCelBooleanFilter(env: Env, filter: String, fieldPath: String) {
  if (filter.isEmpty()) {
    return
  }
  val astAndIssues =
    try {
      env.compile(filter)
    } catch (_: NullPointerException) {
      throw InvalidFieldValueException(fieldPath) { "$it is not a valid CEL expression" }
    }
  if (astAndIssues.hasIssues()) {
    throw InvalidFieldValueException(fieldPath) {
      "$it is not a valid CEL expression: ${astAndIssues.issues}"
    }
  }
  if (astAndIssues.ast.resultType != Decls.Bool) {
    throw InvalidFieldValueException(fieldPath) { "$it does not evaluate to a boolean" }
  }
}
