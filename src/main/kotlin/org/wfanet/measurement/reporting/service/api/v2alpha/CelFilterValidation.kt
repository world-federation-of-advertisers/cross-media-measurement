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
  val issue = checkCelBoolean(env, filter) ?: return
  throw InvalidFieldValueException(fieldPath) { "$it ${issue.fieldSuffix}" }
}

/**
 * Validates that a CEL filter string compiles against the given [Env] and returns a boolean.
 *
 * An empty [filter] is a no-op. Throws [IllegalStateException] with a message built by
 * [buildMessage] from the diagnostic when validation fails. Use for CEL whose source is server-
 * controlled (a configuration error, not user input).
 */
fun validateCelBoolean(env: Env, filter: String, buildMessage: (issue: String) -> String) {
  val issue = checkCelBoolean(env, filter) ?: return
  throw IllegalStateException(buildMessage(issue.diagnostic))
}

/**
 * Returns a [CelValidationIssue] describing why [filter] failed to validate, or `null` if it
 * compiles and returns a boolean. An empty [filter] is treated as valid.
 */
private fun checkCelBoolean(env: Env, filter: String): CelValidationIssue? {
  if (filter.isEmpty()) {
    return null
  }
  val astAndIssues =
    try {
      env.compile(filter)
    } catch (_: NullPointerException) {
      // CEL throws NPE when the filter uses a non-CEL operator. Same swallowing pattern as
      // CelFilteringMethods.filterList; treat as a syntax error.
      return CelValidationIssue(
        diagnostic = "not a valid CEL expression",
        fieldSuffix = "is not a valid CEL expression",
      )
    }
  if (astAndIssues.hasIssues()) {
    val issues = astAndIssues.issues.toString()
    return CelValidationIssue(
      diagnostic = "not a valid CEL expression: $issues",
      fieldSuffix = "is not a valid CEL expression: $issues",
    )
  }
  if (astAndIssues.ast.resultType != Decls.Bool) {
    return CelValidationIssue(
      diagnostic = "does not evaluate to a boolean",
      fieldSuffix = "does not evaluate to a boolean",
    )
  }
  return null
}

/**
 * Diagnostic from a single CEL validation pass, in two formats so the caller can compose the right
 * exception message without re-running the check.
 *
 * [fieldSuffix] reads naturally after a field path -- `"$fieldPath $fieldSuffix"` produces e.g.
 * `"basic_report.…[2].custom is not a valid CEL expression"`. Used by [validateCelBooleanFilter] to
 * feed [InvalidFieldValueException]'s `buildMessage` callback.
 *
 * [diagnostic] is the same content phrased as a standalone clause -- `"not a valid CEL expression"`
 * -- so a caller-built message can splice it in: `"foo bar baz: $diagnostic"`. Used by
 * [validateCelBoolean] for `IllegalStateException` messages where the field-path framing does not
 * apply.
 */
private data class CelValidationIssue(val diagnostic: String, val fieldSuffix: String)
