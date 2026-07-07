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

package org.wfanet.measurement.common.cel

import org.projectnessie.cel.Env
import org.projectnessie.cel.checker.Decls

/**
 * Thrown by [CelFilterValidator] when a CEL filter string fails to compile or does not evaluate
 * to a boolean. Callers should catch and wrap into an appropriate service-layer exception.
 */
class CelValidationException(message: String) : Exception(message)

/**
 * Compile-checks CEL filter strings against a CEL [Env].
 *
 * A filter is rejected if any of the following hold:
 * - **Syntax errors** -- trailing operators, unmatched parens, missing operands.
 * - **Unknown top-level identifiers** -- the expression references a type that does not exist on
 *   the [Env]'s message declarations.
 * - **Unknown nested fields** -- the type exists but the field path does not.
 * - **Type-mismatched operands** -- `bool == float`, `string < int`, etc.
 * - **Non-boolean result** -- the expression compiles but yields a non-bool. Downstream consumers
 *   that expect a bool filter (e.g. RequisitionSpec filters, which cause an EDP to reject the
 *   requisition when the filter does not evaluate to a boolean) must reject up-front.
 *
 * An empty filter string is treated as valid (no-op).
 */
object CelFilterValidator {
  /**
   * Validates that [filter] compiles against [env] and evaluates to a boolean. An empty [filter]
   * is a no-op.
   *
   * @throws CelValidationException when [filter] fails to compile, references unknown fields,
   *   or does not return a boolean. The exception message describes the specific failure.
   */
  fun validateBoolean(env: Env, filter: String) {
    if (filter.isEmpty()) {
      return
    }
    val astAndIssues =
      try {
        env.compile(filter)
      } catch (_: NullPointerException) {
        // CEL throws NPE when the filter uses a non-CEL operator. Same swallowing pattern as
        // filterList in this package; treat as a syntax error.
        throw CelValidationException("not a valid CEL expression")
      }
    if (astAndIssues.hasIssues()) {
      throw CelValidationException("not a valid CEL expression: ${astAndIssues.issues}")
    }
    if (astAndIssues.ast.resultType != Decls.Bool) {
      throw CelValidationException("does not evaluate to a boolean")
    }
  }
}
