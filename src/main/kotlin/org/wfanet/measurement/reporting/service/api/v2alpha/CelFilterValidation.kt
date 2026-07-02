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
 * Compile-checks CEL filter strings produced by [BasicReportTransformations] against a deployment
 * event template before they are persisted into internal MetricCalculationSpec rows.
 *
 * ## What this catches
 *
 * Each generated CEL string is rejected if any of the following hold:
 * - **Syntax errors** -- trailing operators, unmatched parens, missing operands. Surfaces as `"is
 *   not a valid CEL expression"`.
 * - **Unknown top-level identifiers** -- the expression references an event template that does not
 *   exist on the deployment Event message.
 * - **Unknown nested fields** -- the template exists but the field path does not.
 * - **Type-mismatched operands** -- `bool == float`, `string < int`, etc.
 * - **Non-boolean result** -- the expression compiles but yields a non-bool (e.g. a bare message
 *   field access, a string literal, a numeric literal). RequisitionSpec filters MUST evaluate to
 *   bool; an EDP that receives a non-bool filter rejects the requisition with `"does not evaluate
 *   to a boolean"`, and the BasicReport that produced it sits orphaned.
 *
 * An empty filter string is treated as valid (the dedup pass in
 * [BasicReportTransformations.buildReportingSetMetricCalculationSpecDetailsMap] can collapse all
 * terms to `""`, which means "no filter").
 *
 * ## Relationship to upstream validators
 *
 * Two other layers reject bad filter shapes before generated CEL reaches this validator:
 * 1. [CreateBasicReportRequestValidation.validateRequest] rejects user-supplied filter shapes:
 *    unknown field paths, invalid enum names, `selectorCase` mismatches with the field's protobuf
 *    type (including an unconditional rejection of `FLOAT_VALUE` from user input), and media-type /
 *    template-field disagreements.
 * 2. [org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping]
 *    validates server-configured base and named IQFs at startup, including value-type alignment
 *    (`isImpressionQualificationFilterSpecValid`). A base IQF with `floatValue` on a `bool` field
 *    crashes the server at boot, not at CreateBasicReport time.
 *
 * On the current test event templates (`TestEvent`, `MarketEvent`), every filter shape a caller can
 * construct is rejected upstream, so this validator is effectively a no-op for reachable user input
 * today. The reachability-boundary tests in `BasicReportTransformationsTest` document this
 * empirically.
 *
 * ## Why this exists (defense-in-depth)
 *
 * This validator is a backstop against three classes of future or latent problem:
 * 1. **Regressions in the upstream validators.** If `validateEventTemplateFieldValue` is ever
 *    weakened (e.g. by starting to accept `FLOAT_VALUE` from user input), or if
 *    `ImpressionQualificationFilterMapping.validate` stops enforcing value-type alignment, callers
 *    can send values that produce malformed CEL. Without this validator, EDP requisitions fulfill
 *    with `"does not evaluate to a boolean"` errors while the caller's BasicReport shows SUCCEEDED
 *    with empty results.
 * 2. **Additions to the [InternalEventTemplateField.FieldValue] oneof.** A new `selectorCase` (e.g.
 *    `int64Value`) would require corresponding branches in both upstream validators AND in
 *    [BasicReportTransformations.toCelValue]. Any branch that emits CEL of the wrong shape for the
 *    target field's protobuf type is caught here.
 * 3. **Deployments that add STRING or FLOAT-typed IMPRESSION_QUALIFICATION fields.**
 *    [BasicReportTransformations.toCelValue]'s `STRING_VALUE` branch emits `stringValue` raw
 *    (unquoted) and its `FLOAT_VALUE` branch emits `.toString()` (which renders `Float.NaN` as the
 *    identifier `NaN`, not a literal). Neither shape has been exercised in the tree because the
 *    default `TestEvent` and `MarketEvent` templates have no STRING IQF field and no FLOAT IQF
 *    field. A deployment whose event template adds one hits the shielded bug. The correct fix is in
 *    `toCelValue` itself; see #4148. Until that fix lands, this validator surfaces the
 *    problem as `INVALID_ARGUMENT` (Custom source) or `Status.INTERNAL` (Base / Named source) with
 *    a precise diagnostic instead of an unfulfilled requisition.
 */
object CelFilterValidation {
  /**
   * Validates that a CEL filter string compiles against the given [env] and returns a boolean.
   *
   * An empty [filter] is a no-op.
   *
   * @throws InvalidFieldValueException when [filter] fails to compile, references unknown fields,
   *   or does not return a boolean.
   */
  fun validateCelBooleanFilter(env: Env, filter: String, fieldPath: String) {
    val issue = checkCelBoolean(env, filter) ?: return
    throw InvalidFieldValueException(fieldPath) { "$it ${issue.fieldSuffix}" }
  }

  /**
   * Validates that a CEL filter string compiles against the given [env] and returns a boolean.
   *
   * An empty [filter] is a no-op. When validation fails, throws the exception built by
   * [buildException] from the CEL diagnostic. Use for CEL whose source is server-controlled (a
   * configuration error, not user input); callers should construct a typed [ServiceException]
   * routed to `Status.INTERNAL`.
   */
  fun validateCelBoolean(env: Env, filter: String, buildException: (issue: String) -> Throwable) {
    val issue = checkCelBoolean(env, filter) ?: return
    throw buildException(issue.diagnostic)
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
   * Diagnostic from a single CEL validation pass, in two formats so the caller can compose the
   * right exception message without re-running the check.
   *
   * [fieldSuffix] reads naturally after a field path -- `"$fieldPath $fieldSuffix"` produces e.g.
   * `"basic_report.…[2].custom is not a valid CEL expression"`. Used by [validateCelBooleanFilter]
   * to feed [InvalidFieldValueException]'s `buildMessage` callback.
   *
   * [diagnostic] is the same content phrased as a standalone clause -- `"not a valid CEL
   * expression"` -- so a caller-built message can splice it in: `"foo bar baz: $diagnostic"`. Used
   * by [validateCelBoolean] for caller-built exception messages (see
   * [ImpressionQualificationFilterInvalidCelException]) where the field-path framing does not
   * apply.
   */
  private data class CelValidationIssue(val diagnostic: String, val fieldSuffix: String)
}
