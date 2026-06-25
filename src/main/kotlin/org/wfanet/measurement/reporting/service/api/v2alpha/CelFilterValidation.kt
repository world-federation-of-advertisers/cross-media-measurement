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
 *   exist on the deployment Event message (e.g. an EDP retired the template, or the config points
 *   at the wrong message).
 * - **Unknown nested fields** -- the template exists but the field path does not (e.g.
 *   `banner_ad.legacy_field` after the field is removed).
 * - **Type-mismatched operands** -- `bool == float`, `string < int`, etc. This is the primary guard
 *   against [BasicReportTransformations.toCelValue] emitting a literal of the wrong shape for the
 *   field type (most realistically: a Base IQF configured with `floatValue` on a `bool` field; see
 *   [Why this exists] below).
 * - **Non-boolean result** -- the expression compiles but yields a non-bool (e.g. a bare message
 *   field access, a string literal, a numeric literal). RequisitionSpec filters MUST evaluate to
 *   bool; an EDP that receives a non-bool filter rejects the requisition with "does not evaluate to
 *   a boolean", and the BasicReport that produced it sits orphaned.
 *
 * An empty filter string is treated as valid (the dedup pass in
 * [BasicReportTransformations.buildReportingSetMetricCalculationSpecDetailsMap] can collapse all
 * terms to `""`, which means "no filter").
 *
 * ## What this does NOT need to catch (already enforced upstream)
 *
 * `CreateBasicReportRequestValidation` runs before the transformer and rejects:
 * - User-supplied `EventTemplateField` paths that do not exist on the event message
 *   (`EVENT_TEMPLATE_FIELD_INVALID`).
 * - User-supplied field paths whose template is not filterable / not a population attribute / not
 *   impression-qualification-capable, depending on context.
 * - User-supplied `enumValue` strings that do not match a defined enum value name.
 * - User-supplied `fieldValue` whose `selectorCase` does not match the field's protobuf type --
 *   `stringValue` for a `bool` field, `floatValue` for an `enum` field, etc.
 * - User-supplied media-type / template-field combinations that disagree.
 *
 * As of writing, every value-type produced by [BasicReportTransformations.toCelValue] from a
 * user-supplied (Custom / Named) IQF spec is therefore already type-consistent with the field on
 * the event message. The Custom / Named branches of this validator are present so that a future
 * regression in `CreateBasicReportRequestValidation` or `toCelValue` -- or the addition of a field
 * type whose round-trip is not type-safe (e.g. a `string` IQF field whose literal would need
 * quoting) -- surfaces as `INVALID_ARGUMENT` with a precise field path instead of an EDP
 * fulfillment failure.
 *
 * ## Why this exists
 *
 * The only realistic failure mode reachable today is a server-configured Base IQF
 * (`--base-impression-qualification-filters` plus `ImpressionQualificationFilterConfig`) whose spec
 * list translates to CEL that disagrees with the deployment Event message. The IQF mapping's own
 * validation in
 * [org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping] checks
 * the field path and media type but NOT the value-type alignment between
 * `EventTemplateField.FieldValue.selectorCase` and the protobuf field type. A misconfigured base
 * IQF with `floatValue` on a `bool` field passes the mapping check, reaches the transformer, and
 * emits CEL such as `banner_ad.viewable == 1.0` -- caught here, routed to `Status.INTERNAL` with
 * the IQF id in the message.
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
   * by [validateCelBoolean] for `IllegalStateException` messages where the field-path framing does
   * not apply.
   */
  private data class CelValidationIssue(val diagnostic: String, val fieldSuffix: String)
}
