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

package org.wfanet.measurement.edpaggregator.validation

import kotlin.math.abs
import org.wfanet.measurement.config.edpaggregator.ToleranceBand
import org.wfanet.measurement.config.edpaggregator.ToleranceConfig

/**
 * Evaluates the deviation between a reported impression count and the expected count.
 *
 * The expected count is the publisher's source-of-truth count, already scaled to the EDPA's measured
 * population by the caller. This is a pure two-count comparison: scaling and any minimum-count
 * gating are the caller's responsibility. The deviation is checked against the configured warning
 * and failure tolerance bands.
 */
object ToleranceEvaluator {

  /** Result of a tolerance evaluation. */
  enum class Verdict {
    PASS,
    WARNING,
    FAIL,
    SKIPPED,
  }

  /**
   * Result of evaluating the deviation between reported and expected impression counts.
   *
   * @param verdict evaluation outcome.
   * @param reportedCount impression count from the EDPA report.
   * @param expectedCount the count the EDPA was expected to report.
   * @param deviationFraction absolute fractional deviation between the two counts, or `null` when
   *   the evaluation was [Verdict.SKIPPED] and no comparison was performed.
   * @param effectiveTolerance tolerance threshold the deviation was compared against to produce
   *   [verdict], or `null` when the evaluation was [Verdict.SKIPPED].
   */
  data class EvaluationResult(
    val verdict: Verdict,
    val reportedCount: Long,
    val expectedCount: Long,
    val deviationFraction: Double?,
    val effectiveTolerance: Double?,
  )

  /**
   * Evaluates the deviation between reported and expected impression counts.
   *
   * A band is triggered only when the deviation is strictly greater than both the band's
   * `thresholdFraction` and its `minimumAbsoluteDeviation`; equality at either bound does not
   * trigger. So a deviation exactly at the warning fraction is PASS, and one exactly at the failure
   * fraction is WARNING.
   *
   * @param reportedCount impression count from the EDPA report.
   * @param expectedCount the count the EDPA is expected to report — the publisher's count scaled to
   *   the EDPA's measured population by the caller.
   * @param config tolerance configuration with warning and failure bands.
   * @return the evaluation result with verdict and deviation details.
   */
  fun evaluate(
    reportedCount: Long,
    expectedCount: Long,
    config: ToleranceConfig,
  ): EvaluationResult {
    require(
      !config.hasFailure() ||
        config.failure.thresholdFraction >= config.warning.thresholdFraction
    ) {
      "failure band threshold_fraction must be >= warning band threshold_fraction"
    }
    require(
      !config.hasFailure() ||
        config.failure.minimumAbsoluteDeviation >= config.warning.minimumAbsoluteDeviation
    ) {
      "failure band minimum_absolute_deviation must be >= warning band minimum_absolute_deviation"
    }

    // Nothing to compare against when there is no expected count. A skipped row carries no deviation
    // or tolerance, so both are null to distinguish "not evaluated" from a real zero.
    if (expectedCount <= 0L) {
      return EvaluationResult(
        verdict = Verdict.SKIPPED,
        reportedCount = reportedCount,
        expectedCount = expectedCount,
        deviationFraction = null,
        effectiveTolerance = null,
      )
    }

    val absoluteDeviation: Long = abs(expectedCount - reportedCount)
    val deviationFraction: Double = absoluteDeviation.toDouble() / expectedCount.toDouble()

    // An unset failure band means the report is never failed; only the warning band applies.
    val verdict =
      when {
        config.hasFailure() && bandExceeded(config.failure, deviationFraction, absoluteDeviation) ->
          Verdict.FAIL
        bandExceeded(config.warning, deviationFraction, absoluteDeviation) -> Verdict.WARNING
        else -> Verdict.PASS
      }

    // Report the fractional threshold of the band that determined the verdict: the failure band for
    // FAIL, the warning band for WARNING and PASS (the boundary between passing and warning).
    val effectiveTolerance =
      when (verdict) {
        Verdict.FAIL -> config.failure.thresholdFraction
        Verdict.WARNING,
        Verdict.PASS -> config.warning.thresholdFraction
        Verdict.SKIPPED -> error("SKIPPED is returned before tolerance evaluation")
      }

    return EvaluationResult(
      verdict = verdict,
      reportedCount = reportedCount,
      expectedCount = expectedCount,
      deviationFraction = deviationFraction,
      effectiveTolerance = effectiveTolerance,
    )
  }

  /**
   * Whether [band] is exceeded.
   *
   * The band is exceeded only when the deviation is strictly greater than both the band's
   * fractional threshold and its absolute-deviation floor.
   */
  private fun bandExceeded(
    band: ToleranceBand,
    deviationFraction: Double,
    absoluteDeviation: Long,
  ): Boolean =
    deviationFraction > band.thresholdFraction && absoluteDeviation > band.minimumAbsoluteDeviation
}
