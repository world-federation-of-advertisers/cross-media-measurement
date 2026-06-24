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
import org.wfanet.measurement.config.edpaggregator.ToleranceConfig

/**
 * Evaluates the deviation between a reported impression count and the publisher's source of truth.
 *
 * The reported count is compared against the publisher's count scaled by the VID sampling fraction.
 * The deviation is checked against the configured warning and failure thresholds. Validation
 * compares against the publisher's pre-noise reported value, so no noise band is applied; counts
 * below [ToleranceConfig.minimumImpressionCount] are skipped as too noisy to validate.
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
   * Result of evaluating the deviation between reported and publisher impression counts.
   *
   * @param verdict evaluation outcome.
   * @param reportedCount impression count from the EDPA report.
   * @param publisherCount impression count from the publisher's source of truth.
   * @param deviationFraction absolute fractional deviation between the two counts, or `null` when
   *   the evaluation was [Verdict.SKIPPED] and no comparison was performed.
   * @param effectiveTolerance tolerance threshold the deviation was compared against to produce
   *   [verdict], or `null` when the evaluation was [Verdict.SKIPPED].
   */
  data class EvaluationResult(
    val verdict: Verdict,
    val reportedCount: Long,
    val publisherCount: Long,
    val deviationFraction: Double?,
    val effectiveTolerance: Double?,
  )

  /**
   * Evaluates the deviation between reported and publisher impression counts.
   *
   * @param reportedCount impression count from the EDPA report.
   * @param publisherCount impression count from the publisher's source of truth.
   * @param config tolerance configuration with warning and failure thresholds.
   * @param vidSamplingWidth VID sampling fraction (0.0 to 1.0) applied by the EDPA.
   * @return the evaluation result with verdict and deviation details.
   */
  fun evaluate(
    reportedCount: Long,
    publisherCount: Long,
    config: ToleranceConfig,
    vidSamplingWidth: Double = 1.0,
  ): EvaluationResult {
    // The count the EDPA is expected to report, given it measures only a sampled fraction of VIDs.
    val expectedCount = (publisherCount * vidSamplingWidth).toLong()

    // Skip when the publisher count is too small to validate (too noisy), or when there is nothing
    // to expect (a zero or invalid sampling width). A skipped row carries no deviation or
    // tolerance, so both are null to distinguish "not evaluated" from a real zero.
    if (publisherCount < config.minimumImpressionCount || expectedCount <= 0L) {
      return EvaluationResult(
        verdict = Verdict.SKIPPED,
        reportedCount = reportedCount,
        publisherCount = publisherCount,
        deviationFraction = null,
        effectiveTolerance = null,
      )
    }

    val deviationFraction = abs(expectedCount - reportedCount).toDouble() / expectedCount.toDouble()

    val warningTolerance = config.warningThresholdFraction

    // An unset failure threshold means the report is never failed; only the warning band applies.
    val verdict =
      when {
        config.hasFailureThresholdFraction() &&
          deviationFraction > config.failureThresholdFraction -> Verdict.FAIL
        deviationFraction > warningTolerance -> Verdict.WARNING
        else -> Verdict.PASS
      }

    // Report the threshold that determined the verdict: the failure band for FAIL, the warning band
    // for WARNING and PASS (the boundary between passing and warning).
    val effectiveTolerance =
      when (verdict) {
        Verdict.FAIL -> config.failureThresholdFraction
        Verdict.WARNING,
        Verdict.PASS -> warningTolerance
        Verdict.SKIPPED -> error("SKIPPED is returned before tolerance evaluation")
      }

    return EvaluationResult(
      verdict = verdict,
      reportedCount = reportedCount,
      publisherCount = publisherCount,
      deviationFraction = deviationFraction,
      effectiveTolerance = effectiveTolerance,
    )
  }
}
