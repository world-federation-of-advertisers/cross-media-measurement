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

package org.wfanet.measurement.reporting.validation

import kotlin.math.abs
import kotlin.math.max
import kotlin.math.sqrt
import org.wfanet.measurement.config.edpaggregator.ToleranceConfig

/**
 * Evaluates the deviation between a reported impression count and the publisher's source of truth.
 *
 * Applies a noise-aware tolerance formula that accounts for the statistical noise introduced by
 * VID sampling. The effective tolerance is the maximum of the configured threshold and a
 * statistical confidence band based on the sampling fraction.
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
   * @param deviationFraction absolute fractional deviation between the two counts.
   * @param effectiveTolerance tolerance threshold used for this evaluation.
   */
  data class EvaluationResult(
    val verdict: Verdict,
    val reportedCount: Long,
    val publisherCount: Long,
    val deviationFraction: Double,
    val effectiveTolerance: Double,
  )

  /**
   * Evaluates the deviation between reported and publisher impression counts.
   *
   * @param reportedCount impression count from the EDPA report.
   * @param publisherCount impression count from the publisher's source of truth.
   * @param config tolerance configuration with thresholds and noise parameters.
   * @param vidSamplingWidth VID sampling fraction (0.0 to 1.0) applied by the EDPA.
   * @return the evaluation result with verdict and deviation details.
   */
  fun evaluate(
    reportedCount: Long,
    publisherCount: Long,
    config: ToleranceConfig,
    vidSamplingWidth: Double = 1.0,
  ): EvaluationResult {
    if (publisherCount < config.minimumImpressionCount) {
      return EvaluationResult(
        verdict = Verdict.SKIPPED,
        reportedCount = reportedCount,
        publisherCount = publisherCount,
        deviationFraction = 0.0,
        effectiveTolerance = 0.0,
      )
    }

    val expectedCount = (publisherCount * vidSamplingWidth).toLong()
    val deviationFraction =
      if (expectedCount == 0L) {
        if (reportedCount == 0L) 0.0 else 1.0
      } else {
        abs(expectedCount - reportedCount).toDouble() / max(expectedCount, 1).toDouble()
      }

    val noiseAwareBand =
      if (vidSamplingWidth > 0.0 && vidSamplingWidth < 1.0 && publisherCount > 0) {
        config.noiseConfidenceMultiplier *
          sqrt(vidSamplingWidth * (1.0 - vidSamplingWidth) / publisherCount.toDouble())
      } else {
        0.0
      }

    val warningTolerance = max(config.warningThresholdFraction, noiseAwareBand)
    val failureTolerance = max(config.failureThresholdFraction, noiseAwareBand)

    val verdict =
      when {
        deviationFraction > failureTolerance -> Verdict.FAIL
        deviationFraction > warningTolerance -> Verdict.WARNING
        else -> Verdict.PASS
      }

    return EvaluationResult(
      verdict = verdict,
      reportedCount = reportedCount,
      publisherCount = publisherCount,
      deviationFraction = deviationFraction,
      effectiveTolerance = failureTolerance,
    )
  }
}
