/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import kotlin.math.max
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodology
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.computation.ResultMinimumThresholds

/** Builds fixed uncertainty metadata for a reach value suppressed to zero. */
fun buildThresholdedReachMethodology(thresholds: ResultMinimumThresholds): CustomDirectMethodology =
  buildThresholdedScalarMethodology(thresholds.minUsers)

/** Builds fixed uncertainty metadata for an impression value suppressed to zero. */
fun buildThresholdedImpressionMethodology(
  thresholds: ResultMinimumThresholds
): CustomDirectMethodology = buildThresholdedScalarMethodology(thresholds.minImpressions)

/**
 * Builds fixed uncertainty metadata for a frequency histogram whose final 1+ bucket was suppressed.
 *
 * Fold-down has already represented higher-frequency users in the 1+ bucket, so only that bucket
 * gets non-zero variance. The standard deviation is derived solely from configured thresholds.
 */
fun buildThresholdedFrequencyMethodology(
  thresholds: ResultMinimumThresholds,
  maximumFrequency: Int,
  reach: Long,
): CustomDirectMethodology {
  require(reach > 0L) { "Reach must be positive, got $reach" }

  val countStandardDeviation = max(thresholds.minUsers, thresholds.minImpressions).toDouble()
  val relativeStandardDeviation = countStandardDeviation / reach
  val relativeVariance = relativeStandardDeviation * relativeStandardDeviation
  val variances =
    (1..maximumFrequency).associate { frequency ->
      frequency.toLong() to if (frequency == 1) relativeVariance else 0.0
    }

  return customDirectMethodology {
    variance =
      CustomDirectMethodologyKt.variance {
        frequency =
          CustomDirectMethodologyKt.VarianceKt.frequencyVariances {
            this.variances.putAll(variances)
            kPlusVariances.putAll(variances)
          }
      }
  }
}

private fun buildThresholdedScalarMethodology(standardDeviation: Int): CustomDirectMethodology {
  val variance = standardDeviation.toDouble() * standardDeviation
  return customDirectMethodology {
    this.variance = CustomDirectMethodologyKt.variance { scalar = variance }
  }
}
