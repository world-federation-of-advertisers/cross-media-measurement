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

import org.wfanet.measurement.api.v2alpha.CustomDirectMethodology
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.customDirectMethodology

/** Builds fixed uncertainty metadata for values suppressed by minimum-result thresholding. */
object ThresholdedResultMethodologies {
  /** Builds metadata for a scalar value suppressed to zero. */
  fun buildScalar(variance: Double): CustomDirectMethodology = customDirectMethodology {
    this.variance = CustomDirectMethodologyKt.variance { scalar = variance }
  }

  /**
   * Builds metadata for a frequency histogram whose final 1+ bucket was suppressed.
   *
   * Fold-down has already represented higher-frequency users in frequency 1, so only frequency 1
   * and its 1+ cumulative bucket get non-zero variance. [countVariance] is converted from count
   * units to relative-frequency units using [reach].
   */
  fun buildFrequency(
    countVariance: Double,
    maximumFrequency: Int,
    reach: Long,
  ): CustomDirectMethodology {
    require(reach > 0L) { "Reach must be positive, got $reach" }

    val relativeVariance = countVariance / (reach.toDouble() * reach)
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
}
