/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.loadtest.dataprovider

import kotlin.math.ln
import kotlin.math.sqrt
import org.apache.commons.math3.distribution.NormalDistribution
import org.wfanet.measurement.api.v2alpha.MeasurementSpec

class GaussianNoiser(reachAndFrequency: MeasurementSpec.ReachAndFrequency, randomSeed: Long?) :
  Noiser {
  // The sigma calculation formula is a closed-form formula from The Algorithmic
  // Foundations of Differential Privacy p.265 Theorem A.1
  // https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf
  // This sigma formula is valid only for continuous Gaussian noise . It generally works for
  // epsilon <= 1 but not epsilon > 1
  override val distributionForReach: NormalDistribution =
    NormalDistribution(
      0.0,
      sqrt(2 * ln(1.25 / reachAndFrequency.reachPrivacyParams.delta)) /
        reachAndFrequency.reachPrivacyParams.epsilon
    )
  override val distributionForFrequency: NormalDistribution =
    NormalDistribution(
      0.0,
      sqrt(2 * ln(1.25 / reachAndFrequency.frequencyPrivacyParams.delta)) /
        reachAndFrequency.frequencyPrivacyParams.epsilon
    )

  init {
    if (randomSeed != null) {
      distributionForReach.reseedRandomGenerator(randomSeed)
      distributionForFrequency.reseedRandomGenerator(randomSeed)
    }
  }
}
