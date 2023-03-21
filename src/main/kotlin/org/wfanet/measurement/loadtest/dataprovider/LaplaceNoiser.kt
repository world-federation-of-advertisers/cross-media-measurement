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

import org.apache.commons.math3.distribution.LaplaceDistribution
import org.wfanet.measurement.api.v2alpha.MeasurementSpec

class LaplaceNoiser(reachAndFrequency: MeasurementSpec.ReachAndFrequency, randomSeed: Long?) :
  Noiser {
  override val distributionForReach: LaplaceDistribution =
    LaplaceDistribution(0.0, 1 / reachAndFrequency.reachPrivacyParams.epsilon)
  override val distributionForFrequency: LaplaceDistribution =
    LaplaceDistribution(0.0, 1 / reachAndFrequency.frequencyPrivacyParams.epsilon)

  init {
    if (randomSeed != null) {
      distributionForReach.reseedRandomGenerator(randomSeed)
      distributionForFrequency.reseedRandomGenerator(randomSeed)
    }
  }
}
