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

package org.wfanet.measurement.eventdataprovider.noiser

import java.util.Random
import org.apache.commons.math3.distribution.LaplaceDistribution
import org.apache.commons.math3.random.RandomGeneratorFactory

class LaplaceNoiser(privacyParams: DpParams, random: Random) : AbstractNoiser() {
  override val distribution: LaplaceDistribution =
    LaplaceDistribution(
      RandomGeneratorFactory.createRandomGenerator(random),
      0.0,
      1 / privacyParams.epsilon,
    )

  override val variance: Double
    get() = distribution.numericalVariance
}
