/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class DirectDynamicClippingTest {

  @Test
  fun `the clip never exceeds what the frequency vector can represent`() {
    // Every VID saturated. StripedByteFrequencyVector cannot hold more, so no clip above this
    // bounds anything.
    val frequencyData = IntArray(200) { BYTE_CEILING }

    for (mechanism in
      listOf(
        DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
        DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )) {
      val clipped =
        computeDirectDynamicallyClippedImpressions(
          directNoiseMechanism = mechanism,
          frequencyData = frequencyData,
          dpParams = DP_PARAMS,
          vidSamplingIntervalWidth = 1.0,
          resultMinimumThresholds = null,
        )

      assertThat(clipped.clip).isAtMost(BYTE_CEILING)
    }
  }

  private companion object {
    private const val BYTE_CEILING = 127
    private val DP_PARAMS = DpParams(1.0, 1E-3)
  }
}
