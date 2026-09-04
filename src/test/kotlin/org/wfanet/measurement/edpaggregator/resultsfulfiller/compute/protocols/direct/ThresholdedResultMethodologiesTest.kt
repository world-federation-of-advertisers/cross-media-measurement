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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ThresholdedResultMethodologiesTest {
  @Test
  fun `buildScalar returns the specified variance`() {
    val methodology = ThresholdedResultMethodologies.buildScalar(160000.0)

    assertThat(methodology.variance.scalar).isEqualTo(160000.0)
  }

  @Test
  fun `buildFrequency sets variance only for frequency one`() {
    val reach = 1000L
    val methodology =
      ThresholdedResultMethodologies.buildFrequency(
        countVariance = 4000000.0,
        maximumFrequency = 3,
        reach = reach,
      )

    val frequencyVariances = methodology.variance.frequency
    val expectedRelativeVariance = (2000.0 / reach) * (2000.0 / reach)
    assertThat(frequencyVariances.variancesMap)
      .containsExactly(1L, expectedRelativeVariance, 2L, 0.0, 3L, 0.0)
    assertThat(frequencyVariances.kPlusVariancesMap)
      .containsExactly(1L, expectedRelativeVariance, 2L, 0.0, 3L, 0.0)
  }

  @Test
  fun `buildFrequency rejects non-positive reach`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ThresholdedResultMethodologies.buildFrequency(
          countVariance = 4000000.0,
          maximumFrequency = 3,
          reach = 0L,
        )
      }

    assertThat(exception).hasMessageThat().contains("Reach must be positive")
  }
}
