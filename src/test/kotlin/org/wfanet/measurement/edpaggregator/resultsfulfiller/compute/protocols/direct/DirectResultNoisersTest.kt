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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.GaussianResultNoiser
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectResultNoisersTest {

  @Test
  fun `buildDirectResultNoiser returns NoNoise for NONE`() {
    val noiser = build(DirectNoiseMechanism.NONE, truncationBound = null)

    assertThat(noiser).isSameInstanceAs(NoNoise)
  }

  @Test
  fun `buildDirectResultNoiser returns a Gaussian noiser for CONTINUOUS_GAUSSIAN`() {
    val noiser = build(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN, truncationBound = null)

    assertThat(noiser).isInstanceOf(GaussianResultNoiser::class.java)
  }

  @Test
  fun `buildDirectResultNoiser returns a deterministic noiser for DETERMINISTIC_TRUNCATED_LAPLACE`() {
    val noiser = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = 10)

    assertThat(noiser).isInstanceOf(DeterministicTruncatedLaplaceResultNoiser::class.java)
  }

  @Test
  fun `buildDirectResultNoiser rejects a missing truncation bound`() {
    assertFailsWith<IllegalArgumentException> {
      build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = null)
    }
  }

  @Test
  fun `buildDirectResultNoiser rejects a non-positive truncation bound`() {
    assertFailsWith<IllegalArgumentException> {
      build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = 0)
    }
  }

  @Test
  fun `buildDirectResultNoiser rejects CONTINUOUS_LAPLACE`() {
    assertFailsWith<IllegalArgumentException> {
      build(DirectNoiseMechanism.CONTINUOUS_LAPLACE, truncationBound = null)
    }
  }

  @Test
  fun `deterministic noiser draws are reproducible for the same frequency vector`() {
    val first = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = 10)
    val second = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = 10)

    assertThat(second.noiseReach(1_000L)).isEqualTo(first.noiseReach(1_000L))
    assertThat(second.noiseFrequencyBucket(0, 500L))
      .isEqualTo(first.noiseFrequencyBucket(0, 500L))
    assertThat(second.noiseImpressionsFromFrequencyHistogram(HISTOGRAM))
      .isEqualTo(first.noiseImpressionsFromFrequencyHistogram(HISTOGRAM))
  }

  @Test
  fun `deterministic noiser draws differ for a different frequency vector`() {
    val first = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = 10)
    val second =
      buildDirectResultNoiser(
        directNoiseMechanism = DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
        frequencyData = IntArray(100) { if (it < 50) 1 else 2 },
        reachDpParams = DP_PARAMS,
        frequencyDpParams = DP_PARAMS,
        maxFrequencyPerUser = MAX_FREQUENCY,
        truncationBound = 10,
      )

    assertThat(second.noiseReach(1_000L)).isNotEqualTo(first.noiseReach(1_000L))
  }

  @Test
  fun `deterministic noiser stays within the truncation bound`() {
    val truncationBound = 5
    val noiser =
      build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, truncationBound = truncationBound)

    // Reach draws at unit sensitivity, so the offset cannot exceed the bound.
    val noised = noiser.noiseReach(1_000L)

    assertThat(noised).isAtLeast(1_000L - truncationBound)
    assertThat(noised).isAtMost(1_000L + truncationBound)
  }

  @Test
  fun `toProtocolConfigNoiseMechanism maps every mechanism`() {
    assertThat(DirectNoiseMechanism.NONE.toProtocolConfigNoiseMechanism())
      .isEqualTo(NoiseMechanism.NONE)
    assertThat(DirectNoiseMechanism.CONTINUOUS_LAPLACE.toProtocolConfigNoiseMechanism())
      .isEqualTo(NoiseMechanism.CONTINUOUS_LAPLACE)
    assertThat(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN.toProtocolConfigNoiseMechanism())
      .isEqualTo(NoiseMechanism.CONTINUOUS_GAUSSIAN)
    assertThat(
        DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE.toProtocolConfigNoiseMechanism()
      )
      .isEqualTo(NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
  }

  private fun build(directNoiseMechanism: DirectNoiseMechanism, truncationBound: Int?) =
    buildDirectResultNoiser(
      directNoiseMechanism = directNoiseMechanism,
      frequencyData = FREQUENCY_DATA,
      reachDpParams = DP_PARAMS,
      frequencyDpParams = DP_PARAMS,
      maxFrequencyPerUser = MAX_FREQUENCY,
      truncationBound = truncationBound,
    )

  companion object {
    private const val MAX_FREQUENCY = 10
    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 1.0, delta = 1e-9)
    private val FREQUENCY_DATA = IntArray(100) { if (it < 90) 1 else 2 }
    private val HISTOGRAM = longArrayOf(90L, 10L)
  }
}
