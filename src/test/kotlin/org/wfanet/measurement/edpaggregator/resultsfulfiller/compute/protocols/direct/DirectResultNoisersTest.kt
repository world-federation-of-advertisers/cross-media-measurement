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
    val noiser = build(DirectNoiseMechanism.NONE)

    assertThat(noiser).isSameInstanceAs(NoNoise)
  }

  @Test
  fun `buildDirectResultNoiser returns a Gaussian noiser for CONTINUOUS_GAUSSIAN`() {
    val noiser = build(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)

    assertThat(noiser).isInstanceOf(GaussianResultNoiser::class.java)
  }

  @Test
  fun `buildDirectResultNoiser returns a deterministic noiser for DETERMINISTIC_TRUNCATED_LAPLACE`() {
    val noiser = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)

    assertThat(noiser).isInstanceOf(DeterministicTruncatedLaplaceResultNoiser::class.java)
  }

  @Test
  fun `buildDirectResultNoiser rejects CONTINUOUS_LAPLACE`() {
    assertFailsWith<IllegalArgumentException> { build(DirectNoiseMechanism.CONTINUOUS_LAPLACE) }
  }

  @Test
  fun `deterministic noiser draws are reproducible for the same frequency vector`() {
    val first = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
    val second = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)

    assertThat(second.noiseReach(1_000L)).isEqualTo(first.noiseReach(1_000L))
    assertThat(second.noiseFrequencyBucket(0, 500L)).isEqualTo(first.noiseFrequencyBucket(0, 500L))
    assertThat(second.noiseImpressionsFromFrequencyHistogram(HISTOGRAM))
      .isEqualTo(first.noiseImpressionsFromFrequencyHistogram(HISTOGRAM))
  }

  @Test
  fun `deterministic noiser draws differ for a different frequency vector`() {
    // The scale (sensitivity / epsilon = 1) is fixed by the compiled params, so a single draw can
    // collide between seeds. Comparing every label at once makes an all-label collision negligible.
    fun draws(frequencyData: IntArray): List<Long> {
      val noiser = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE, frequencyData)
      return buildList {
        add(noiser.noiseReach(1_000L))
        for (bucket in 0 until MAX_FREQUENCY) {
          add(noiser.noiseFrequencyBucket(bucket, 500L))
        }
        add(noiser.noiseImpressionsFromFrequencyHistogram(HISTOGRAM))
      }
    }

    assertThat(draws(IntArray(100) { if (it < 50) 1 else 2 })).isNotEqualTo(draws(FREQUENCY_DATA))
  }

  @Test
  fun `deterministic noiser stays within the truncation bound`() {
    val noiser = build(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)

    // Reach draws at unit sensitivity, so the offset cannot exceed the compiled truncation bound.
    val noised = noiser.noiseReach(1_000L)

    assertThat(noised).isAtLeast(1_000L - COMPILED_TRUNCATION_BOUND)
    assertThat(noised).isAtMost(1_000L + COMPILED_TRUNCATION_BOUND)
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

  private fun build(
    directNoiseMechanism: DirectNoiseMechanism,
    frequencyData: IntArray = FREQUENCY_DATA,
  ) =
    buildDirectResultNoiser(
      directNoiseMechanism = directNoiseMechanism,
      frequencyData = frequencyData,
      reachDpParams = DP_PARAMS,
      frequencyDpParams = DP_PARAMS,
      maxFrequencyPerUser = MAX_FREQUENCY,
    )

  companion object {
    private const val MAX_FREQUENCY = 10

    // The compiled truncation bound; the mechanism ignores any measurement-supplied epsilon.
    private const val COMPILED_TRUNCATION_BOUND = 8L
    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 1.0, delta = 1e-9)
    private val FREQUENCY_DATA = IntArray(100) { if (it < 90) 1 else 2 }
    private val HISTOGRAM = longArrayOf(90L, 10L)
  }
}
