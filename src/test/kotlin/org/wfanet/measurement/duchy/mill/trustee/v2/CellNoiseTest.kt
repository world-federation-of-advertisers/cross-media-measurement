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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.computation.ResultNoiser
import org.wfanet.measurement.internal.duchy.NoiseMechanism

@RunWith(JUnit4::class)
class CellNoiseTest {
  @Test
  fun `cellNoiser releases exact figures under no noise`() {
    val noiser = CellNoise(NO_NOISE, MEASUREMENT).cellNoiser(testCell())

    assertThat(noiser.noiseReach(5L)).isEqualTo(5L)
    assertThat(noiser.noiseImpressions(9L)).isEqualTo(9L)
    assertThat(noiser.noiseFrequencyBucket(0, 3L)).isEqualTo(3L)
  }

  @Test
  fun `variances are zero under no noise`() {
    val noise = CellNoise(NO_NOISE, MEASUREMENT)

    assertThat(noise.unitVariance).isEqualTo(0.0)
    assertThat(noise.impressionVariance).isEqualTo(0.0)
  }

  @Test
  fun `publicNoiseMechanism reports the configured mechanism`() {
    assertThat(CellNoise(NO_NOISE, MEASUREMENT).publicNoiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertThat(CellNoise(TRUNCATED_LAPLACE, MEASUREMENT).publicNoiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
  }

  @Test
  fun `truncated Laplace variances are positive and below the untruncated variance`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)

    // The untruncated variance of a unit-sensitivity draw at epsilon 1 is 2.
    assertThat(noise.unitVariance).isGreaterThan(0.0)
    assertThat(noise.unitVariance).isLessThan(2.0)
    assertThat(noise.impressionVariance).isGreaterThan(noise.unitVariance)
  }

  @Test
  fun `cellNoiser draws are deterministic`() {
    val first = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT).cellNoiser(testCell())
    val second = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT).cellNoiser(testCell())

    assertThat(draws(first)).isEqualTo(draws(second))
  }

  @Test
  fun `cellNoiser draws depend on the measurement name`() {
    val noiser = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT).cellNoiser(testCell())
    val other = CellNoise(TRUNCATED_LAPLACE, "$MEASUREMENT-other").cellNoiser(testCell())

    assertThat(draws(noiser)).isNotEqualTo(draws(other))
  }

  @Test
  fun `cellNoiser draws depend on the query id`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)

    assertThat(draws(noise.cellNoiser(testCell())))
      .isNotEqualTo(draws(noise.cellNoiser(testCell(queryId = "other"))))
  }

  @Test
  fun `cellNoiser draws depend on the grouping values`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)

    assertThat(draws(noise.cellNoiser(testCell(attributeValues = listOf("A")))))
      .isNotEqualTo(draws(noise.cellNoiser(testCell(attributeValues = listOf("B")))))
  }

  @Test
  fun `cellNoiser tells an unset grouping value from an empty one`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)

    assertThat(draws(noise.cellNoiser(testCell(attributeValues = listOf(null)))))
      .isNotEqualTo(draws(noise.cellNoiser(testCell(attributeValues = listOf("")))))
  }

  @Test
  fun `cellNoiser draws depend on the fingerprint`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)

    assertThat(draws(noise.cellNoiser(testCell(fingerprint = byteArrayOf(1)))))
      .isNotEqualTo(draws(noise.cellNoiser(testCell(fingerprint = byteArrayOf(2)))))
  }

  @Test
  fun `sliceNoiser draws differ by DataProvider and from the cell draws`() {
    val noise = CellNoise(TRUNCATED_LAPLACE, MEASUREMENT)
    val cell = testCell()

    assertThat(draws(noise.sliceNoiser(cell, DATA_PROVIDER_A)))
      .isNotEqualTo(draws(noise.sliceNoiser(cell, DATA_PROVIDER_B)))
    assertThat(draws(noise.sliceNoiser(cell, DATA_PROVIDER_A)))
      .isNotEqualTo(draws(noise.cellNoiser(cell)))
  }

  @Test
  fun `CellPrivacyParams throws for an unsupported noise mechanism`() {
    assertFailsWith<IllegalArgumentException> {
      CellPrivacyParams(NoiseMechanism.CONTINUOUS_GAUSSIAN, resultMinimumThresholds = null)
    }
  }

  @Test
  fun `CellPrivacyParams throws for a non-positive threshold`() {
    assertFailsWith<IllegalArgumentException> {
      CellPrivacyParams(
        NoiseMechanism.NONE,
        ResultMinimumThresholds(minUsers = 0, minImpressions = 1),
      )
    }
  }

  companion object {
    private const val MEASUREMENT = "measurementConsumers/mc/measurements/m"
    private const val DATA_PROVIDER_A = "a"
    private const val DATA_PROVIDER_B = "b"
    private val NO_NOISE = CellPrivacyParams(NoiseMechanism.NONE, resultMinimumThresholds = null)
    private val TRUNCATED_LAPLACE =
      CellPrivacyParams(
        NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
        ResultMinimumThresholds(minUsers = 1, minImpressions = 1, reachMaxFrequencyPerUser = 5),
      )

    /**
     * Returns many draws of [noiser], so that two differently seeded noisers are all but certain to
     * differ even though each draw rounds into a small range.
     */
    private fun draws(noiser: ResultNoiser): List<Long> =
      List(DRAW_COUNT) { noiser.noiseFrequencyBucket(it, 1_000L) } +
        noiser.noiseReach(1_000L) +
        noiser.noiseImpressions(1_000L)

    private const val DRAW_COUNT = 32

    private fun testCell(
      queryId: String = "query",
      attributeValues: List<String?> = listOf("A"),
      fingerprint: ByteArray = byteArrayOf(0),
    ) =
      QueryEvaluator.Cell(
        queryId = queryId,
        groupingPaths = listOf("person.age_group"),
        attributeValues = attributeValues,
        frequencyHistogram = longArrayOf(1L),
        impressions = 1L,
        populationSize = 1L,
        sliceReach = mapOf(),
        summandDataProviders = setOf(DATA_PROVIDER_A, DATA_PROVIDER_B),
        fingerprint = fingerprint,
      )
  }
}
