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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt.QueryResultKt
import org.wfanet.measurement.api.v2alpha.PopulationAttributeValue
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.internal.duchy.NoiseMechanism

@RunWith(JUnit4::class)
class CellReleaserTest {
  @Test
  fun `release returns exact figures without noise or thresholds`() {
    val releaser =
      CellReleaser(
        resultMinimumThresholds = null,
        noise = noise(NO_NOISE),
        vidSamplingIntervalWidth = FULL_SAMPLING_WIDTH,
      )

    val released = releaser.release(testCell(longArrayOf(3L, 1L, 2L), impressions = 13L))

    assertThat(released)
      .isEqualTo(
        QueryResultKt.cell {
          attributeValues += populationAttributeValue { enumValue = YOUNG }
          attributeValues += PopulationAttributeValue.getDefaultInstance()
          reach = 6L
          reachVariance = 0.0
          impressions = 13L
          impressionVariance = 0.0
          frequencyBins += listOf(3L, 1L, 2L)
          frequencyBinVariance += listOf(0.0, 0.0, 0.0)
          populationSize = POPULATION_SIZE
        }
      )
  }

  @Test
  fun `release suppresses a cell whose reach is below min_users`() {
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 7, minImpressions = 1),
        noise(NO_NOISE),
        FULL_SAMPLING_WIDTH,
      )

    val released = releaser.release(testCell(longArrayOf(3L, 1L, 2L), impressions = 13L))

    assertThat(released).isEqualTo(suppressedCell(binCount = 3))
  }

  @Test
  fun `release compares reach scaled by the sampling width against min_users`() {
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 7, minImpressions = 1),
        noise(NO_NOISE),
        vidSamplingIntervalWidth = 0.5,
      )

    val released = releaser.release(testCell(longArrayOf(3L, 1L, 2L), impressions = 13L))

    assertThat(released.suppressed).isFalse()
    assertThat(released.reach).isEqualTo(6L)
  }

  @Test
  fun `release suppresses a cell whose impressions are below min_impressions`() {
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 1, minImpressions = 14),
        noise(NO_NOISE),
        FULL_SAMPLING_WIDTH,
      )

    val released = releaser.release(testCell(longArrayOf(3L, 1L, 2L), impressions = 13L))

    assertThat(released).isEqualTo(suppressedCell(binCount = 3))
  }

  @Test
  fun `release folds failing bins down from the top`() {
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 2, minImpressions = 1),
        noise(NO_NOISE),
        FULL_SAMPLING_WIDTH,
      )

    val released = releaser.release(testCell(longArrayOf(5L, 1L, 1L), impressions = 10L))

    // Bin 3 fails and folds into bin 2, which then holds 2 and passes.
    assertThat(released.frequencyBinsList).containsExactly(5L, 2L, 0L).inOrder()
    assertThat(released.reach).isEqualTo(7L)
    assertThat(released.suppressed).isFalse()
  }

  @Test
  fun `release folds by frequency times count against min_impressions`() {
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 1, minImpressions = 7),
        noise(NO_NOISE),
        FULL_SAMPLING_WIDTH,
      )

    val released = releaser.release(testCell(longArrayOf(9L, 3L, 2L), impressions = 21L))

    // Bin 3 holds 3 * 2 = 6 impressions and folds into bin 2, which then holds 2 * 5 = 10.
    assertThat(released.frequencyBinsList).containsExactly(9L, 5L, 0L).inOrder()
  }

  @Test
  fun `release carries a folded bin's variance into the lower bin`() {
    val noise = noise(TRUNCATED_LAPLACE)
    val releaser =
      CellReleaser(
        ResultMinimumThresholds(minUsers = 50, minImpressions = 1),
        noise,
        FULL_SAMPLING_WIDTH,
      )

    // The top bin stays below 50 and the others above it whatever the bounded draws are.
    val released = releaser.release(testCell(longArrayOf(1_000L, 1_000L, 1L), impressions = 3_003L))

    assertThat(released.frequencyBinsList.last()).isEqualTo(0L)
    assertThat(released.frequencyBinVarianceList)
      .containsExactly(noise.unitVariance, 2 * noise.unitVariance, 0.0)
      .inOrder()
    assertThat(released.reachVariance).isEqualTo(noise.unitVariance)
    assertThat(released.impressionVariance).isEqualTo(noise.impressionVariance)
  }

  @Test
  fun `release is deterministic under truncated Laplace noise`() {
    val cell = testCell(longArrayOf(100L, 50L, 25L), impressions = 275L)

    val first = CellReleaser(null, noise(TRUNCATED_LAPLACE), FULL_SAMPLING_WIDTH).release(cell)
    val second = CellReleaser(null, noise(TRUNCATED_LAPLACE), FULL_SAMPLING_WIDTH).release(cell)

    assertThat(first).isEqualTo(second)
  }

  @Test
  fun `suppressed returns zero metrics and sets suppressed`() {
    val releaser =
      CellReleaser(
        resultMinimumThresholds = null,
        noise = noise(NO_NOISE),
        vidSamplingIntervalWidth = FULL_SAMPLING_WIDTH,
      )

    val suppressed = releaser.suppressed(testCell(longArrayOf(3L, 1L), impressions = 5L))

    assertThat(suppressed).isEqualTo(suppressedCell(binCount = 2))
    assertThat(suppressed.hasReachVariance()).isFalse()
  }

  companion object {
    private const val YOUNG = "YEARS_18_TO_34"
    private const val FULL_SAMPLING_WIDTH = 1.0
    private const val POPULATION_SIZE = 1000L
    private val NO_NOISE = CellPrivacyParams(NoiseMechanism.NONE, resultMinimumThresholds = null)
    private val TRUNCATED_LAPLACE =
      CellPrivacyParams(
        NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
        resultMinimumThresholds = null,
      )

    private fun noise(params: CellPrivacyParams) =
      CellNoise(params, "measurementConsumers/mc/measurements/m")

    private fun testCell(histogram: LongArray, impressions: Long) =
      QueryEvaluator.Cell(
        queryId = "query",
        groupingPaths = listOf("person.age_group", "person.gender"),
        attributeValues = listOf(YOUNG, null),
        frequencyHistogram = histogram,
        impressions = impressions,
        populationSize = POPULATION_SIZE,
        sliceReach = mapOf(),
        summandDataProviders = setOf(),
        fingerprint = byteArrayOf(0),
      )

    private fun suppressedCell(binCount: Int) =
      QueryResultKt.cell {
        attributeValues += populationAttributeValue { enumValue = YOUNG }
        attributeValues += PopulationAttributeValue.getDefaultInstance()
        reach = 0L
        impressions = 0L
        frequencyBins += List(binCount) { 0L }
        suppressed = true
        populationSize = POPULATION_SIZE
      }
  }
}
