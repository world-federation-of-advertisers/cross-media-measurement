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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.internal.duchy.NoiseMechanism

@RunWith(JUnit4::class)
class SliceSuppressionTest {
  @Test
  fun `verdict releases every cell without thresholds`() {
    val suppression =
      SliceSuppression(
        resultMinimumThresholds = null,
        noise = NO_NOISE,
        vidSamplingIntervalWidth = FULL_SAMPLING_WIDTH,
      )

    assertThat(suppression.verdict(testCell(mapOf(DATA_PROVIDER_A to 1L))))
      .isEqualTo(SliceSuppression.Verdict.Release)
  }

  @Test
  fun `verdict releases a cell whose slices all pass`() {
    val verdict =
      SUPPRESSION.verdict(testCell(mapOf(DATA_PROVIDER_A to MIN_USERS, DATA_PROVIDER_B to 9L)))

    assertThat(verdict).isEqualTo(SliceSuppression.Verdict.Release)
  }

  @Test
  fun `verdict corrects a cell with a failed slice`() {
    val verdict =
      SUPPRESSION.verdict(
        testCell(mapOf(DATA_PROVIDER_A to MIN_USERS, DATA_PROVIDER_B to MIN_USERS - 1))
      )

    assertThat(verdict).isEqualTo(SliceSuppression.Verdict.Correct(setOf(DATA_PROVIDER_B)))
  }

  @Test
  fun `verdict fails the slice of a summand DataProvider that reached no VID in the cell`() {
    val verdict = SUPPRESSION.verdict(testCell(mapOf(DATA_PROVIDER_A to MIN_USERS)))

    assertThat(verdict).isEqualTo(SliceSuppression.Verdict.Correct(setOf(DATA_PROVIDER_B)))
  }

  @Test
  fun `verdict scales slice reach by the sampling width`() {
    val suppression =
      SliceSuppression(
        ResultMinimumThresholds(minUsers = MIN_USERS.toInt(), minImpressions = 1),
        NO_NOISE,
        vidSamplingIntervalWidth = 0.5,
      )

    // 3 VIDs in a half-width sample scale to 6, which passes; 2 scale to 4, which fails.
    val verdict = suppression.verdict(testCell(mapOf(DATA_PROVIDER_A to 3L, DATA_PROVIDER_B to 2L)))

    assertThat(verdict).isEqualTo(SliceSuppression.Verdict.Correct(setOf(DATA_PROVIDER_B)))
  }

  @Test
  fun `verdict suppresses a cell whose slices all fail`() {
    val verdict = SUPPRESSION.verdict(testCell(mapOf(DATA_PROVIDER_A to 1L, DATA_PROVIDER_B to 1L)))

    assertThat(verdict).isEqualTo(SliceSuppression.Verdict.Suppress)
  }

  companion object {
    private const val FULL_SAMPLING_WIDTH = 1.0
    private const val MIN_USERS = 5L
    private const val DATA_PROVIDER_A = "dataProviders/a"
    private const val DATA_PROVIDER_B = "dataProviders/b"
    private val NO_NOISE =
      CellNoise(
        CellPrivacyParams(NoiseMechanism.NONE, resultMinimumThresholds = null),
        "measurementConsumers/mc/measurements/m",
      )
    private val SUPPRESSION =
      SliceSuppression(
        ResultMinimumThresholds(minUsers = MIN_USERS.toInt(), minImpressions = 1),
        NO_NOISE,
        FULL_SAMPLING_WIDTH,
      )

    private fun testCell(sliceReach: Map<String, Long>) =
      QueryEvaluator.Cell(
        queryId = "query",
        groupingPaths = listOf(),
        attributeValues = listOf(),
        frequencyHistogram = longArrayOf(sliceReach.values.sum()),
        impressions = sliceReach.values.sum(),
        populationSize = 1L,
        sliceReach = sliceReach,
        summandDataProviders = setOf(DATA_PROVIDER_A, DATA_PROVIDER_B),
        fingerprint = byteArrayOf(0),
        sliceFingerprints =
          mapOf(DATA_PROVIDER_A to byteArrayOf(0), DATA_PROVIDER_B to byteArrayOf(0)),
      )
  }
}
