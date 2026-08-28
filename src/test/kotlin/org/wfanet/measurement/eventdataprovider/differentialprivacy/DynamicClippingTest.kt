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
package org.wfanet.measurement.eventdataprovider.differentialprivacy

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DynamicClippingTest {

  @Test
  fun `computeImpressionCappedHistogram returns expected result with small maxThreshold`() {
    val frequencyMap =
      mapOf(1L to 1L, 2L to 1L, 3L to 2L) // cumulativeHistogramList is listOf(4.0, 3.0, 2.0)
    val maxThreshold = 2

    // Set rho large enough to minimize noise.
    val dynamicClipping = DynamicClipping(BIG_RHO, IMPRESSION_MEASUREMENT_TYPE, maxThreshold)
    val dynamicClipResult = dynamicClipping.computeImpressionCappedHistogram(frequencyMap)

    val expectedThreshold = 2
    // Expected noisedCumulativeHistogramList should only have 2 elements.
    val expectedCumulativeHistogramList = listOf(4.0, 3.0)

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertAlmostEqualCumulativeHistogram(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList,
    )
  }

  @Test
  fun `computeImpressionCappedHistogram returns expected result with large maxThreshold`() {
    val frequencyMap =
      mapOf(1L to 1L, 2L to 1L, 3L to 2L) // cumulativeHistogramList is listOf(4.0, 3.0, 2.0)
    val maxThreshold = 500

    // Set rho large enough to minimize noise.
    val dynamicClipping = DynamicClipping(BIG_RHO, IMPRESSION_MEASUREMENT_TYPE, maxThreshold)
    val dynamicClipResult = dynamicClipping.computeImpressionCappedHistogram(frequencyMap)

    // Output threshold is 6 calculated by the algorithm, pre-set sliding window size, and input
    // histogramList. It can be verified by the original colab.
    val expectedThreshold = 6
    // Expected noisedCumulativeHistogramList should have all elements.
    val expectedCumulativeHistogramList = listOf(4.0, 3.0, 2.0)

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertAlmostEqualCumulativeHistogram(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList,
    )
  }

  @Test
  fun `computeImpressionCappedHistogram result has expected threshold and error with small maxThreshold`() {
    val frequencyMap =
      mapOf(
        1L to 1L,
        2L to 1L,
        3L to 1L,
        4L to 1L,
      ) // cumulativeHistogramList is listOf(4.0, 3.0, 2.0, 1.0)
    val maxThreshold = 3

    // Set rho large enough to minimize noise.
    val dynamicClipping = DynamicClipping(BIG_RHO, IMPRESSION_MEASUREMENT_TYPE, maxThreshold)
    val dynamicClipResult = dynamicClipping.computeImpressionCappedHistogram(frequencyMap)

    val histogramList = DynamicClipping.frequencyHistogramMapToList(frequencyMap)
    val totalImpressionCount = histogramList.mapIndexed { index, count -> index * count }.sum()
    val errPercent =
      abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - totalImpressionCount) /
        totalImpressionCount.toDouble()

    val expectedCumulativeHistogramList = listOf(4.0, 3.0, 2.0)
    val expectedThreshold = 3
    // The max frequency is 4 but the impression clipping threshold is 3, so the
    // truncated total impressions is 9, one less than the actual which is 10. Thus,
    // the relative error is 1 / 10 = 0.1.
    val expectedErrPercent = 1.0 / 10

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertAlmostEqualCumulativeHistogram(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList,
    )
    assertThat(errPercent).isWithin(TOLERANCE).of(expectedErrPercent)
  }

  @Test
  fun `computeImpressionCappedHistogram result has expected threshold and error with another dataset`() {
    val frequencyMap =
      mapOf(
        1L to 20L,
        2L to 20L,
        3L to 12L,
        4L to 1L,
      ) // cumulativeHistogramList is listOf(53.0, 33.0, 13.0, 1.0)
    val maxThreshold = 3

    // Set rho large enough to minimize noise.
    val dynamicClipping = DynamicClipping(BIG_RHO, IMPRESSION_MEASUREMENT_TYPE, maxThreshold)
    val dynamicClipResult = dynamicClipping.computeImpressionCappedHistogram(frequencyMap)

    val histogramList = DynamicClipping.frequencyHistogramMapToList(frequencyMap)
    val totalImpressionCount = histogramList.mapIndexed { index, count -> index * count }.sum()
    val errPercent =
      abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - totalImpressionCount) /
        totalImpressionCount.toDouble()

    val expectedCumulativeHistogramList = listOf(53.0, 33.0, 13.0)
    val expectedThreshold = 3
    // The max frequency is 4 but the impression clipping threshold is 3, so the
    // truncated total impressions is 99, one less than the actual which is 100. Thus,
    // the relative error is 1 / 100= 0.1.
    val expectedErrPercent = 1.0 / 100

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertAlmostEqualCumulativeHistogram(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList,
    )
    assertThat(errPercent).isWithin(TOLERANCE).of(expectedErrPercent)
  }

  @Test
  fun `computeImpressionCappedHistogram result has expected threshold and error with default max threshold`() {
    val frequencyMap =
      mapOf(
        1L to 20L,
        2L to 20L,
        3L to 12L,
        4L to 1L,
      ) // cumulativeHistogramList is listOf(53.0, 33.0, 13.0, 1.0)

    // Set rho large enough to minimize noise.
    val dynamicClipping = DynamicClipping(BIG_RHO, IMPRESSION_MEASUREMENT_TYPE)
    val dynamicClipResult = dynamicClipping.computeImpressionCappedHistogram(frequencyMap)

    val histogramList = DynamicClipping.frequencyHistogramMapToList(frequencyMap)
    val totalImpressionCount = histogramList.mapIndexed { index, count -> index * count }.sum()
    val errPercent =
      abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - totalImpressionCount) /
        totalImpressionCount.toDouble()

    val expectedCumulativeHistogramList = listOf(53.0, 33.0, 13.0, 1.0)
    val expectedThreshold = 7
    // No impression is truncated so the relative error is 0.
    val expectedErrPercent = 0.0

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertAlmostEqualCumulativeHistogram(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList,
    )
    assertThat(errPercent).isWithin(TOLERANCE).of(expectedErrPercent)
  }

  @Test
  fun `frequencyHistogramMapToList works as expected`() {
    val frequencyHistogramMap = mapOf(1L to 5L, 3L to 10L, 5L to 3L)
    val histogramList = DynamicClipping.frequencyHistogramMapToList(frequencyHistogramMap)

    // The first element should be zero which means 0 impression/duration has 0 count.
    val expectedHistogramList = listOf(0L, 5L, 0L, 10L, 0L, 3L)

    assertThat(histogramList).isEqualTo(expectedHistogramList)
  }

  private companion object {
    private val IMPRESSION_MEASUREMENT_TYPE = DynamicClipping.MeasurementType.IMPRESSION
    private const val EPSILON = 0.00558073744893074
    // Set rho large enough to minimize noise.
    private const val BIG_RHO = 1E10
    private const val TOLERANCE = 1E-3

    private fun assertAlmostEqualCumulativeHistogram(
      cumulativeHistogramList: List<Double>,
      expectedCumulativeHistogramList: List<Double>,
    ) {
      assertThat(cumulativeHistogramList.size).isEqualTo(expectedCumulativeHistogramList.size)
      cumulativeHistogramList.forEachIndexed { index, count ->
        assertThat(count).isWithin(TOLERANCE).of(expectedCumulativeHistogramList[index])
      }
    }
  }
}
