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
package org.wfanet.measurement.eventdataprovider.directmeasurement

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DynamicClipperTest {

  @Test
  fun `runTwoPhaseDynamicClipping returns expected dynamicClipResult with small maxThreshold`() {
    val histogramList = listOf(0L, 1L, 1L, 2L) // cumulativeHistogramList is listOf(0, 4, 3, 2)
    // Set rho large enough to minimize noise.
    val queryRho = 1E10
    val maxThreshold = 3

    val dynamicClipper = DynamicClipper(histogramList, queryRho, maxThreshold)
    val dynamicClipResult = dynamicClipper.runTwoPhaseDynamicClipping()

    val expectedThreshold = 3
    // Expected noisedCumulativeHistogramList should only have 3 elements.
    val expectedCumulativeHistogramList = listOf(0.0, 4.0, 3.0)

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertCumulativeHistogramResult(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList
    )
  }

  @Test
  fun `runTwoPhaseDynamicClipping returns expected dynamicClipResult with large maxThreshold`() {
    val histogramList = listOf(0L, 1L, 1L, 2L) // cumulativeHistogramList is listOf(0, 4, 3, 2)
    // Set rho large enough to minimize noise.
    val queryRho = 1E10
    val maxThreshold = 500

    val dynamicClipper = DynamicClipper(histogramList, queryRho, maxThreshold)
    val dynamicClipResult = dynamicClipper.runTwoPhaseDynamicClipping()

    // Output threshold is 7 with the algorithm and pre-set sliding window size.
    val expectedThreshold = 7
    // Expected noisedCumulativeHistogramList should have all elements.
    val expectedCumulativeHistogramList = listOf(0.0, 4.0, 3.0, 2.0)

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertCumulativeHistogramResult(
      dynamicClipResult.noisedCumulativeHistogramList,
      expectedCumulativeHistogramList
    )
  }

  @Test
  fun `runTwoPhaseDynamicClipping result has expected threshold and error`() {
    val histogramList = listOf(0L, 1L, 1L, 1L, 1L)
    // Set rho large enough to minimize noise.
    val queryRho = 1E10
    val maxThreshold = 4

    val dynamicClipper = DynamicClipper(histogramList, queryRho, maxThreshold)
    val dynamicClipResult = dynamicClipper.runTwoPhaseDynamicClipping()

    val totalImpressionCount = histogramList.mapIndexed { index, count -> index * count }.sum()
    val errPercent =
      abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - totalImpressionCount) /
        totalImpressionCount.toDouble()

    val expectedThreshold = 4
    // The max frequency is 4 but the impression clipping cap is threshold-1=3, so the
    // truncated total impressions is one less than the actual which is 10. Thus,
    // the relative error is 1 / 10 = 0.1.
    val expectedErrPercent = 0.1

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertThat(errPercent).isWithin(TOLERANCE).of(expectedErrPercent)
  }

  @Test
  fun `runTwoPhaseDynamicClipping result has expected threshold and error with another dataset`() {
    val histogramList = listOf(0L, 20L, 20L, 13L, 1L)
    // Set rho large enough to minimize noise.
    val queryRho = 1E10
    val maxThreshold = 4

    val dynamicClipper = DynamicClipper(histogramList, queryRho, maxThreshold)
    val dynamicClipResult = dynamicClipper.runTwoPhaseDynamicClipping()

    val totalImpressionCount = histogramList.mapIndexed { index, count -> index * count }.sum()
    val errPercent =
      abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - totalImpressionCount) /
        totalImpressionCount.toDouble()

    val expectedThreshold = 4
    // The max frequency is 4 but the impression clipping cap is 4-1=3, so the
    // truncated total impressions is one less than the actual which is 10. Thus,
    // the relative error is 1 / 100 = 0.01.
    val expectedErrPercent = 0.01

    assertThat(dynamicClipResult.threshold).isEqualTo(expectedThreshold)
    assertThat(errPercent).isWithin(TOLERANCE).of(expectedErrPercent)
  }

  @Test
  fun `use csv data`() {
    val queryRho = 7.789385926446293E-7
    val maxThreshold = 300
    val dynamicClipper = DynamicClipper(csvData, queryRho, maxThreshold)

    val errPercent = mutableListOf<Double>()
    for (i in 0..200) {
      val dynamicClipResult = dynamicClipper.runTwoPhaseDynamicClipping()
      errPercent.add(
        abs(dynamicClipResult.noisedCumulativeHistogramList.sum() - csvDataTotalImpressions) /
          csvDataTotalImpressions.toDouble()
      )
    }
    val errPercentAverage = errPercent.average()
    println("errPercentAverage: $errPercentAverage")
  }

  companion object {
    private const val EPSILON = 0.0039987743864059455
    private val smallData = listOf(0L, 5000L, 4000L, 3000L, 2000L, 0L)
    private val csvData =
      listOf<Long>(
        0,
        4249792,
        2179764,
        1297705,
        879946,
        626592,
        472820,
        385889,
        299318,
        239864,
        197137,
        163959,
        137995,
        118604,
        106594,
        88074,
        74174,
        62564,
        53281,
        46014,
        39682,
        35689,
        28852,
        24255,
        20274,
        17587,
        15055,
        12891,
        11807,
        9258,
        7628,
        6328,
        5312,
        4465,
        3803,
        3231,
        2819,
        2478,
        2207,
        1898,
        1694,
        1557,
        1371,
        1225,
        1089,
        1003,
        874,
        805,
        757,
        721,
        707,
        546,
        570,
        508,
        495,
        466,
        442,
        439,
        388,
        393,
        369,
        321,
        296,
        301,
        278,
        286,
        284,
        259,
        241,
        249,
        211,
        229,
        229,
        220,
        209,
        197,
        210,
        188,
        160,
        179,
        156,
        143,
        155,
        128,
        120,
        117,
        128,
        112,
        98,
        97,
        103,
        90,
        111,
        97,
        91,
        82,
        91,
        106,
        75,
        95,
        62,
        67,
        54,
        66,
        52,
        61,
        49,
        56,
        47,
        55,
        38,
        29,
        27,
        36,
        37,
        29,
        26,
        31,
        29,
        23,
        19,
        33,
        24,
        16,
        15,
        17,
        21,
        14,
        12,
        11,
        18,
        10,
        9,
        11,
        10,
        12,
        10,
        4,
        6,
        11,
        7,
        5,
        7,
        4,
        6,
        7,
        0,
        4,
        3,
        3,
        6,
        1,
        1,
        2,
        2,
        3,
        1,
        1,
        2,
        2,
        0,
        1,
        1,
        4,
        2,
        1,
        0,
        0,
        1,
        1,
        1,
        0,
        1,
        0,
        1,
        0,
        3,
        0,
        2,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1
      )
    private const val csvDataTotalImpressions = 50881178L
    private const val TOLERANCE = 1E-3

    private fun assertCumulativeHistogramResult(
      cumulativeHistogramList: List<Double>,
      expectedCumulativeHistogramList: List<Double>
    ) {
      assertThat(cumulativeHistogramList.size).isEqualTo(expectedCumulativeHistogramList.size)
      cumulativeHistogramList.mapIndexed { index, count ->
        assertThat(count).isWithin(TOLERANCE).of(expectedCumulativeHistogramList[index])
      }
    }
  }
}
