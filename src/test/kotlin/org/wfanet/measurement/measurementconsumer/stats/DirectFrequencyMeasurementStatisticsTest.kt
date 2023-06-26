/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.measurementconsumer.stats

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class DirectFrequencyMeasurementStatisticsTest {
  private lateinit var directFrequencyMeasurementStatistics: DirectFrequencyMeasurementStatistics

  @Before
  fun initService() {
    directFrequencyMeasurementStatistics = DirectFrequencyMeasurementStatistics()
  }

  @Test
  fun `variances returns variances when total reach is small and sampling width is small`() {
    val totalReach = 1
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        1e-4,
        DpParams(0.05, 1e-15),
        DpParams(0.2, 1e-15),
        maximumFrequency
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(130523240799.76, 110944754739.79, 104418592319.84, 110944753539.91, 130523238400.0)
    val expectedRKPlus =
      listOf(0.0, 130523240799.75995, 215363345459.78998, 215363344259.90997, 130523238400.0)
    val expectedNK =
      listOf(
        2.5828737279268425e+23,
        2.195442669924104e+23,
        2.06629897600801e+23,
        2.1954426461785614e+23,
        2.582873680435757e+23
      )
    val expectedNKPlus =
      listOf(
        1978861168399.0,
        2.5828737279307992e+23,
        4.261741614272709e+23,
        4.2617415905271664e+23,
        2.582873680435757e+23
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
  }

  @Test
  fun `variances returns variances when total reach is small and sampling width is large`() {
    val totalReach = 10
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.9, DpParams(0.05, 1e-15), DpParams(0.2, 1e-15), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        16.116646716049377,
        13.69921637530864,
        12.89296181728395,
        13.697883041975308,
        16.113980049382715
      )
    val expectedRKPlus =
      listOf(0.0, 16.116646716049377, 26.590400414814813, 26.58906708148148, 16.113980049382715)
    val expectedNK =
      listOf(
        399274.49027152435,
        338261.103357843,
        317260.89827872894,
        336273.87503418204,
        395300.03362420265
      )
    val expectedNKPlus =
      listOf(
        24431.495782716047,
        404160.78942806745,
        654501.1302572051,
        652513.9019335442,
        395300.03362420265
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
  }

  @Test
  fun `variances returns variances when total reach is large and sampling width is small`() {
    val totalReach = 3e8.toInt()
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.1, DpParams(0.05, 1e-15), DpParams(0.2, 1e-15), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        7.201450258204445e-09,
        6.301232719473777e-09,
        4.8011602065635574e-09,
        2.701232719473778e-09,
        1.4502582044444444e-12
      )
    val expectedRKPlus =
      listOf(
        0.0,
        7.201450258204445e-09,
        6.302392926037333e-09,
        2.702392926037334e-09,
        1.4502582044444444e-12
      )
    val expectedNK =
      listOf(
        1080447160.4819105,
        810289059.2826936,
        540183586.0096896,
        270130740.66289777,
        130523.24231856702
      )
    val expectedNKPlus =
      listOf(
        2701978861.1583996,
        1620842932.7135906,
        810393477.8765483,
        270235159.25675255,
        130523.24231856702
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
  }

  @Test
  fun `variances returns variances when total reach is large and sampling width is large`() {
    val totalReach = 3e8.toInt()
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.9, DpParams(0.05, 1e-15), DpParams(0.2, 1e-15), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        8.890679331116596e-11,
        7.77929965367133e-11,
        5.927358279708091e-11,
        3.3348552092268854e-11,
        1.790442227709191e-14
      )
    val expectedRKPlus =
      listOf(
        0.0,
        8.890679331116596e-11,
        7.780732007453497e-11,
        3.336287563009053e-11,
        1.790442227709191e-14
      )
    val expectedNK =
      listOf(
        13338853.595851457,
        10003568.425519641,
        6668933.002434713,
        3334947.3265966796,
        1611.398005535523
      )
    val expectedNKPlus =
      listOf(
        33357763.718004923,
        20010406.339452446,
        10004857.543924069,
        3336236.445001108,
        1611.398005535523
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKVars.getValue(frequency), expectedRK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(rKPlusVars.getValue(frequency), expectedRKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1]))
        .isLessThan(ERROR_TOLERANCE)
    }
  }

  @Test
  fun `variances returns difference variances when maximum freqeuncy is 1`() {
    val totalReach = 100
    val maximumFrequency = 1
    val relativeFrequencyDistribution = mapOf(1 to 1.0)
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        1e-3,
        DpParams(0.05, 1e-15),
        DpParams(0.2, 1e-15),
        maximumFrequency
      )

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 19788711484.000004
    val expectedNKPlus = 19788711484.000004

    assertThat(percentageError(rKVars.getValue(1), expectedRK)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(rKPlusVars.getValue(1), expectedRKPlus)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(nKVars.getValue(1), expectedNK)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(nKPlusVars.getValue(1), expectedNKPlus)).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variances throws IllegalArgumentException when reach is negative`() {
    val totalReach = -1
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(
        1e-3,
        DpParams(0.05, 1e-15),
        DpParams(0.2, 1e-15),
        maximumFrequency
      )

    assertThrows(IllegalArgumentException::class.java) {
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )
    }
  }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val ERROR_TOLERANCE = 1e-2
  }
}
