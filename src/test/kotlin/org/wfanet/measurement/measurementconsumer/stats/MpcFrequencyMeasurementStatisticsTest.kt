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
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class MpcFrequencyMeasurementStatisticsTest {
  private lateinit var mpcFrequencyMeasurementStatistics: MpcFrequencyMeasurementStatistics

  @Before
  fun initService() {
    mpcFrequencyMeasurementStatistics = MpcFrequencyMeasurementStatistics(DECAY_RATE, SKETCH_SIZE)
  }

  @Test
  fun `variances returns variances when small total reach, small sampling width, and small decay rate`() {
    val totalReach = 10
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(1e-2, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(6924955.020569592, 3895287.199070394, 1731238.755142398, 432809.6887855995, 0.0)
    val expectedNKPlus =
      listOf(43280968.87855994, 15581148.79628158, 3895287.199070395, 432809.6887855995, 0.0)

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
  fun `variances returns variances when small total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    mpcFrequencyMeasurementStatistics = MpcFrequencyMeasurementStatistics(decayRate, SKETCH_SIZE)

    val totalReach = 10
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(1e-2, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(6931747.5574386, 3899108.001059211, 1732936.88935965, 433234.2223399125, 0.0)
    val expectedNKPlus =
      listOf(43323422.233991235, 15596432.004236847, 3899108.001059212, 433234.2223399125, 0.0)

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
  fun `variances returns variances when small total reach, large sampling width, and small decay rate`() {
    val totalReach = 10
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(1.0, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        4.809657736908107,
        4.088209119574676,
        3.84772595911162,
        4.088208255518936,
        4.809656008796624
      )
    val expectedRKPlus =
      listOf(0.0, 4.809657736908105, 7.935933926611972, 7.935933062556234, 4.809656008796624)
    val expectedNK =
      listOf(
        21993.508778450716,
        18495.358884218687,
        17213.873575034922,
        18149.05285089939,
        21300.89671181211
      )
    val expectedNKPlus =
      listOf(
        4328.777582607534,
        22859.264294972218,
        35536.07625366837,
        35189.77022034907,
        21300.89671181211
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
  fun `variances returns variances when small total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    mpcFrequencyMeasurementStatistics = MpcFrequencyMeasurementStatistics(decayRate, SKETCH_SIZE)
    val totalReach = 10
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(1.0, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        4.852333214777005,
        4.1244853983815455,
        3.8818550207757827,
        4.124442081959716,
        4.852246581933344
      )
    val expectedRKPlus =
      listOf(0.0, 4.852333214777005, 8.00628266392822, 8.006239347506389, 4.852246581933344)
    val expectedNK =
      listOf(
        22396.225589524824,
        18835.716311720287,
        17532.244237694387,
        18485.809367447113,
        21696.411700978482
      )
    val expectedNKPlus =
      listOf(
        4371.415731789478,
        23270.508735882726,
        36192.84567250306,
        35842.938728229885,
        21696.411700978482
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
  fun `variances returns variances when large total reach, small sampling width, and small decay rate`() {
    val totalReach = 3e8.toInt()
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.01, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(
        7.908010724473936e+32,
        4.448256032516588e+32,
        1.977002681118484e+32,
        4.94250670279621e+31,
        0.0
      )
    val expectedNKPlus =
      listOf(
        4.942506702796209e+33,
        1.779302413006636e+33,
        4.44825603251659e+32,
        4.94250670279621e+31,
        0.0
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
  fun `variances returns variances when large total reach, small sampling width, and large decay rate`() {
    val decayRate = 100.0
    mpcFrequencyMeasurementStatistics = MpcFrequencyMeasurementStatistics(decayRate, SKETCH_SIZE)
    val totalReach = 3e8.toInt()
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.01, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        0.0007208782342626791,
        0.0006187464791082771,
        0.0005447026941568097,
        0.0004987468794082772,
        0.00048087903486267896
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626791,
        0.0010034497069984203,
        0.0008834501072984204,
        0.00048087903486267896
      )
    val expectedNK =
      listOf(
        137508046270732.0,
        96637366321740.02,
        67345201977040.01,
        49631553236626.37,
        43496420100501.89
      )
    val expectedNKPlus =
      listOf(
        451895273252719.94,
        227887100921272.0,
        131434502402141.0,
        84428689317028.02,
        43496420100501.89
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
  fun `variances returns variances when large total reach, large sampling width, and large decay rate`() {
    val decayRate = 100.0
    mpcFrequencyMeasurementStatistics = MpcFrequencyMeasurementStatistics(decayRate, SKETCH_SIZE)
    val totalReach = 3e8.toInt()
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.99, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        0.0007208782342626792,
        0.0006187464791082771,
        0.00054470269415681,
        0.0004987468794082774,
        0.0004808790348626793
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.0007208782342626785,
        0.0010034497069984208,
        0.0008834501072984206,
        0.0004808790348626793
      )
    val expectedNK =
      listOf(
        137507997147242.02,
        96637338624596.98,
        67345189584765.0,
        49631550027739.75,
        43496419953523.766
      )
    val expectedNKPlus =
      listOf(
        451894967607984.06,
        227886990668836.03,
        131434474587415.98,
        84428685990558.75,
        43496419953523.766
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
      FrequencyMeasurementParams(1e-1, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = 0.0
    val expectedRKPlus = 0.0
    val expectedNK = 433777.75826075335
    val expectedNKPlus = 433777.75826075335

    assertThat(percentageError(rKVars.getValue(1), expectedRK)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(rKPlusVars.getValue(1), expectedRKPlus)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(nKVars.getValue(1), expectedNK)).isLessThan(ERROR_TOLERANCE)
    assertThat(percentageError(nKPlusVars.getValue(1), expectedNKPlus)).isLessThan(ERROR_TOLERANCE)
  }

  @Test
  fun `variances return 0 when reach is less than 2`() {
    val totalReach = 1
    val maximumFrequency = 5
    val relativeFrequencyDistribution =
      (1..maximumFrequency).associateWith { (maximumFrequency - it) / 10.0 }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(1e-3, DpParams(0.1, 1e-9), DpParams(0.3, 1e-9), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      mpcFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedRKPlus = listOf(0.0, 0.0, 0.0, 0.0, 0.0)
    val expectedNK =
      listOf(692479821.8969592, 389519899.81703943, 173119955.4742398, 43279988.86855995, 0.0)
    val expectedNKPlus =
      listOf(4327998886.855994, 1558079599.2681584, 389519899.8170396, 43279988.86855995, 0.0)

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

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val ERROR_TOLERANCE = 1e-2
    const val DECAY_RATE = 1e-3
    const val SKETCH_SIZE = 1e5
  }
}
