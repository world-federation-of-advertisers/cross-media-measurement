package org.wfanet.measurement.measurementconsumer.stats

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
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
  fun `variances returns values for different types of frequency results`() {
    val totalReach = 100
    val maximumFrequency = 6
    val relativeFrequencyDistribution =
      (1..maximumFrequency step 2).associateWith { 2.0 / maximumFrequency }
    val frequencyMeasurementParams =
      FrequencyMeasurementParams(0.5, DpParams(0.05, 1e-15), DpParams(0.2, 1e-15), maximumFrequency)

    val (rKVars, rKPlusVars, nKVars, nKPlusVars) =
      directFrequencyMeasurementStatistics.variances(
        totalReach,
        relativeFrequencyDistribution,
        frequencyMeasurementParams
      )

    val expectedRK =
      listOf(
        0.5243151758222222,
        0.5220929536,
        0.5243151758222222,
        0.5220929536,
        0.5243151758222222,
        0.5220929536
      )
    val expectedRKPlus =
      listOf(
        0.0,
        0.5243151758222223,
        0.6983461603555556,
        0.8723771448888887,
        0.6983461603555556,
        0.5220929536
      )
    val expectedNK =
      listOf(
        55603.51031646383,
        46599.11750949494,
        55603.51031646383,
        46599.11750949494,
        55603.51031646383,
        46599.11750949494
      )
    val expectedNKPlus =
      listOf(
        79254.44633600001,
        82021.6590951305,
        97554.69826496215,
        86669.5886561271,
        71136.5494862955,
        46599.11750949494
      )

    for (frequency in 1..maximumFrequency) {
      assertThat(rKVars.getValue(frequency))
        .isWithin(ABSOLUTE_ERROR_TOLERANCE)
        .of(expectedRK[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(rKPlusVars.getValue(frequency))
        .isWithin(ABSOLUTE_ERROR_TOLERANCE)
        .of(expectedRKPlus[frequency - 1])
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKVars.getValue(frequency), expectedNK[frequency - 1]))
        .isLessThan(PERCENTAGE_ERROR_TOLERANCE)
    }
    for (frequency in 1..maximumFrequency) {
      assertThat(percentageError(nKPlusVars.getValue(frequency), expectedNKPlus[frequency - 1]))
        .isLessThan(PERCENTAGE_ERROR_TOLERANCE)
    }
  }

  // @Test
  // fun `variance returns a value when vid sampling interval width is small`() {
  //   val reach = 10
  //   val vidSamplingIntervalWidth = 1e-4
  //   val dpParams = DpParams(1e-3, 1e-9)
  //   val reachMeasurementParams = FrequencyMeasurementParams(vidSamplingIntervalWidth, dpParams)
  //
  //   val variance = directFrequencyMeasurementStatistics.variance(reach, reachMeasurementParams)
  //   val expect = 1701291910858389.8
  //   val percentageError = percentageError(variance, expect)
  //   assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  // }
  //
  // @Test
  // fun `variance returns a value when reach is large`() {
  //   val reach = 3e8.toInt()
  //   val vidSamplingIntervalWidth = 0.7
  //   val dpParams = DpParams(1e-2, 1e-15)
  //   val reachMeasurementParams = FrequencyMeasurementParams(vidSamplingIntervalWidth, dpParams)
  //
  //   val variance = directFrequencyMeasurementStatistics.variance(reach, reachMeasurementParams)
  //   val expect = 129519192.01384492
  //   val percentageError = percentageError(variance, expect)
  //   assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  // }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val ABSOLUTE_ERROR_TOLERANCE = 1e-2
    const val PERCENTAGE_ERROR_TOLERANCE = 1e-2
  }
}
