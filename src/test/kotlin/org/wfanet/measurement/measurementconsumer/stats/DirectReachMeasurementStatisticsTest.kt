package org.wfanet.measurement.measurementconsumer.stats

import com.google.common.truth.Truth.assertThat
import kotlin.math.abs
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class DirectReachMeasurementStatisticsTest {
  private lateinit var directReachMeasurementStatistics: DirectReachMeasurementStatistics

  @Before
  fun initService() {
    directReachMeasurementStatistics = DirectReachMeasurementStatistics()
  }

  @Test
  fun `variance returns a value when reach is 0`() {
    val reach = 0
    val vidSamplingIntervalWidth = 1.0
    val dpParams = DpParams(1.0, 1.0)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 2.5e-7
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when vid sampling interval width is small`() {
    val reach = 10
    val vidSamplingIntervalWidth = 1e-4
    val dpParams = DpParams(1e-3, 1e-9)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 1701291910858389.8
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `variance returns a value when reach is large`() {
    val reach = 3e8.toInt()
    val vidSamplingIntervalWidth = 0.7
    val dpParams = DpParams(1e-2, 1e-15)
    val reachMeasurementParams = ReachMeasurementParams(vidSamplingIntervalWidth, dpParams)

    val variance = directReachMeasurementStatistics.variance(reach, reachMeasurementParams)
    val expect = 129519192.01384492
    val percentageError = percentageError(variance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when reaches are overlapped`() {
    val covariance =
      directReachMeasurementStatistics.covariance(
        3e6.toInt(),
        3e6.toInt(),
        3e6.toInt(),
        0.7,
        0.2,
        0.7
      )
    val expect = 1285714.2857142845
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when one reach is small`() {
    val covariance =
      directReachMeasurementStatistics.covariance(1, 3e6.toInt(), 3e6.toInt(), 0.5, 0.4, 0.7)
    val expect = 2.220446049250313e-16
    val percentageError = percentageError(covariance, expect)
    assertThat(percentageError).isLessThan(PERCENTAGE_ERROR_TOLERANCE)
  }

  @Test
  fun `covariance returns a value when no overlapping reach`() {
    val covariance =
      directReachMeasurementStatistics.covariance(
        3e6.toInt(),
        3e6.toInt(),
        6e6.toInt(),
        0.7,
        0.7,
        0.7,
      )
    val expect = 0.0
    assertThat(covariance).isEqualTo(expect)
  }

  companion object {
    fun percentageError(estimate: Double, truth: Double): Double {
      return if (truth == 0.0) {
        estimate
      } else {
        abs(1.0 - (estimate / truth))
      }
    }

    const val PERCENTAGE_ERROR_TOLERANCE = 1e-2
  }
}
