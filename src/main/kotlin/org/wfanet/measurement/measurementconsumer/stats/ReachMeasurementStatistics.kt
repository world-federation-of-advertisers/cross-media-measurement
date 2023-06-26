package org.wfanet.measurement.measurementconsumer.stats

interface ReachMeasurementStatistics {

  /** Outputs the variance of the given [reach]. */
  fun variance(reach: Int, params: ReachMeasurementParams): Double

  /** Outputs the covariance of the given two reaches. */
  fun covariance(
    thisReach: Int,
    thatReach: Int,
    unionReach: Int,
    thisSamplingWidth: Double,
    thatSamplingWidth: Double,
    unionSamplingWidth: Double
  ): Double
}
