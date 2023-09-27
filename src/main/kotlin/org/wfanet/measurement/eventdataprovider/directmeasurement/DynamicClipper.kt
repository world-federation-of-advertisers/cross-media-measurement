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

import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt
import org.apache.commons.math3.distribution.NormalDistribution

/** Represents the Dynamic Clipping result for impression or duration. */
data class DynamicClipResult(val threshold: Int, val noisedCumulativeHistogramList: List<Double>)

/**
 * Computes impression and duration measurement's noised cumulative histogram and optimized
 * threshold with Dynamic Clipping algorithm and ACDP parameters. Dynamic Clipping algorithm
 * requires the system to use Gaussian noise and ACDP.
 *
 * @param histogramList An impression or duration histogram converted from the labeled event query.
 *   For impression, the index is number of impression. For duration, the index is watch duration in
 *   seconds. The list element is data counts. The first element should be zero which means 0
 *   impression/duration has zero counts.
 * @param queryRho ACDP rho of the query converted from dpParams(epsilon, delta).
 * @param maxThreshold The maximum number of elements in the cumulativeHistogramList.
 */
class DynamicClipper(
  histogramList: List<Long>,
  private val queryRho: Double,
  private val maxThreshold: Int = MAX_THRESHOLD
) {

  private val cumulativeHistogramList = generateCumulativeHistogram(histogramList)

  /**
   * Computes noised cumulative histogram and optimized threshold with two-phase approach and
   * outputs the result. This algorithm is a variation of the original one described in the doc but
   * has a better error performance. It first chooses a candidate threshold and uses it to reduce
   * the maximum threshold. The threshold and lower maximum threshold improves the accuracy of the
   * cumulative histogram estimates, and we can use this improved accuracy to refine the choice of
   * clipping threshold.
   *
   * @return [DynamicClipResult] Data class of threshold and noisedCumulativeHistogramList.
   */
  fun runTwoPhaseDynamicClipping(): DynamicClipResult {
    var localMaxThreshold = maxThreshold
    var noisedCumulativeHistogramList = generateNoisedCumulativeHistogram(maxThreshold, queryRho)

    // Find out the threshold(cap) we stopped at based on the stopping criterion.
    var threshold = defaultChooseThreshold(noisedCumulativeHistogramList, maxThreshold, queryRho)

    // Refine the maximum threshold based on the threshold we stopped at.
    val refinedLocalMaxThreshold = (1.5 * threshold).toInt() + 1

    // Phase 1: use refinedLocalMaxThreshold to try to generate a candidate threshold and noise
    // cumulative histogram.
    if (refinedLocalMaxThreshold < localMaxThreshold) {
      val rhoRemaining =
        (localMaxThreshold - refinedLocalMaxThreshold) * queryRho / localMaxThreshold
      // Use the refinedLocalMaxThreshold and rhoRemaining to generate a new noised cumulative
      // histogram. Linearly combine it with the previous one inside useRemainingCharge.
      noisedCumulativeHistogramList =
        useRemainingCharge(noisedCumulativeHistogramList, refinedLocalMaxThreshold, rhoRemaining)
      // Update the maximum threshold, and based on it, we find a new threshold we stopped at based
      // on the stopping criterion.
      localMaxThreshold = refinedLocalMaxThreshold
      val newThreshold =
        defaultChooseThreshold(noisedCumulativeHistogramList, localMaxThreshold, queryRho)
      threshold = max(threshold, newThreshold)
    }

    // Phase 2: If the threshold we stopped at is less than the localMaxThreshold, compute the
    // remaining
    // rho, and use it to generate the noised cumulative histogram again, and linearly combine it
    // with the previous estimation.
    if (threshold < localMaxThreshold) {
      val rhoRemaining = (localMaxThreshold - threshold) * queryRho / localMaxThreshold
      noisedCumulativeHistogramList =
        useRemainingCharge(noisedCumulativeHistogramList, threshold, rhoRemaining)
    }

    return DynamicClipResult(threshold, noisedCumulativeHistogramList)
  }

  /**
   * Converts a histogram list into a cumulative histogram list.
   *
   * @param histogramList A list where `histogramList[k]` denotes the number of data points with
   *   value between k (inclusive) and k+1 (exclusive). The first element should be zero which means
   *   0 impression/duration has zero counts.
   * @param durationTruncatedList Only needed for duration measurement. A list of the total change
   *   in value from taking the floor of values x with k < x < k+1. That is,
   *   `durationTruncatedList[k]` = sum(x - floor(x)) where the sum is over all data points x with k
   *   <= x < k+1.
   * @return cumulativeHistogramList which is a list where `cumulativeHistogramList[k]` denotes the
   *   sum of the data truncated at k + 1 minus the sum of the data truncated at k. For
   *   integer-valued data, it is the number of data points with value >= k + 1. The first element
   *   should be zero which means 0 impression/duration has zero counts.
   */
  private fun generateCumulativeHistogram(
    histogramList: List<Long>,
    durationTruncatedList: List<Long> = List(histogramList.size) { 0 }
  ): List<Long> {
    var remainingCount = histogramList.sum()
    val cumulativeHistogramList = MutableList(min(maxThreshold, histogramList.size)) { 0L }

    for (i in 1 until cumulativeHistogramList.size) {
      cumulativeHistogramList[i] = remainingCount + durationTruncatedList[i]
      remainingCount -= histogramList[i]
    }

    return cumulativeHistogramList
  }

  /**
   * Use privacy charge to generate a noised cumulative histogram.
   *
   * @param maxThreshold The maximum number of elements in the cumulativeHistogramList.
   * @param rho ACDP rho of the query converted from dpParams(epsilon, delta).
   * @return noisedCumulativeHistogramList which is a list of [Double] since the continuous Gaussian
   *   noise sample is [Double].
   */
  private fun generateNoisedCumulativeHistogram(maxThreshold: Int, rho: Double): List<Double> {
    val sigma = BAR_SENSITIVITY * sqrt(maxThreshold.toDouble() / (2 * rho))
    // Generate noise samples from Gaussian distribution.
    val normalDistribution = NormalDistribution(0.0, sigma)
    val noisedCumulativeHistogramList = MutableList(cumulativeHistogramList.size) { 0.0 }

    for (i in 1 until cumulativeHistogramList.size) {
      noisedCumulativeHistogramList[i] = cumulativeHistogramList[i] + normalDistribution.sample()
    }

    return noisedCumulativeHistogramList
  }

  /**
   * A default method to choose and output a clipping threshold based on a stopping condition. Note
   * that to be a valid threshold method for the algorithm, it can't read any indices of
   * noisedCumulativeHistogramList past the final chosen threshold.
   *
   * @param noisedCumulativeHistogramList A list of [Double]
   * @param maxThreshold The maximum number of elements in the cumulativeHistogramList.
   * @param rho ACDP rho of the query converted from dpParams(epsilon, delta).
   * @return chosen threshold
   */
  private fun defaultChooseThreshold(
    noisedCumulativeHistogramList: List<Double>,
    maxThreshold: Int,
    rho: Double
  ): Int {
    val terminationSum =
      (IMPRESSION_SLIDING_WINDOW_SIZE.toDouble() * BAR_SENSITIVITY / sqrt(2 * rho)).toInt()

    for (threshold in IMPRESSION_SLIDING_WINDOW_SIZE - 1 until maxThreshold) {
      val slidingWindowSum =
        noisedCumulativeHistogramList
          .subList(
            threshold + 1 - IMPRESSION_SLIDING_WINDOW_SIZE,
            min(noisedCumulativeHistogramList.size, threshold + 1),
          )
          .sum()
      if (slidingWindowSum <= terminationSum) {
        return threshold + 1
      }
    }

    return maxThreshold
  }

  /**
   * Use remaining charge to improve noised histogram estimate below threshold.
   *
   * Given a noised cumulative histogram and a threshold below the maximum threshold, we can improve
   * the noised cumulative histogram estimates of all histogram bars below the threshold by using
   * the "remaining" privacy charge to noise only the bars below the threshold.
   *
   * @param noisedCumulativeHistogramList1 A list of [Double].
   * @param maxThreshold The maximum number of elements in the cumulativeHistogramList..
   * @param rhoRemaining Remaining privacy charge if estimate only the bars below the threshold.
   * @return A new noisedCumulativeHistogramList.
   */
  private fun useRemainingCharge(
    noisedCumulativeHistogramList1: List<Double>,
    maxThreshold: Int,
    rhoRemaining: Double
  ): List<Double> {
    val noisedCumulativeHistogramList2 =
      generateNoisedCumulativeHistogram(maxThreshold, rhoRemaining)

    //  Optimally combine two noisy cumulative histogram list by variance weights.
    val variance1 = 1.toDouble() / (queryRho - rhoRemaining)
    val variance2 = 1.toDouble() / rhoRemaining
    val w1 = variance2 / (variance1 + variance2)
    val w2 = variance1 / (variance1 + variance2)

    val noisedCumulativeHistogramList =
      noisedCumulativeHistogramList2.mapIndexed { index, count ->
        (w1 * noisedCumulativeHistogramList1[index]) + (w2 * count)
      }

    return noisedCumulativeHistogramList
  }

  companion object {
    private const val MAX_THRESHOLD = 200
    private const val BAR_SENSITIVITY = 1.0
    private const val IMPRESSION_SLIDING_WINDOW_SIZE = 3
    private const val DURATION_SLIDING_WINDOW_SIZE = 10
  }
}
