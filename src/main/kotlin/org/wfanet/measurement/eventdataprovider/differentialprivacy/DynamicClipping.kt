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

import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt
import org.apache.commons.math3.distribution.NormalDistribution
import org.jetbrains.annotations.VisibleForTesting

/**
 * Computes impression or duration measurement's capped noised cumulative histogram and optimized
 * dynamic threshold with Dynamic Clipping algorithm and ACDP parameters. Dynamic Clipping algorithm
 * requires the system to use Gaussian noise and ACDP composition.
 *
 * @param queryRho ACDP rho of the query converted from dpParams(epsilon, delta).
 * @param measurementType Impression or Duration type.
 * @param maxThreshold (Optional)The maximum threshold in the cumulativeHistogramList.
 */
class DynamicClipping(
  private val queryRho: Double,
  measurementType: MeasurementType,
  private val maxThreshold: Int = defaultMaxThreshold(measurementType),
) {
  private lateinit var cumulativeHistogramList: List<Double>
  private val slidingWindowSize: Int =
    when (measurementType) {
      MeasurementType.IMPRESSION -> IMPRESSION_SLIDING_WINDOW_SIZE
      MeasurementType.DURATION -> DURATION_SLIDING_WINDOW_SIZE
    }

  /**
   * Computes impression's capped noised cumulative histogram and optimized dynamic threshold.
   *
   * This algorithm is a variation of the original algorithm described in the doc and has a better
   * error performance. It first chooses a candidate threshold and uses it to reduce the maximum
   * threshold. The threshold and lower maximum threshold improves the accuracy of the cumulative
   * histogram estimates, and we can use this improved accuracy to refine the choice of clipping
   * threshold.
   *
   * @param frequencyMap Map representation of impression's frequency histogram.
   * @return [Result] Data class of noisedCumulativeHistogramList and threshold.
   */
  fun computeImpressionCappedHistogram(frequencyMap: Map<Long, Long>): Result {
    cumulativeHistogramList = generateCumulativeHistogram(frequencyHistogramMapToList(frequencyMap))

    var localMaxThreshold = maxThreshold
    var noisedCumulativeHistogramList = generateNoisedCumulativeHistogram(maxThreshold, queryRho)

    // Find out the threshold we stopped at based on the stopping criterion.
    var threshold = defaultChooseThreshold(noisedCumulativeHistogramList, maxThreshold, queryRho)

    // Refine the maximum threshold based on the threshold we stopped at. If this refined maximum
    // threshold is larger than the original threshold, this algorithm reduces to the
    // original algorithm in the doc.
    val refinedLocalMaxThreshold = (1.5 * threshold).toInt() + 1

    // Use refinedLocalMaxThreshold to try to generate a candidate threshold and noise
    // the cumulative histogram.
    if (refinedLocalMaxThreshold < localMaxThreshold) {
      val rhoRemaining =
        (localMaxThreshold - refinedLocalMaxThreshold) * queryRho / localMaxThreshold
      // Use the refinedLocalMaxThreshold and rhoRemaining to generate a new noised cumulative
      // histogram by linearly combining it with the previous noisedCumulativeHistogramList inside
      // useRemainingCharge.
      noisedCumulativeHistogramList =
        useRemainingCharge(noisedCumulativeHistogramList, refinedLocalMaxThreshold, rhoRemaining)
      // Update the maximum threshold and based on it, we find a new threshold we stopped at based
      // on the stopping criterion.
      localMaxThreshold = refinedLocalMaxThreshold
      val newThreshold =
        defaultChooseThreshold(noisedCumulativeHistogramList, localMaxThreshold, queryRho)
      threshold = max(threshold, newThreshold)
    }

    // If the threshold we stopped at is less than the localMaxThreshold, compute the remaining
    // rho, and use it to generate the noised cumulative histogram again by linearly combine it with
    // the previous noisedCumulativeHistogramList inside useRemainingCharge.
    if (threshold < localMaxThreshold) {
      val rhoRemaining = (localMaxThreshold - threshold) * queryRho / localMaxThreshold
      noisedCumulativeHistogramList =
        useRemainingCharge(noisedCumulativeHistogramList, threshold, rhoRemaining)
    }

    return Result(noisedCumulativeHistogramList, threshold)
  }

  /**
   * Converts a histogram list into a cumulative histogram list.
   *
   * @param histogramList A list where `histogramList[k]` denotes the number of data points with
   *   value between k (inclusive) and k+1 (exclusive). The first element should be 0 which means 0
   *   impression/duration has 0 count.
   * @param durationTruncatedList Only needed for duration measurement. A list of the total change
   *   in value from taking the floor of values x with k < x < k+1. That is,
   *   `durationTruncatedList[k]` = sum(x - floor(x)) where the sum is over all data points x with k
   *   <= x < k+1. It's list of [Double] since each term in the sum(x - floor(x)) is non-integer.
   * @return cumulativeHistogramList which is a list where `cumulativeHistogramList[k]` denotes the
   *   sum of the data truncated at k + 1 minus the sum of the data truncated at k. For
   *   integer-valued data, it is the number of data points with value >= k + 1. Set it to list of
   *   [Double] which will work both for impression and duration types.
   */
  private fun generateCumulativeHistogram(
    histogramList: List<Long>,
    durationTruncatedList: List<Double> = List(histogramList.size) { 0.0 },
  ): List<Double> {
    var remainingCount = histogramList.sum()
    // histogramList.size - 1 since the first element in histogramList is 0.
    val cumulativeHistogramList = MutableList(min(maxThreshold, histogramList.size - 1)) { 0.0 }

    for (i in 0 until cumulativeHistogramList.size) {
      remainingCount -= histogramList[i]
      cumulativeHistogramList[i] = remainingCount + durationTruncatedList[i]
    }

    return cumulativeHistogramList
  }

  /**
   * Use privacy charge to generate a noised cumulative histogram from property
   * cumulativeHistogramList.
   *
   * @param maxThreshold The maximum threshold in the cumulativeHistogramList.
   * @param rho ACDP rho param.
   * @return noisedCumulativeHistogramList which is a list of [Double] since the continuous Gaussian
   *   noise sample is [Double].
   */
  private fun generateNoisedCumulativeHistogram(maxThreshold: Int, rho: Double): List<Double> {
    val sigma = BAR_SENSITIVITY * sqrt(maxThreshold.toDouble() / (2 * rho))
    // Generate noise samples from Gaussian distribution.
    val normalDistribution = NormalDistribution(0.0, sigma)

    return cumulativeHistogramList.map { it + normalDistribution.sample() }
  }

  /**
   * A default method to choose and output a clipping threshold based on a stopping condition. Note
   * that to be a valid threshold method for the algorithm, it can't read any indices of
   * noisedCumulativeHistogramList past the final chosen threshold.
   *
   * @param noisedCumulativeHistogramList A list of [Double]
   * @param maxThreshold The maximum threshold in the cumulativeHistogramList.
   * @param rho ACDP rho param.
   * @return chosen threshold in noisedCumulativeHistogramList.
   */
  private fun defaultChooseThreshold(
    noisedCumulativeHistogramList: List<Double>,
    maxThreshold: Int,
    rho: Double,
  ): Int {
    val terminationSum = (slidingWindowSize.toDouble() * BAR_SENSITIVITY / sqrt(2 * rho))

    for (threshold in slidingWindowSize - 1 until maxThreshold) {
      val slidingWindowSum =
        noisedCumulativeHistogramList
          .subList(
            threshold + 1 - slidingWindowSize,
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
   * Use remaining privacy charge to improve noisedCumulativeHistogramList below threshold.
   *
   * Given a noised cumulative histogram and a threshold below the maximum threshold, we can improve
   * the noised cumulative histogram estimates of all histogram bars below the threshold by using
   * the "remaining" privacy charge to noise the bars only below the threshold.
   *
   * @param noisedCumulativeHistogramList1 A list of [Double].
   * @param maxThreshold The maximum threshold in the cumulativeHistogramList.
   * @param rhoRemaining Remaining privacy charge if estimate only the bars below the threshold.
   * @return A new noisedCumulativeHistogramList.
   */
  private fun useRemainingCharge(
    noisedCumulativeHistogramList1: List<Double>,
    maxThreshold: Int,
    rhoRemaining: Double,
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

  /** Dynamic Clipping only supports impression and duration measurements */
  enum class MeasurementType {
    IMPRESSION,
    DURATION,
  }

  /**
   * Represents the Dynamic Clipping result for impression or duration.
   *
   * @param noisedCumulativeHistogramList cumulativeHistogramList with Gaussian noise added to each
   *   histogram bar. It's a list of [Double] since the continuous Gaussian noise sample is
   *   [Double].The total impression/duration count will be the sum of all the bars.
   * @param threshold Optimized dynamic impression/duration cutoff threshold.
   */
  data class Result(val noisedCumulativeHistogramList: List<Double>, val threshold: Int)

  companion object {
    // The default max thresholds are based on analysis result and are subject to change.
    private const val IMPRESSION_MAX_THRESHOLD = 200
    private const val DURATION_MAX_THRESHOLD = 1800
    // The default sliding window sizes are based on analysis result and are subject to change.
    private const val IMPRESSION_SLIDING_WINDOW_SIZE = 3
    private const val DURATION_SLIDING_WINDOW_SIZE = 30
    private const val BAR_SENSITIVITY = 1.0

    private fun defaultMaxThreshold(measurementType: MeasurementType) =
      when (measurementType) {
        MeasurementType.IMPRESSION -> IMPRESSION_MAX_THRESHOLD
        MeasurementType.DURATION -> DURATION_MAX_THRESHOLD
      }

    /** A helper function to convert frequencyHistogramMap to histogramList for DynamicClipping */
    @VisibleForTesting
    internal fun frequencyHistogramMapToList(frequencyMap: Map<Long, Long>): List<Long> {
      // frequencyMap.keys.max().toInt() + 1 since the first element in histogramList should be 0.
      val histogramList = MutableList(frequencyMap.keys.max().toInt() + 1) { 0L }
      for ((frequency, count) in frequencyMap) {
        histogramList[frequency.toInt()] = count
      }

      return histogramList
    }
  }
}
