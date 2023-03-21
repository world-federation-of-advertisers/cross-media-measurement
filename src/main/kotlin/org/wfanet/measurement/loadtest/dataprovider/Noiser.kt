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

package org.wfanet.measurement.loadtest.dataprovider

import org.apache.commons.math3.distribution.AbstractRealDistribution

/** The reach and frequency estimation of a computation. */
data class ReachAndFrequencyPair(val reach: Long, val frequency: Map<Long, Double>)

/** A base noiser class for direct measurement */
interface Noiser {
  val distributionForReach: AbstractRealDistribution
  val distributionForFrequency: AbstractRealDistribution

  /**
   * Add publisher noise to calculated direct reach and frequency.
   *
   * @param reachValue Direct reach value.
   * @param frequencyMap Direct frequency.
   * @return Pair of noised reach value and frequency map.
   */
  fun addPublisherNoise(
    reachValue: Long,
    frequencyMap: Map<Long, Double>,
  ): ReachAndFrequencyPair {
    val noisedReachValue = reachValue + distributionForReach.sample().toInt()
    val noisedFrequencyMap = mutableMapOf<Long, Double>()
    frequencyMap.forEach { (frequency, percentage) ->
      noisedFrequencyMap[frequency] =
        (percentage * reachValue.toDouble() + distributionForFrequency.sample()) /
          reachValue.toDouble()
    }

    return ReachAndFrequencyPair(noisedReachValue, noisedFrequencyMap)
  }
}
