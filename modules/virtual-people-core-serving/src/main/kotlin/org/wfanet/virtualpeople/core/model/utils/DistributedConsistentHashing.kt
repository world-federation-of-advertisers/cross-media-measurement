// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.virtualpeople.core.model.utils

import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets
import kotlin.math.ln

/**
 * The Kotlin implementation of consistent hashing for distributions.
 *
 * A distribution is represented by a set of choices, with a given probability for each choice to be
 * the output of the hashing.
 *
 * The details of the algorithm can be found in
 * https://github.com/world-federation-of-advertisers/virtual_people_examples/blob/main/notebooks/Consistent_Hashing.ipynb
 */
class DistributedConsistentHashing(distribution: List<DistributionChoice>) {
  private val normalizedDistribution: List<DistributionChoice>

  init {
    if (distribution.isEmpty()) {
      error("The given distribution is empty.")
    }

    /** Returns error status for any negative probability, and gets sum of probabilities. */
    var probabilitiesSum = 0.0
    distribution.forEach {
      if (it.probability < 0) {
        error("Negative probability ${it.probability} is provided.")
      }
      probabilitiesSum += it.probability
    }
    if (probabilitiesSum < 1 - NORMALIZE_ERROR || probabilitiesSum > 1 + NORMALIZE_ERROR) {
      error("Probabilities do not sum to 1. $probabilitiesSum")
    }

    /** Normalizes the probabilities. */
    normalizedDistribution =
      distribution.map { DistributionChoice(it.choiceId, it.probability / probabilitiesSum) }
  }

  /**
   * A Kotlin version of the Python function ConsistentHashing.hash in
   * https://github.com/world-federation-of-advertisers/virtual_people_examples/blob/main/notebooks/Consistent_Hashing.ipynb
   */
  fun hash(randomSeed: String): Int {
    var choiceId = 0
    var choiceXi = Double.MAX_VALUE
    normalizedDistribution.forEach {
      val xi = computeXi(randomSeed, it.choiceId, it.probability)
      if (choiceXi > xi) {
        choiceId = it.choiceId
        choiceXi = xi
      }
    }
    return choiceId
  }

  companion object {
    private const val NORMALIZE_ERROR: Double = 0.0000001

    private fun getFullSeed(randomSeed: String, choice: Int): String {
      return "consistent-hashing-$randomSeed-$choice"
    }

    private fun floatHash(fullSeed: String): Double {
      /**
       * FarmHash C++ use uint64 as output type. FarmHash Java use Long as output type. Convert to
       * ULong to make sure our Kotlin and C++ implementations match each other.
       */
      return Hashing.farmHashFingerprint64()
        .hashString(fullSeed, StandardCharsets.UTF_8)
        .asLong()
        .toULong()
        .toDouble() / ULong.MAX_VALUE.toDouble()
    }

    private fun computeXi(randomSeed: String, choice: Int, probability: Double): Double {
      return -ln(floatHash(getFullSeed(randomSeed, choice))) / probability
    }
  }
}

data class DistributionChoice(val choiceId: Int, val probability: Double)
