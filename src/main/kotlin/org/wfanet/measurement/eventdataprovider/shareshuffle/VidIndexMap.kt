// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.protobuf.ByteString
import java.math.BigInteger
import java.nio.ByteOrder
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.toByteString

class VidNotFoundException(vid: Long) : Exception("Failed to find VID $vid.")

/**
 * A mapping of VIDs to [FrequencyVector] indexes for a [PopulationSpec].
 *
 * @param [salt] If provided, this value is append to the VID before hashing it
 * and will alter the indexes of the VIDs.
 *
 * @constructor Creates a [VidIndexMap] for the given [PopulationSpec]
 */
class VidIndexMap(
  populationSpec: PopulationSpec,
  private val salt : ByteString = ByteString.EMPTY) {
  /** The number of VIDs managed by this VidIndexMap */
  val vidCount get()=indexMap.size

  /** A map of a VID to its index in the [Frequency Vector]. */
  private val indexMap = hashMapOf<Long, Int>()

  init {
    PopulationSpecValidator.validateVidRangesList(populationSpec)

    val hashes = mutableListOf<Pair<Long, BigInteger>>()

    for (subPop in populationSpec.subpopulationsList) {
      for (range in subPop.vidRangesList) {
        for (vid in range.startVid.. range.endVidInclusive) {
          val hash = BigInteger(1, getVidHash(vid).toByteArray())
          hashes.add(Pair(vid, hash))
        }
      }
    }

    hashes.sortWith(compareBy<Pair<Long, BigInteger>> { it.second }.thenBy { it.first })

    for ((index, pair) in hashes.withIndex()) {
      indexMap[pair.first] = index
    }
  }

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  fun getVidIndex(vid: Long): Int =
    if (indexMap.containsKey(vid)) {
      indexMap[vid]!!
    } else {
      throw VidNotFoundException(vid)
    }

  private fun getVidHash(vid: Long): ByteString {
    val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    return Hashing.hashSha256(hashInput)
  }

}
