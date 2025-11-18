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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntry

class VidNotFoundException(vid: Long) : Exception("Failed to find VID $vid.")

/** A data class for a VID and its hash value. */
data class VidAndHash(val vid: Int, val hash: Long) : Comparable<VidAndHash> {
  override operator fun compareTo(other: VidAndHash): Int =
    compareValuesBy(this, other, { it.hash }, { it.vid })
}

/** A mapping of VIDs to [FrequencyVector] indexes for a [PopulationSpec]. */
interface VidIndexMap {
  /** Gets the index in the [FrequencyVector] for the given VID */
  operator fun get(vid: Long): Int

  /** The number of VIDs managed by this VidIndexMap */
  val size: Long

  /** The PopulationSpec used to create this map */
  val populationSpec: PopulationSpec

  /**
   * Get an iterator for the VidIndexMapEntries of this VidIndexMap.
   *
   * The order of iteration is undefined.
   */
  operator fun iterator(): Iterator<VidIndexMapEntry>

  companion object {
    val HASH_SALT: ByteString = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

    val EMPTY: VidIndexMap =
      object : VidIndexMap {
        override fun get(vid: Long): Int = throw VidNotFoundException(vid)

        override val size: Long = 0L

        override val populationSpec: PopulationSpec
          get() = PopulationSpec.getDefaultInstance()

        override fun iterator(): Iterator<VidIndexMapEntry> =
          emptyList<VidIndexMapEntry>().iterator()
      }

    fun validatePopulationSpec(populationSpec: PopulationSpec) {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
    }

    fun hashVidToLongWithFarmHash(vid: Long, salt: ByteString = HASH_SALT): Long {
      val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
      return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
    }

    @VisibleForTesting
    fun collectVids(populationSpec: PopulationSpec): IntArray {
      val totalVidCount =
        populationSpec.subpopulationsList.fold(0L) { acc, subPop ->
          acc +
            subPop.vidRangesList.fold(0L) { rangeAcc, range ->
              rangeAcc + (range.endVidInclusive - range.startVid + 1)
            }
        }

      require(totalVidCount <= Int.MAX_VALUE) { "Total VID count exceeds supported maximum." }
      val vids = IntArray(totalVidCount.toInt())
      var writeIndex = 0
      for (subPop in populationSpec.subpopulationsList) {
        for (range in subPop.vidRangesList) {
          for (vid in range.startVid..range.endVidInclusive) {
            require(vid < Integer.MAX_VALUE) {
              "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}"
            }
            vids[writeIndex++] = vid.toInt()
          }
        }
      }
      return vids
    }
  }
}
