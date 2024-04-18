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

import com.google.common.hash.Hashing
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator.validateVidRangesList

class VidNotFoundException(vid: Long) : Exception("Failed to find VID $vid.")

/** A mapping of VIDs to [FrequencyVector] indexes for a [PopulationSpec]. */
interface VidIndexMap {
  /** Gets the index in the [FrequencyVector] for the given VID */
  operator fun get(vid: Long): Int
}

/**
 * An implementation of [VidIndexMap] that holds the Map in memory.
 *
 * This implementation of [VidIndexMap] creates the mapping from scratch given a [PopulationSpec]
 *
 * Overriding the default hash function can cause incompatibilities between EDPs which can lead to
 * bad measurement. The [hashFunction] is exposed only for testing.
 *
 * @param[populationSpec] The [PopulationSpec] to build the map for.
 * @param [hashFunction] The hash function to use for hashing VIDs.
 * @constructor Creates a [VidIndexMap] for the given [PopulationSpec]
 * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
 */
class InMemoryVidIndexMap(
  populationSpec: PopulationSpec,
  private val hashFunction: (Long, Long) -> Long = ::hashVidToLongWithFarmHash,
) : VidIndexMap {
  // TODO(@kungfucraig): Provide a constructor that reads the vid->index map from a file.

  /** The number of VIDs managed by this VidIndexMap */
  val size
    get() = indexMap.size

  /** A map of a VID to its index in the [Frequency Vector]. */
  private val indexMap = hashMapOf<Long, Int>()

  /**
   * A salt value to ensure the output of the hash used by the VidIndexMap is different from other
   * functions that hash VIDs (e.g. the labeler). These are the first several digits of phi (the
   * golden ratio) added to the date this value was created.
   */
  private val salt = 1_618_033L + 20_240_417L

  /** A data class for a VID and its hash value. */
  data class VidAndHash(val vid: Long, val hash: Long) : Comparable<VidAndHash> {
    override operator fun compareTo(other: VidAndHash): Int =
      compareValuesBy(this, other, { it.hash }, { it.vid })
  }

  init {
    validateVidRangesList(populationSpec).getOrThrow()

    val hashes = mutableListOf<VidAndHash>()
    for (subPop in populationSpec.subpopulationsList) {
      for (range in subPop.vidRangesList) {
        for (vid in range.startVid..range.endVidInclusive) {
          hashes.add(VidAndHash(vid, hashFunction(vid, salt)))
        }
      }
    }

    hashes.sortWith(compareBy<VidAndHash>() { it })

    for ((index, vidAndHash) in hashes.withIndex()) {
      indexMap[vidAndHash.vid] = index
    }
  }

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  override operator fun get(vid: Long): Int = indexMap[vid] ?: throw VidNotFoundException(vid)

  companion object {
    /**
     * Hash a VID with FarmHash and return the output as a [Long]
     *
     * @param [vid] the vid to hash
     * @returns The first 64-bits of the hash as a [Long]
     */
    fun hashVidToLongWithFarmHash(vid: Long, salt: Long): Long {
      val hasher = Hashing.farmHashFingerprint64().newHasher()
      return hasher.putLong(vid + salt).hash().asLong()
    }
  }
}
