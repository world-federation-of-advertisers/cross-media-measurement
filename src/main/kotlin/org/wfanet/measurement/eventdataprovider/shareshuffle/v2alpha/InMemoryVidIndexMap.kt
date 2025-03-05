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

package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntry
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntryKt.value
import org.wfanet.measurement.eventdataprovider.shareshuffle.vidIndexMapEntry

/**
 * An exception that encapsulates the inconsistency between a PopulationSpec and a VID index map.
 */
class InconsistentIndexMapAndPopulationSpecException(
  vidsNotInPopulationSpec: List<Long>,
  vidsNotInIndexMap: List<Long>,
) : Exception(buildMessage(vidsNotInPopulationSpec, vidsNotInIndexMap)) {
  companion object {
    const val MAX_LIST_SIZE = 10

    private fun buildMessage(
      vidsNotInPopulationSpec: List<Long>,
      vidsNotInIndexMap: List<Long>,
    ): String {
      return buildString {
        appendLine("The provided IndexMap and PopulationSpec are inconsistent.")
        appendLine("First IDs in the indexMap, but not in the populationSpec (max $MAX_LIST_SIZE):")
        if (vidsNotInPopulationSpec.isNotEmpty()) {
          appendLine(
            if (vidsNotInPopulationSpec.size > MAX_LIST_SIZE) {
                vidsNotInPopulationSpec.take(MAX_LIST_SIZE)
              } else {
                vidsNotInPopulationSpec
              }
              .joinToString()
          )
        }

        if (vidsNotInIndexMap.isNotEmpty()) {
          appendLine("IDs in the populationSpec, but not in the indexMap (max $MAX_LIST_SIZE):")
          appendLine(
            if (vidsNotInIndexMap.size > MAX_LIST_SIZE) {
                vidsNotInIndexMap.take(MAX_LIST_SIZE)
              } else {
                vidsNotInIndexMap
              }
              .joinToString()
          )
        }
      }
    }
  }
}

/**
 * An implementation of [VidIndexMap] that holds the Map in memory.
 *
 * See build methods in the companion object.
 *
 * @param[populationSpec] The [PopulationSpec] represented by this map
 * @param[indexMap] The map of VIDs to indexes
 * @constructor Create a [VidIndexMap] for the given [PopulationSpec] and indexMap
 * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
 */
class InMemoryVidIndexMap
private constructor(
  override val populationSpec: PopulationSpec,
  private val indexMap: HashMap<Long, Int>,
) : VidIndexMap {
  override val size
    get() = indexMap.size.toLong()

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  override operator fun get(vid: Long): Int = indexMap[vid] ?: throw VidNotFoundException(vid)

  /** Get an Iterator for the VidIndexMapEntries of this VidIndexMap. */
  override operator fun iterator(): Iterator = Iterator()

  /** A data class for a VID and its hash value. */
  data class VidAndHash(val vid: Long, val hash: Long) : Comparable<VidAndHash> {
    override operator fun compareTo(other: VidAndHash): Int =
      compareValuesBy(this, other, { it.hash }, { it.vid })
  }

  companion object {
    /**
     * A salt value to ensure the output of the hash used by the VidIndexMap is different from other
     * functions that hash VIDs (e.g. the labeler). These are the first several digits of phi (the
     * golden ratio) added to the date this value was created.
     */
    private val SALT = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

    /**
     * Create a [InMemoryVidIndexMap] given a [PopulationSpec] and a hash function
     *
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
     */
    @JvmStatic
    fun build(populationSpec: PopulationSpec): InMemoryVidIndexMap {
      return buildInternal(populationSpec, ::hashVidToLongWithFarmHash)
    }

    /**
     * Create an [InMemoryVidIndexMap] for the given [PopulationSpec] and Sequence of
     * VidIndexMapEntries.
     *
     * This method requires that the VIDs in the Sequence match exactly the VIDs in the
     * populationSpec.
     *
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @param[indexMapEntries] The complete set of entries for the PopulationSpec. The [value] field
     *   of each [VidIndexMapEntry] is ignored.
     * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
     * @throws [InconsistentIndexMapAndPopulationSpecException] if the inputs are inconsistent.
     */
    suspend fun build(
      populationSpec: PopulationSpec,
      indexMapEntries: Flow<VidIndexMapEntry>,
    ): InMemoryVidIndexMap {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()

      val indexMap = hashMapOf<Long, Int>()

      // Ensure the indexMap is contained by the population spec
      val populationRanges: List<LongRange> =
        populationSpec.subpopulationsList.flatMap { subPop ->
          subPop.vidRangesList.map { (it.startVid..it.endVidInclusive) }
        }

      val vidsNotInPopulationSpec = mutableListOf<Long>()
      indexMapEntries.collect { vidEntry ->
        val vid = vidEntry.key
        indexMap[vid] = vidEntry.value.index
        var vidFound = false
        for (range in populationRanges) {
          // Ensure the VID is in one of the ranges. We already know the ranges are disjoint.
          if (range.contains(vid)) {
            vidFound = true
            break
          }
        }
        if (
          !vidFound &&
            vidsNotInPopulationSpec.size <
              InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE
        ) {
          vidsNotInPopulationSpec.add(vid)
        }
      }

      // Ensure the populationSpec is contained by the indexMap
      val vidsNotInIndexMap: List<Long> =
        populationRanges.flatMap { range ->
          range.filter { vid ->
            !indexMap.containsKey(vid) &&
              vidsNotInPopulationSpec.size <
                InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE
          }
        }

      if (vidsNotInPopulationSpec.isNotEmpty() || vidsNotInIndexMap.isNotEmpty()) {
        throw InconsistentIndexMapAndPopulationSpecException(
          vidsNotInPopulationSpec,
          vidsNotInIndexMap,
        )
      }
      return InMemoryVidIndexMap(populationSpec, indexMap)
    }

    /**
     * Hash a VID with FarmHash and return the output as a [Long]
     *
     * The input of the hash function is determined by converting the [vid] to a byte array with big
     * endian ordering and concatenating the [salt] to it.
     *
     * This input is passed to farmHashFingerprint64() whose output is a byte array.
     *
     * The bytearray is converted to a long in little endian order, which is then returned.
     *
     * @param [vid] the vid to hash
     * @param [salt] the value concatenated to the [vid] prior to hashing
     * @returns the hash of the vid
     */
    private fun hashVidToLongWithFarmHash(vid: Long, salt: ByteString): Long {
      val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
      return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
    }

    /**
     * Same as the build function above that takes a populationSpec with the addition that this
     * function allows the client to specify the VID hash function. This function is exposed for
     * testing and should not be used by client code.
     */
    @VisibleForTesting
    fun buildInternal(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
    ): InMemoryVidIndexMap {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
      val indexMap = hashMapOf<Long, Int>()
      val hashes = mutableListOf<VidAndHash>()
      for (subPop in populationSpec.subpopulationsList) {
        for (range in subPop.vidRangesList) {
          for (vid in range.startVid..range.endVidInclusive) {
            hashes.add(VidAndHash(vid, hashFunction(vid, SALT)))
          }
        }
      }
      hashes.sortWith(compareBy { it })

      for ((index, vidAndHash) in hashes.withIndex()) {
        indexMap[vidAndHash.vid] = index
      }
      return InMemoryVidIndexMap(populationSpec, indexMap)
    }
  }

  /** An iterator over the VidIndexMapEntries of this VidIndexMap */
  inner class Iterator : kotlin.collections.Iterator<VidIndexMapEntry> {
    private val vidIndexIterator = this@InMemoryVidIndexMap.indexMap.iterator()

    override fun hasNext(): Boolean = vidIndexIterator.hasNext()

    override fun next(): VidIndexMapEntry {
      val (k, v) = vidIndexIterator.next()
      return vidIndexMapEntry {
        key = k
        value = value {
          index = v
          unitIntervalValue = v.toDouble() / this@InMemoryVidIndexMap.size
        }
      }
    }
  }
}
