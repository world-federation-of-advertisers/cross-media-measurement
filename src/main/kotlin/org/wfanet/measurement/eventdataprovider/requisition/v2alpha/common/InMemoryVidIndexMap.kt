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

import com.google.protobuf.ByteString
import kotlin.collections.withIndex
import kotlinx.coroutines.flow.Flow
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
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
 * An implementation of [VidIndexMap] that holds the map in memory and performs all work
 * sequentially.
 *
 * This constructor is private. See build methods in the companion object.
 *
 * Note that this class takes ownership of the `indexMap` and that the caller must not modify it
 * after calling this constructor.
 *
 * @param[populationSpec] The [PopulationSpec] represented by this map
 * @param[indexMap] The map of VIDs to indexes
 * @constructor Create a [VidIndexMap] for the given [PopulationSpec] and indexMap
 * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
 */
class InMemoryVidIndexMap
private constructor(
  override val populationSpec: PopulationSpec,
  private val indexMap: HashMap<Int, Int>,
) : VidIndexMap {
  override val size
    get() = indexMap.size.toLong()

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  override operator fun get(vid: Long): Int {
    require(vid < Integer.MAX_VALUE) { "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}" }
    return indexMap[vid.toInt()] ?: throw VidNotFoundException(vid)
  }

  /** Get an Iterator for the VidIndexMapEntries of this VidIndexMap. */
  override operator fun iterator(): Iterator = Iterator()

  companion object {
    @JvmStatic
    fun build(populationSpec: PopulationSpec): InMemoryVidIndexMap {
      return buildInternal(populationSpec, VidIndexMap::hashVidToLongWithFarmHash)
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
      VidIndexMap.validatePopulationSpec(populationSpec)

      val indexMap = hashMapOf<Int, Int>()

      // Ensure the indexMap is contained by the population spec
      val populationRanges: List<LongRange> =
        populationSpec.subpopulationsList.flatMap { subPop ->
          subPop.vidRangesList.map { (it.startVid..it.endVidInclusive) }
        }

      val vidsNotInPopulationSpec = mutableListOf<Long>()
      indexMapEntries.collect { vidEntry ->
        val vid = vidEntry.key
        require(vid < Integer.MAX_VALUE) {
          "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}"
        }
        indexMap[vid.toInt()] = vidEntry.value.index
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
            require(vid < Integer.MAX_VALUE) {
              "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}"
            }
            !indexMap.containsKey(vid.toInt()) &&
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
     * Same as the build function above that takes a populationSpec with the addition that this
     * function allows the client to specify the VID hash function. This function is exposed for
     * testing and should not be used by client code.
     */
    @VisibleForTesting
    fun buildInternal(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
    ): InMemoryVidIndexMap {
      VidIndexMap.validatePopulationSpec(populationSpec)
      val indexMap = hashMapOf<Int, Int>()
      val hashes = generateHashes(populationSpec, hashFunction).sortedBy { it }

      for ((index, vidAndHash) in hashes.withIndex()) {
        indexMap[vidAndHash.vid] = index
      }
      return InMemoryVidIndexMap(populationSpec, indexMap)
    }

    @VisibleForTesting
    fun generateHashes(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
    ): List<VidAndHash> {
      return VidIndexMap.collectVids(populationSpec).map { vid ->
        VidAndHash(vid, hashFunction(vid.toLong(), VidIndexMap.HASH_SALT))
      }
    }
  }

  /** An iterator over the VidIndexMapEntries of this VidIndexMap */
  inner class Iterator : kotlin.collections.Iterator<VidIndexMapEntry> {
    private val vidIndexIterator = this@InMemoryVidIndexMap.indexMap.iterator()

    override fun hasNext(): Boolean = vidIndexIterator.hasNext()

    override fun next(): VidIndexMapEntry {
      val (k, v) = vidIndexIterator.next()
      return vidIndexMapEntry {
        key = k.toLong()
        value = value {
          index = v
          unitIntervalValue = v.toDouble() / this@InMemoryVidIndexMap.size
        }
      }
    }
  }
}
