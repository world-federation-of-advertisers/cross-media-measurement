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
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import java.nio.ByteOrder
import java.util.Arrays
import java.util.Locale
import kotlin.collections.withIndex
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntry
import org.wfanet.measurement.eventdataprovider.shareshuffle.VidIndexMapEntryKt.value
import org.wfanet.measurement.eventdataprovider.shareshuffle.vidIndexMapEntry
import java.util.Collections

/**
 * An exception that encapsulates the inconsistency between a PopulationSpec and a VID index map.
 */
class InconsistentIndexMapAndPopulationSpecException(
  vidsNotInPopulationSpec: List<Long>,
  vidsNotInIndexMap: List<Long>,
) : Exception(buildMessage(vidsNotInPopulationSpec, vidsNotInIndexMap)) {
  companion object Companion {
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
 * This constructor is private. See build methods in the companion object.
 *
 * Note that this class takes ownership of the `indexMap` and that the caller must not modify it
 * after calling this constructor.
 *
 * The map uses FastUtil's primitive `int` storage to minimize memory and cpu usage.
 * Int2IntOpenHashMap delivers a 2× speedup for building the hash map and 3x savings in memory usage.
 *
 * @param[populationSpec] The [PopulationSpec] represented by this map
 * @param[indexMap] The map of VIDs to indexes
 * @constructor Create a [VidIndexMap] for the given [PopulationSpec] and indexMap
 * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
 */
class InMemoryVidIndexMap
private constructor(
  override val populationSpec: PopulationSpec,
  private val indexMap: Int2IntOpenHashMap,
) : VidIndexMap {
  init {
    indexMap.defaultReturnValue(-1)
  }

  override val size
    get() = indexMap.size.toLong()


  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  override operator fun get(vid: Long): Int {
    require(vid < Integer.MAX_VALUE) { "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}" }
    val key = vid.toInt()
    val value = indexMap.get(key)
    if (value == indexMap.defaultReturnValue() && !indexMap.containsKey(key)) {
      throw VidNotFoundException(vid)
    }
    return value
  }

  /** Get an Iterator for the VidIndexMapEntries of this VidIndexMap. */
  override operator fun iterator(): Iterator = Iterator()

  /** A data class for a VID and its hash value. */
  data class VidAndHash(val vid: Int, val hash: Long) : Comparable<VidAndHash> {
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
     * The default number of partitions to create when partitioning the VID/hash array.
     * For our use case, 8 partitions is a good compromise between parallelism and overhead.
     */
    const val DEFAULT_PARTITION_COUNT = 8

    /**
     * Create a [InMemoryVidIndexMap] given a [PopulationSpec] and a hash function
     *
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @param[runInParallel] whether to perform hashing, sorting, and population on multiple threads
     * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
     */
    @JvmStatic
    fun build(
      populationSpec: PopulationSpec,
      runInParallel: Boolean = false,
    ): InMemoryVidIndexMap {
      return buildInternal(populationSpec, Companion::hashVidToLongWithFarmHash, runInParallel)
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

      val indexMap = Int2IntOpenHashMap().apply { defaultReturnValue(-1) }

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
        indexMap.put(vid.toInt(), vidEntry.value.index)
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
     * 
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @param[hashFunction] The function to hash VIDs
     * @param[runInParallel] whether to perform hashing, sorting, and population on multiple threads
     * @param[partitionCount] The number of partitions / threads to create during population
     */
    @VisibleForTesting
    fun buildInternal(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
      runInParallel: Boolean,
      partitionCount: Int = DEFAULT_PARTITION_COUNT,
    ): InMemoryVidIndexMap {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
      val indexMap = Int2IntOpenHashMap().apply { defaultReturnValue(-1) }

      if (runInParallel) {
        val hashes: Array<VidAndHash> = generateHashesParallel(populationSpec, hashFunction)
        Arrays.parallelSort(hashes)
        populateIndexMapPartitionMerge(hashes, indexMap, partitionCount = partitionCount)
      } else {
        val hashes: Array<VidAndHash> = generateHashesSequential(populationSpec, hashFunction)
        hashes.sort()
        for ((index, vidAndHash) in hashes.withIndex()) {
          indexMap.put(vidAndHash.vid, index)
        }
      }
      return InMemoryVidIndexMap(populationSpec, indexMap)
    }

    /**
     * Populates `indexMap` from a sorted VID/hash array by processing disjoint partitions in parallel.
     *
     * Callers must ensure `hashesArray` is globally sorted and contains unique VIDs.
     *
     * @param hashesArray globally sorted VID/hash pairs.
     * @param indexMap destination map to populate.
     * @param partitionCount number of coroutine partitions to launch.
     */
    @VisibleForTesting
    fun populateIndexMapPartitionMerge(
      hashesArray: Array<VidAndHash>,
      indexMap: Int2IntOpenHashMap,
      partitionCount: Int,
    ) {
      val partialMaps = mutableListOf<Int2IntOpenHashMap>()
      applyPartitioned(hashesArray.size, partitionCount) { partition ->
        val partialMap = Int2IntOpenHashMap(partition.length).apply { defaultReturnValue(-1) }
        for (globalIndex in partition.startIndex until partition.endIndexExclusive) {
          val vidAndHash = hashesArray[globalIndex]
          partialMap.put(vidAndHash.vid, globalIndex)
        }

        partialMaps += partialMap
      }

      partialMaps.forEach(indexMap::putAll)
    }

    /**
     * Computes VID/hash pairs for `populationSpec` using coroutine partitions.
     *
     * @param populationSpec source VID ranges.
     * @param hashFunction VID hash function.
     * @param partitionCount optional upper bound on partition count. When null, defaults to
     *   available processors.
     * @return array of VID/hash pairs in population order.
     */
    @VisibleForTesting
    fun generateHashesParallel(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
      partitionCount: Int? = null,
    ): Array<VidAndHash> {
      val vidCollection = collectVids(populationSpec)
      val vids = vidCollection.vids
      val totalVidCountInt = vidCollection.count
      if (totalVidCountInt == 0) {
        return emptyArray()
      }

      val boundedPartitionCount =
        minOf(
          totalVidCountInt,
          (partitionCount ?: Runtime.getRuntime().availableProcessors())
            .coerceAtLeast(1),
        )
      val results = Array(totalVidCountInt) { VidAndHash(0, 0L) }

      applyPartitioned(totalVidCountInt, boundedPartitionCount) { partition ->
        for (i in partition.startIndex until partition.endIndexExclusive) {
          val vidInt = vids[i]
          val hash = hashFunction(vidInt.toLong(), SALT)
          results[i] = VidAndHash(vidInt, hash)
        }
      }

      return results
    }

    /**
     * Computes VID/hash pairs for `populationSpec` on the calling thread.
     *
     * @param populationSpec source VID ranges.
     * @param hashFunction VID hash function.
     * @return array of VID/hash pairs in population order.
     */
    @VisibleForTesting
    fun generateHashesSequential(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
    ): Array<VidAndHash> {
      val vidCollection = collectVids(populationSpec)
      val vids = vidCollection.vids
      val totalVidCountInt = vidCollection.count
      if (totalVidCountInt == 0) {
        return emptyArray()
      }

      val results = Array(totalVidCountInt) { VidAndHash(0, 0L) }
      for (i in 0 until totalVidCountInt) {
        val vidInt = vids[i]
        val hash = hashFunction(vidInt.toLong(), SALT)
        results[i] = VidAndHash(vidInt, hash)
      }

      return results
    }

    /**
     * Executes [task] for each partition of `[0, totalElements)` on a limited-parallelism dispatcher.
     *
     * @param totalElements total range length.
     * @param desiredPartitions upper bound on coroutine partitions.
     * @param task partition callback that receives closed-open bounds.
     */
    @VisibleForTesting
    fun <R> applyPartitioned(
      totalElements: Int,
      desiredPartitions: Int,
      task: suspend (PartitionBounds) -> R,
    ) {
      if (totalElements == 0) {
        return
      }

      val parallelism = minOf(totalElements, desiredPartitions.coerceAtLeast(1))
      val chunkSize = (totalElements + parallelism - 1) / parallelism
      val dispatcher = Dispatchers.Default.limitedParallelism(parallelism)
      val partitions = mutableListOf<PartitionBounds>()
      // Materialize partitions ahead of launching coroutines so the dispatcher sees the exact
      // number of jobs, and to enforce closed-open bounds that are safe to reuse on the worker
      // threads.
      var start = 0
      while (start < totalElements) {
        val end = minOf(start + chunkSize, totalElements)
        partitions += PartitionBounds(start, end)
        start = end
      }
      runBlocking(dispatcher) {
        coroutineScope {
          partitions.forEach { bounds ->
            launch { task(bounds) }
          }
        }
      }
    }

    /** Closed-open bounds for a partition generated by [applyPartitioned]. */
    @VisibleForTesting
    data class PartitionBounds(val startIndex: Int, val endIndexExclusive: Int) {
      init {
        require(startIndex in 0..endIndexExclusive) { "Invalid partition bounds" }
      }

      val length: Int
        get() = endIndexExclusive - startIndex
    }

    /**
     * Collects all VIDs from the population spec into a dense array.
     *
     * @return pair of VID array and element count.
     */
    @VisibleForTesting
    fun collectVids(populationSpec: PopulationSpec): CollectedVids {
      val totalVidCount =
        populationSpec.subpopulationsList.fold(0L) { acc, subPop ->
          acc +
            subPop.vidRangesList.fold(0L) { rangeAcc, range ->
              rangeAcc + (range.endVidInclusive - range.startVid + 1)
            }
        }

      require(totalVidCount <= Int.MAX_VALUE) { "Total VID count exceeds supported maximum." }
      val totalVidCountInt = totalVidCount.toInt()

      val vids = IntArray(totalVidCountInt)
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

      return CollectedVids(vids = vids, count = totalVidCountInt)
    }

    @VisibleForTesting
    data class CollectedVids(val vids: IntArray, val count: Int)

    private fun formatDurationMillis(durationNanos: Long): String {
      val millis = durationNanos / 1_000_000.0
      return String.format(Locale.ROOT, "%.3f", millis)
    }
  }

  /** An iterator over the VidIndexMapEntries of this VidIndexMap */
  inner class Iterator : kotlin.collections.Iterator<VidIndexMapEntry> {
    private val vidIndexIterator =
      this@InMemoryVidIndexMap.indexMap.int2IntEntrySet().iterator()

    override fun hasNext(): Boolean = vidIndexIterator.hasNext()

    override fun next(): VidIndexMapEntry {
      val entry = vidIndexIterator.next()
      val k = entry.intKey
      val v = entry.intValue
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
