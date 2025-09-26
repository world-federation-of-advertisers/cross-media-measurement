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
import java.util.concurrent.ConcurrentLinkedQueue
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

/**
 * Parallel-friendly [VidIndexMap] implementation that shards hashing and index population across
 * coroutines while storing results in a FastUtil primitive map.
 *
 * Callers should prefer this variant when the VID population is large enough to benefit from the
 * reduced garbage and parallel hashing cost. For small populations, use [InMemoryVidIndexMap]
 * instead.
 */
class ParallelInMemoryVidIndexMap
private constructor(
  override val populationSpec: PopulationSpec,
  private val indexMap: Int2IntOpenHashMap,
) : VidIndexMap {
  init {
    indexMap.defaultReturnValue(-1)
  }

  override val size
    get() = indexMap.size.toLong()

  override operator fun get(vid: Long): Int {
    require(vid < Integer.MAX_VALUE) { "VIDs must be less than ${Integer.MAX_VALUE}. Got ${vid}" }
    val key = vid.toInt()
    val value = indexMap.get(key)
    if (value == indexMap.defaultReturnValue() && !indexMap.containsKey(key)) {
      throw VidNotFoundException(vid)
    }
    return value
  }

  override operator fun iterator(): Iterator = Iterator()

  data class VidAndHash(val vid: Int, val hash: Long) : Comparable<VidAndHash> {
    override fun compareTo(other: VidAndHash): Int =
      compareValuesBy(this, other, { it.hash }, { it.vid })
  }

  companion object {
    private val SALT = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

    const val DEFAULT_PARTITION_COUNT = 8

    /**
     * Builds a parallel map using the default hash function and FastUtil backing map.
     *
     * @param populationSpec source population definition
     * @param partitionCount upper bound on concurrent partitions used during hashing and population
     */
    @JvmStatic
    fun build(
      populationSpec: PopulationSpec,
      partitionCount: Int = DEFAULT_PARTITION_COUNT,
    ): ParallelInMemoryVidIndexMap {
      return buildInternal(
        populationSpec,
        Companion::hashVidToLongWithFarmHash,
        partitionCount,
      )
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
     * @param vid the vid to hash
     * @param salt the value concatenated to the vid prior to hashing
     * @returns the hash of the vid
     */
    private fun hashVidToLongWithFarmHash(vid: Long, salt: ByteString): Long {
      val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
      return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
    }


    /**
     * Hashes VIDs and populates the index map using parallel helpers.
     *
     * @param populationSpec source population definition
     * @param hashFunction hashing routine used for every VID
     * @param partitionCount maximum partitions to schedule; useful for capping parallelism in tests
     */
    @VisibleForTesting
    fun buildInternal(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
      partitionCount: Int = DEFAULT_PARTITION_COUNT,
    ): ParallelInMemoryVidIndexMap {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
      val indexMap = Int2IntOpenHashMap().apply { defaultReturnValue(-1) }
      val hashes = generateHashes(populationSpec, hashFunction).copyOf()
      Arrays.parallelSort(hashes)
      populateIndexMap(hashes, indexMap, partitionCount = partitionCount)
      return ParallelInMemoryVidIndexMap(populationSpec, indexMap)
    }

    /**
     * Populates `indexMap` from the already sorted `hashesArray`, partitioning the work across
     * coroutines and merging the FastUtil partial maps on completion.
     */
    @VisibleForTesting
    fun populateIndexMap(
      hashesArray: Array<VidAndHash>,
      indexMap: Int2IntOpenHashMap,
      partitionCount: Int,
    ) {
      val partialMaps = ConcurrentLinkedQueue<Int2IntOpenHashMap>()
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
     * Computes VID/hash pairs using coroutine partitions.
     */
    @VisibleForTesting
    fun generateHashes(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
      partitionCount: Int? = null,
    ): Array<VidAndHash> {
      val vidCollection = InMemoryVidIndexMap.collectVids(populationSpec)
      val vids = vidCollection.vids
      val totalVidCountInt = vidCollection.count
      if (totalVidCountInt == 0) {
        return emptyArray()
      }

      val boundedPartitionCount =
        minOf(
          totalVidCountInt,
          (partitionCount ?: Runtime.getRuntime().availableProcessors()).coerceAtLeast(1),
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
     * Executes [task] for each partition of `[0, totalElements)` on a limited-parallelism dispatcher.
     */
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
    data class PartitionBounds(val startIndex: Int, val endIndexExclusive: Int) {
      val length: Int
        get() = endIndexExclusive - startIndex
    }
  }

  /** Iterator over the VID/index pairs stored in the FastUtil map. */
  inner class Iterator : kotlin.collections.Iterator<VidIndexMapEntry> {
    private val vidIndexIterator =
      this@ParallelInMemoryVidIndexMap.indexMap.int2IntEntrySet().iterator()

    override fun hasNext(): Boolean = vidIndexIterator.hasNext()

    override fun next(): VidIndexMapEntry {
      val entry = vidIndexIterator.next()
      val k = entry.intKey
      val v = entry.intValue
      return vidIndexMapEntry {
        key = k.toLong()
        value = value {
          index = v
          unitIntervalValue = v.toDouble() / this@ParallelInMemoryVidIndexMap.size
        }
      }
    }
  }
}
