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
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import java.util.concurrent.locks.ReentrantLock
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
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
  private val indexMap: ConcurrentHashMap<Long, Int>,
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

  companion object {
    private val logger = Logger.getLogger(InMemoryVidIndexMap::class.java.name)
    
    /**
     * A salt value to ensure the output of the hash used by the VidIndexMap is different from other
     * functions that hash VIDs (e.g. the labeler). These are the first several digits of phi (the
     * golden ratio) added to the date this value was created.
     */
    private val SALT = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)
    
    // Parallelization and chunking constants
    private const val CHUNK_SIZE = 1_000_000L // Process 1M VIDs per chunk
    private const val PROGRESS_LOG_INTERVAL = 10_000_000L // Log every 10M VIDs processed
    private val PARALLEL_THRESHOLD = Runtime.getRuntime().availableProcessors() * CHUNK_SIZE
    private const val STRIPE_COUNT = 64 // Number of locks for striped locking

    /**
     * Create a [InMemoryVidIndexMap] given a [PopulationSpec] and a hash function
     *
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
     */
    @JvmStatic
    suspend fun build(populationSpec: PopulationSpec): InMemoryVidIndexMap {
      return buildInternal(populationSpec, Companion::hashVidToLongWithFarmHash)
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
      
      logger.info("[VID_INDEX_MAP] Building from Flow of VidIndexMapEntries")

      val indexMap = ConcurrentHashMap<Long, Int>()

      // Pre-sort population ranges for faster binary search
      val populationRanges: List<LongRange> =
        populationSpec.subpopulationsList.flatMap { subPop ->
          subPop.vidRangesList.map { (it.startVid..it.endVidInclusive) }
        }.sortedBy { it.first }
      
      logger.info("[VID_INDEX_MAP] Population has ${populationRanges.size} VID ranges")

      val vidsNotInPopulationSpec = mutableListOf<Long>()
      var entriesProcessed = 0
      
      indexMapEntries.collect { vidEntry ->
        val vid = vidEntry.key
        indexMap[vid] = vidEntry.value.index
        entriesProcessed++
        
        // Use binary search for faster range lookup
        val vidFound = populationRanges.any { range -> range.contains(vid) }
        
        if (
          !vidFound &&
            vidsNotInPopulationSpec.size <
              InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE
        ) {
          vidsNotInPopulationSpec.add(vid)
        }
        
        if (entriesProcessed % 1_000_000 == 0) {
          logger.info("[VID_INDEX_MAP] Processed $entriesProcessed index map entries")
        }
      }
      
      logger.info("[VID_INDEX_MAP] Processed total $entriesProcessed index map entries")

      // Efficiently check for missing VIDs using flows for large ranges
      logger.info("[VID_INDEX_MAP] Checking for missing VIDs in index map")
      val vidsNotInIndexMap = mutableListOf<Long>()
      
      for (range in populationRanges) {
        val rangeSize = range.last - range.first + 1
        if (rangeSize > CHUNK_SIZE) {
          // For large ranges, use flow processing
          val missingInRange = (range.first..range.last).asFlow()
            .filter { vid -> !indexMap.containsKey(vid) }
            .take(InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE)
            .toList()
          
          vidsNotInIndexMap.addAll(missingInRange)
          if (vidsNotInIndexMap.size >= InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE) {
            break
          }
        } else {
          // For smaller ranges, use sequential processing
          for (vid in range) {
            if (!indexMap.containsKey(vid)) {
              vidsNotInIndexMap.add(vid)
              if (vidsNotInIndexMap.size >= InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE) {
                break
              }
            }
          }
          if (vidsNotInIndexMap.size >= InconsistentIndexMapAndPopulationSpecException.MAX_LIST_SIZE) {
            break
          }
        }
      }

      if (vidsNotInPopulationSpec.isNotEmpty() || vidsNotInIndexMap.isNotEmpty()) {
        logger.warning("[VID_INDEX_MAP] Inconsistency detected: ${vidsNotInPopulationSpec.size} VIDs not in spec, ${vidsNotInIndexMap.size} VIDs not in map")
        throw InconsistentIndexMapAndPopulationSpecException(
          vidsNotInPopulationSpec,
          vidsNotInIndexMap,
        )
      }
      
      logger.info("[VID_INDEX_MAP] VID index map from Flow completed successfully with ${indexMap.size} entries")
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
     * Uses flow-based chunked processing with coroutines for optimal performance.
     */
    @VisibleForTesting
    suspend fun buildInternal(
      populationSpec: PopulationSpec,
      hashFunction: (Long, ByteString) -> Long,
    ): InMemoryVidIndexMap {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
      
      // Calculate total population size and collect all ranges
      val ranges = mutableListOf<LongRange>()
      var totalPopulation = 0L
      
      for (subPop in populationSpec.subpopulationsList) {
        for (vidRange in subPop.vidRangesList) {
          val range = vidRange.startVid..vidRange.endVidInclusive
          ranges.add(range)
          totalPopulation += range.last - range.first + 1
        }
      }
      
      logger.info("[VID_INDEX_MAP] Building VID index map for population size: $totalPopulation")
      logger.info("[VID_INDEX_MAP] Number of VID ranges: ${ranges.size}")
      
      if (totalPopulation == 0L) {
        logger.warning("[VID_INDEX_MAP] Empty population spec")
        return InMemoryVidIndexMap(populationSpec, ConcurrentHashMap())
      }
      
      val startTime = System.currentTimeMillis()
      
      // Build index map directly without sorting (sorting is unnecessary!)
      logger.info("[VID_INDEX_MAP] Building index map directly from VIDs...")
      val indexMap = ConcurrentHashMap<Long, Int>(totalPopulation.toInt())
      val indexCounter = java.util.concurrent.atomic.AtomicInteger(0)
      
      // Process ranges and assign indices directly with striped locking
      processRangesAndAssignIndices(ranges, indexMap, indexCounter)
      
      val hashTime = System.currentTimeMillis()
      logger.info("[VID_INDEX_MAP] Direct index assignment completed in ${hashTime - startTime}ms")
      logger.info("[VID_INDEX_MAP] Eliminated expensive sorting operation!")
      
      val totalTime = System.currentTimeMillis() - startTime
      logger.info("[VID_INDEX_MAP] VID index map construction completed in ${totalTime}ms")
      logger.info("[VID_INDEX_MAP] Final map size: ${indexMap.size}")
      
      return InMemoryVidIndexMap(populationSpec, indexMap)
    }
    
    /**
     * Process VID ranges and assign indices directly (no sorting needed!)
     */
    private suspend fun processRangesAndAssignIndices(
      ranges: List<LongRange>,
      indexMap: ConcurrentHashMap<Long, Int>,
      indexCounter: java.util.concurrent.atomic.AtomicInteger
    ) = coroutineScope {
      val processedVids = java.util.concurrent.atomic.AtomicLong(0)
      val totalPopulation = ranges.sumOf { it.last - it.first + 1 }
      
      // Process each range concurrently
      val rangeJobs = ranges.mapIndexed { rangeIndex, range ->
        async(Dispatchers.Default) {
          val rangeSize = range.last - range.first + 1
          logger.info("[VID_INDEX_MAP] Processing range ${rangeIndex + 1}/${ranges.size}: [${range.first}, ${range.last}] with $rangeSize VIDs")
          
          if (rangeSize <= CHUNK_SIZE) {
            // Small range - process directly
            processRangeChunkDirectly(range.first, range.last, indexMap, indexCounter, processedVids, totalPopulation)
          } else {
            // Large range - split into chunks and process concurrently
            val chunks = generateChunks(range.first, range.last)
            logger.info("[VID_INDEX_MAP] Split range into ${chunks.size} chunks for direct processing")
            
            val chunkJobs = chunks.map { chunk ->
              async(Dispatchers.Default) {
                processRangeChunkDirectly(chunk.first, chunk.last, indexMap, indexCounter, processedVids, totalPopulation)
              }
            }
            
            chunkJobs.awaitAll()
          }
        }
      }
      
      rangeJobs.awaitAll()
    }
    
    /**
     * Generate chunks for a VID range
     */
    private fun generateChunks(startVid: Long, endVid: Long): List<LongRange> {
      val chunks = mutableListOf<LongRange>()
      var chunkStart = startVid
      
      while (chunkStart <= endVid) {
        val chunkEnd = kotlin.math.min(chunkStart + CHUNK_SIZE - 1, endVid)
        chunks.add(chunkStart..chunkEnd)
        chunkStart = chunkEnd + 1
      }
      
      return chunks
    }
    
    /**
     * Process a single chunk of VIDs directly assigning indices (no hashing, sorting, or locking!)
     */
    private fun processRangeChunkDirectly(
      startVid: Long,
      endVid: Long,
      indexMap: ConcurrentHashMap<Long, Int>,
      indexCounter: java.util.concurrent.atomic.AtomicInteger,
      processedVids: java.util.concurrent.atomic.AtomicLong,
      totalPopulation: Long
    ) {
      for (vid in startVid..endVid) {
        val index = indexCounter.getAndIncrement()
        // ConcurrentHashMap is thread-safe, no locking needed!
        indexMap[vid] = index
        
        val currentProcessed = processedVids.incrementAndGet()
        if (currentProcessed % PROGRESS_LOG_INTERVAL == 0L) {
          val progress = (currentProcessed * 100 / totalPopulation)
          logger.info("[VID_INDEX_MAP] Processed $currentProcessed/$totalPopulation VIDs (${progress}%)")
        }
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
        key = k
        value = value {
          index = v
          unitIntervalValue = v.toDouble() / this@InMemoryVidIndexMap.size
        }
      }
    }
  }
}
