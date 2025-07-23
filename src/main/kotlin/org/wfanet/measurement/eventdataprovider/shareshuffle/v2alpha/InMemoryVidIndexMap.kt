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
import java.util.PriorityQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
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
    private val logger = Logger.getLogger(InMemoryVidIndexMap::class.java.name)
    
    /**
     * A salt value to ensure the output of the hash used by the VidIndexMap is different from other
     * functions that hash VIDs (e.g. the labeler). These are the first several digits of phi (the
     * golden ratio) added to the date this value was created.
     */
    private val SALT = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)
    
    private const val PROGRESS_REPORT_INTERVAL_MILLIS = 5000L // Report progress every 5 seconds
    private const val MIN_VIDS_FOR_PROGRESS_TRACKING = 10_000L // Only show progress for large populations

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
     * Create a [InMemoryVidIndexMap] given a [PopulationSpec] using parallelized processing
     *
     * This method splits all VID ranges into chunks and processes them in parallel,
     * hashing and pre-sorting VIDs, then merges the sorted chunks efficiently.
     * Progress is reported for large populations.
     *
     * @param[populationSpec] The [PopulationSpec] represented by this map
     * @param[dispatcher] The [CoroutineDispatcher] to use for parallel processing (defaults to Dispatchers.Default)
     * @param[parallelism] The number of parallel workers to use (defaults to available processors)
     * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
     */
    @JvmStatic
    suspend fun buildParallel(
      populationSpec: PopulationSpec,
      dispatcher: CoroutineDispatcher = Dispatchers.Default,
      parallelism: Int = Runtime.getRuntime().availableProcessors(),
    ): InMemoryVidIndexMap = coroutineScope {
      PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()
      
      // Collect all VID ranges from all subpopulations
      val allRanges = mutableListOf<LongRange>()
      for (subPop in populationSpec.subpopulationsList) {
        for (range in subPop.vidRangesList) {
          allRanges.add(range.startVid..range.endVidInclusive)
        }
      }
      
      val totalVids = allRanges.sumOf { it.last - it.first + 1 }
      val showProgress = totalVids >= MIN_VIDS_FOR_PROGRESS_TRACKING
      
      if (showProgress) {
        logger.info("Processing $totalVids VIDs using $parallelism workers...")
      }
      
      // Split ranges into chunks for parallel processing
      val chunks = distributeRanges(allRanges, parallelism)
      
      // Set up progress tracking
      val processedVids = AtomicLong(0)
      val progressJob = if (showProgress) {
        launch {
          val startTime = System.currentTimeMillis()
          while (processedVids.get() < totalVids) {
            kotlinx.coroutines.delay(PROGRESS_REPORT_INTERVAL_MILLIS)
            val currentProcessed = processedVids.get()
            val elapsedTime = System.currentTimeMillis() - startTime
            val progressPercent = (currentProcessed * 100.0 / totalVids)
            val vidsPerSecond = if (elapsedTime > 0) currentProcessed * 1000 / elapsedTime else 0
            
            logger.info("Progress: ${String.format("%.1f", progressPercent)}% " +
                       "($currentProcessed/$totalVids VIDs) - " +
                       "${String.format("%,d", vidsPerSecond)} VIDs/sec")
          }
        }
      } else null
      
      try {
        // Process chunks in parallel
        val sortedChunks = withContext(dispatcher) {
          chunks.map { chunk ->
            async {
              processChunkWithProgress(chunk, ::hashVidToLongWithFarmHash, processedVids)
            }
          }.awaitAll()
        }
        
        if (showProgress) {
          logger.info("Hashing and sorting completed. Starting merge phase...")
        }
        
        // Merge sorted chunks
        val mergedHashes = if (showProgress) {
          mergeSortedChunksWithProgress(sortedChunks, totalVids)
        } else {
          mergeSortedChunks(sortedChunks)
        }
        
        if (showProgress) {
          logger.info("Building index map from ${mergedHashes.size} sorted entries...")
        }
        
        // Build the index map
        val indexMap = hashMapOf<Long, Int>()
        for ((index, vidAndHash) in mergedHashes.withIndex()) {
          indexMap[vidAndHash.vid] = index
        }
        
        if (showProgress) {
          logger.info("VID index map construction completed successfully")
        }
        
        InMemoryVidIndexMap(populationSpec, indexMap)
      } finally {
        progressJob?.cancel()
      }
    }

    /**
     * Distributes VID ranges across workers to balance the load
     */
    private fun distributeRanges(ranges: List<LongRange>, workers: Int): List<List<LongRange>> {
      val totalVids = ranges.sumOf { it.last - it.first + 1 }
      val vidsPerWorker = (totalVids + workers - 1) / workers
      
      val chunks = List(workers) { mutableListOf<LongRange>() }
      var currentWorker = 0
      var currentWorkerVids = 0L
      
      for (range in ranges) {
        val rangeSize = range.last - range.first + 1
        
        // If adding this range would exceed the target size significantly, 
        // move to next worker (unless this is the last worker)
        if (currentWorkerVids > 0 && currentWorkerVids + rangeSize > vidsPerWorker && currentWorker < workers - 1) {
          currentWorker++
          currentWorkerVids = 0
        }
        
        chunks[currentWorker].add(range)
        currentWorkerVids += rangeSize
      }
      
      return chunks.filter { it.isNotEmpty() }
    }

    /**
     * Processes a chunk of VID ranges by hashing and sorting
     */
    private fun processChunk(
      ranges: List<LongRange>,
      hashFunction: (Long, ByteString) -> Long
    ): List<VidAndHash> {
      val hashes = mutableListOf<VidAndHash>()
      
      for (range in ranges) {
        for (vid in range) {
          hashes.add(VidAndHash(vid, hashFunction(vid, SALT)))
        }
      }
      
      hashes.sortWith(compareBy { it })
      return hashes
    }

    /**
     * Processes a chunk of VID ranges by hashing and sorting with progress tracking
     */
    private fun processChunkWithProgress(
      ranges: List<LongRange>,
      hashFunction: (Long, ByteString) -> Long,
      processedVids: AtomicLong
    ): List<VidAndHash> {
      val hashes = mutableListOf<VidAndHash>()
      
      for (range in ranges) {
        for (vid in range) {
          hashes.add(VidAndHash(vid, hashFunction(vid, SALT)))
          processedVids.incrementAndGet()
        }
      }
      
      hashes.sortWith(compareBy { it })
      return hashes
    }

    /**
     * Merges multiple sorted chunks into a single sorted list using a priority queue
     */
    private fun mergeSortedChunks(chunks: List<List<VidAndHash>>): List<VidAndHash> {
      if (chunks.isEmpty()) return emptyList()
      if (chunks.size == 1) return chunks[0]
      
      // Use a min-heap to efficiently merge sorted chunks
      data class ChunkElement(val vidAndHash: VidAndHash, val chunkIndex: Int, val elementIndex: Int)
      
      val heap = PriorityQueue<ChunkElement>(chunks.size) { a, b ->
        a.vidAndHash.compareTo(b.vidAndHash)
      }
      
      // Initialize heap with first element from each non-empty chunk
      val indices = IntArray(chunks.size) { 0 }
      for ((chunkIndex, chunk) in chunks.withIndex()) {
        if (chunk.isNotEmpty()) {
          heap.offer(ChunkElement(chunk[0], chunkIndex, 0))
          indices[chunkIndex] = 1
        }
      }
      
      val result = mutableListOf<VidAndHash>()
      
      while (heap.isNotEmpty()) {
        val element = heap.poll()
        result.add(element.vidAndHash)
        
        // Add next element from the same chunk if available
        val nextIndex = indices[element.chunkIndex]
        if (nextIndex < chunks[element.chunkIndex].size) {
          heap.offer(
            ChunkElement(
              chunks[element.chunkIndex][nextIndex],
              element.chunkIndex,
              nextIndex
            )
          )
          indices[element.chunkIndex] = nextIndex + 1
        }
      }
      
      return result
    }

    /**
     * Merges multiple sorted chunks into a single sorted list using a priority queue with progress tracking
     */
    private suspend fun mergeSortedChunksWithProgress(
      chunks: List<List<VidAndHash>>, 
      totalVids: Long
    ): List<VidAndHash> = coroutineScope {
      if (chunks.isEmpty()) return@coroutineScope emptyList()
      if (chunks.size == 1) return@coroutineScope chunks[0]
      
      logger.info("Merging ${chunks.size} sorted chunks containing $totalVids total VIDs...")
      
      // Use a min-heap to efficiently merge sorted chunks
      data class ChunkElement(val vidAndHash: VidAndHash, val chunkIndex: Int, val elementIndex: Int)
      
      val heap = PriorityQueue<ChunkElement>(chunks.size) { a, b ->
        a.vidAndHash.compareTo(b.vidAndHash)
      }
      
      // Initialize heap with first element from each non-empty chunk
      val indices = IntArray(chunks.size) { 0 }
      for ((chunkIndex, chunk) in chunks.withIndex()) {
        if (chunk.isNotEmpty()) {
          heap.offer(ChunkElement(chunk[0], chunkIndex, 0))
          indices[chunkIndex] = 1
        }
      }
      
      val result = mutableListOf<VidAndHash>()
      val mergedCount = AtomicLong(0)
      val startTime = System.currentTimeMillis()
      
      // Progress tracking job
      val progressJob = launch {
        while (mergedCount.get() < totalVids) {
          kotlinx.coroutines.delay(PROGRESS_REPORT_INTERVAL_MILLIS)
          val currentMerged = mergedCount.get()
          val elapsedTime = System.currentTimeMillis() - startTime
          val progressPercent = (currentMerged * 100.0 / totalVids)
          val vidsPerSecond = if (elapsedTime > 0) currentMerged * 1000 / elapsedTime else 0
          
          logger.info("Merge Progress: ${String.format("%.1f", progressPercent)}% " +
                     "($currentMerged/$totalVids VIDs) - " +
                     "${String.format("%,d", vidsPerSecond)} VIDs/sec")
        }
      }
      
      try {
        while (heap.isNotEmpty()) {
          val element = heap.poll()
          result.add(element.vidAndHash)
          mergedCount.incrementAndGet()
          
          // Add next element from the same chunk if available
          val nextIndex = indices[element.chunkIndex]
          if (nextIndex < chunks[element.chunkIndex].size) {
            heap.offer(
              ChunkElement(
                chunks[element.chunkIndex][nextIndex],
                element.chunkIndex,
                nextIndex
              )
            )
            indices[element.chunkIndex] = nextIndex + 1
          }
        }
        
        logger.info("Merge phase completed successfully")
        result
      } finally {
        progressJob.cancel()
      }
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
