/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.dataprovider

import com.google.protobuf.TypeRegistry
import java.util.BitSet
import java.util.concurrent.atomic.AtomicIntegerArray
import kotlin.math.ln
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.size
import org.wfanet.measurement.populationdataprovider.PopulationInfo
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

/**
 * HyperLogLog implementation for estimating cardinality (reach) from impression data Optimized for
 * integer user IDs
 */
class HyperLogLog(private val precision: Int = 12) {
  private val buckets = 1 shl precision // 2^precision
  private val registers = IntArray(buckets)
  private val alpha = calculateAlpha()

  private fun calculateAlpha(): Double {
    return when (buckets) {
      16 -> 0.673
      32 -> 0.697
      64 -> 0.709
      else -> 0.7213 / (1 + 1.079 / buckets)
    }
  }

  /** Hash function optimized for integers Uses a variation of MurmurHash3's finalizer */
  private fun hash(value: Int): Int {
    var k = value
    k = k xor (k ushr 16)
    k *= 0x85ebca6b.toInt()
    k = k xor (k ushr 13)
    k *= 0xc2b2ae35.toInt()
    k = k xor (k ushr 16)
    return k
  }

  /** Count leading zeros in a 32-bit integer */
  private fun leadingZeros(value: Int): Int {
    return if (value == 0) 32 else Integer.numberOfLeadingZeros(value) + 1
  }

  /** Add a user ID to the sketch */
  fun add(userId: Int) {
    val hashValue = hash(userId)
    val bucket = hashValue and ((1 shl precision) - 1)
    val remainingBits = hashValue ushr precision
    val leadingZerosCount = leadingZeros(remainingBits)

    registers[bucket] = maxOf(registers[bucket], leadingZerosCount)
  }

  /** Estimate the cardinality (unique count) */
  fun estimate(): Long {
    val denominator = registers.sumOf { 2.0.pow(-it) }
    if (denominator == 0.0 || !denominator.isFinite()) {
      return 0L
    }

    val rawEstimate = alpha * buckets * buckets / denominator

    if (!rawEstimate.isFinite()) {
      return 0L
    }

    // Apply small range correction
    if (rawEstimate <= 2.5 * buckets) {
      val zeros = registers.count { it == 0 }
      if (zeros != 0) {
        val correctedEstimate = buckets * ln(buckets.toDouble() / zeros)
        return if (correctedEstimate.isFinite()) correctedEstimate.roundToLong() else 0L
      }
    }

    // Apply large range correction
    if (rawEstimate <= (1.0 / 30.0) * (1L shl 32)) {
      return rawEstimate.roundToLong()
    } else {
      val largeRangeEstimate = -(1L shl 32) * ln(1 - rawEstimate / (1L shl 32))
      return if (largeRangeEstimate.isFinite()) largeRangeEstimate.roundToLong() else 0L
    }
  }

  /** Merge another HyperLogLog sketch into this one */
  fun merge(other: HyperLogLog) {
    require(other.precision == this.precision) {
      "Cannot merge HyperLogLog with different precision"
    }
    for (i in registers.indices) {
      registers[i] = maxOf(registers[i], other.registers[i])
    }
  }
}

/** Reach and Frequency calculator using HyperLogLog for reach estimation */
class ReachFrequencyCalculator(precision: Int = 12) {
  private val hll = HyperLogLog(precision)
  private var totalImpressions = 0

  // Optional exact tracking for comparison (disable for large datasets)
  private val exactUsers = mutableSetOf<Int>()
  private val userFrequency = mutableMapOf<Int, Int>()
  private var trackExact = true

  /** Process an array of impression user IDs */
  fun processImpressions(userIds: IntArray) {
    for (userId in userIds) {
      addImpression(userId)
    }
  }

  /** Process a list of impression user IDs */
  fun processImpressions(userIds: List<Int>) {
    for (userId in userIds) {
      addImpression(userId)
    }
  }

  /** Add a single impression */
  fun addImpression(userId: Int) {
    hll.add(userId)
    totalImpressions++

    if (trackExact) {
      exactUsers.add(userId)
      userFrequency[userId] = userFrequency.getOrDefault(userId, 0) + 1
    }
  }

  /** Get estimated reach (unique users) */
  fun getEstimatedReach(): Long = hll.estimate()

  /** Get exact reach (only if exact tracking is enabled) */
  fun getExactReach(): Int = if (trackExact) exactUsers.size else -1

  /** Get estimated average frequency */
  fun getEstimatedAverageFrequency(): Double {
    val reach = getEstimatedReach()
    return if (reach > 0) totalImpressions.toDouble() / reach else 0.0
  }

  /** Get exact average frequency (only if exact tracking is enabled) */
  fun getExactAverageFrequency(): Double {
    return if (trackExact && exactUsers.isNotEmpty()) {
      totalImpressions.toDouble() / exactUsers.size
    } else -1.0
  }

  /** Get frequency distribution (only if exact tracking is enabled) */
  fun getFrequencyDistribution(): Map<Int, Int>? {
    if (!trackExact) return null

    val distribution = mutableMapOf<Int, Int>()
    userFrequency.values.forEach { frequency ->
      distribution[frequency] = distribution.getOrDefault(frequency, 0) + 1
    }
    return distribution
  }

  /** Disable exact tracking to save memory for large datasets */
  fun disableExactTracking() {
    trackExact = false
    exactUsers.clear()
    userFrequency.clear()
  }

  /** Get comprehensive metrics */
  fun getMetrics(): ReachFrequencyMetrics {
    val estimatedReach = getEstimatedReach()
    val exactReach = getExactReach()
    val estimatedAvgFreq = getEstimatedAverageFrequency()
    val exactAvgFreq = getExactAverageFrequency()

    return ReachFrequencyMetrics(
        totalImpressions = totalImpressions,
        estimatedReach = estimatedReach,
        exactReach = if (exactReach >= 0) exactReach else null,
        estimatedAverageFrequency = estimatedAvgFreq,
        exactAverageFrequency = if (exactAvgFreq >= 0) exactAvgFreq else null,
        reachAccuracy = if (exactReach > 0) estimatedReach.toDouble() / exactReach else null,
        frequencyDistribution = getFrequencyDistribution())
  }
}

/** Data class to hold reach and frequency metrics */
data class ReachFrequencyMetrics(
    val totalImpressions: Int,
    val estimatedReach: Long,
    val exactReach: Int?,
    val estimatedAverageFrequency: Double,
    val exactAverageFrequency: Double?,
    val reachAccuracy: Double?,
    val frequencyDistribution: Map<Int, Int>?
) {
  override fun toString(): String {
    val sb = StringBuilder()
    sb.appendLine("=== Reach & Frequency Metrics ===")
    sb.appendLine("Total Impressions: $totalImpressions")
    sb.appendLine("Estimated Reach: $estimatedReach")
    exactReach?.let { sb.appendLine("Exact Reach: $it") }
    sb.appendLine("Estimated Avg Frequency: ${"%.2f".format(estimatedAverageFrequency)}")
    exactAverageFrequency?.let { sb.appendLine("Exact Avg Frequency: ${"%.2f".format(it)}") }
    reachAccuracy?.let { sb.appendLine("Reach Accuracy: ${"%.2f".format(it * 100)}%") }
    frequencyDistribution?.let { sb.appendLine("Frequency Distribution: $it") }
    return sb.toString()
  }
}

/** Utilities for computing Measurement results. */
object MeasurementResults {
  data class ReachAndFrequency(val reach: Int, val relativeFrequencyDistribution: Map<Int, Double>)

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  suspend fun computeReachAndFrequency(
      filteredVids: Flow<Long>,
      maxFrequency: Int,
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()

    // Drain the flow entirely into a list first
    val drainStartTime = System.currentTimeMillis()
    val vidsList = filteredVids.toList()
    val drainDuration = System.currentTimeMillis() - drainStartTime
    println("Profile: Flow draining took ${drainDuration}ms for ${vidsList.size} total VIDs")

    // Count occurrences of each VID from the drained list
    val foldStartTime = System.currentTimeMillis()
    val vidToIndex = mutableMapOf<Long, Int>()
    var nextIndex = 0

    // First pass: build VID to index mapping
    for (vid in vidsList) {
      if (!vidToIndex.containsKey(vid)) {
        vidToIndex[vid] = nextIndex++
      }
    }

    // Create atomic integer array for counts
    val countsArray = AtomicIntegerArray(vidToIndex.size)

    // Second pass: count occurrences using atomic operations in parallel
    val countingStartTime = System.currentTimeMillis()
    coroutineScope {
      val chunkSize =
          (vidsList.size / Runtime.getRuntime().availableProcessors()).coerceAtLeast(1000)
      vidsList
          .chunked(chunkSize)
          .map { chunk ->
            async(Dispatchers.Default) {
              for (vid in chunk) {
                val index = vidToIndex[vid]!!
                countsArray.incrementAndGet(index)
              }
            }
          }
          .awaitAll()
    }
    val countingDuration = System.currentTimeMillis() - countingStartTime
    println("Profile: Parallel counting took ${countingDuration}ms")

    val foldDuration = System.currentTimeMillis() - foldStartTime
    println("Profile: VID counting took ${foldDuration}ms for ${vidToIndex.size} unique VIDs")

    val reach: Int = vidToIndex.size

    // If the filtered VIDs is empty, set the distribution with all 0s up to maxFrequency.
    if (reach == 0) {
      val totalDuration = System.currentTimeMillis() - startTime
      println("Profile: computeReachAndFrequency (empty) took ${totalDuration}ms")
      return ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency histogram as a 0-based array.
    val histogramStartTime = System.currentTimeMillis()
    val frequencyArray = IntArray(maxFrequency)
    for (i in 0 until countsArray.length()) {
      val count = countsArray.get(i)
      val bucket = count.coerceAtMost(maxFrequency)
      frequencyArray[bucket - 1]++
    }
    val histogramDuration = System.currentTimeMillis() - histogramStartTime
    println("Profile: Frequency histogram creation took ${histogramDuration}ms")

    val distributionStartTime = System.currentTimeMillis()
    val frequencyDistribution: Map<Int, Double> =
        frequencyArray.withIndex().associateBy({ it.index + 1 }, { it.value.toDouble() / reach })
    val distributionDuration = System.currentTimeMillis() - distributionStartTime
    println("Profile: Frequency distribution calculation took ${distributionDuration}ms")

    val totalDuration = System.currentTimeMillis() - startTime
    println(
        "Profile: computeReachAndFrequency total took ${totalDuration}ms (reach: $reach, maxFreq: $maxFrequency)")

    return ReachAndFrequency(reach, frequencyDistribution)
  }

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  fun computeReachAndFrequency(filteredVids: Iterable<Long>, maxFrequency: Int): ReachAndFrequency {
    val startTime = System.currentTimeMillis()
    val result = runBlocking { computeReachAndFrequency(filteredVids.asFlow(), maxFrequency) }
    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeReachAndFrequency (sync wrapper) took ${totalDuration}ms")
    return result
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  suspend fun computeReach(filteredVids: Flow<Long>): Int {
    // Use a mutable set to track distinct VIDs as they flow through
    val distinctVids = mutableSetOf<Long>()

    filteredVids.collect { vid -> distinctVids.add(vid) }

    return distinctVids.size
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  fun computeReach(filteredVids: Iterable<Long>): Int {
    return filteredVids.distinct().size
  }

  /** Computes impression using the "deterministic count" methodology. */
  suspend fun computeImpression(filteredVids: Flow<Long>, maxFrequency: Int): Long {
    // Drain the flow into a list
    val vidsList = filteredVids.toList()

    // Build VID to index mapping
    val vidToIndex = mutableMapOf<Long, Int>()
    var nextIndex = 0
    for (vid in vidsList) {
      if (!vidToIndex.containsKey(vid)) {
        vidToIndex[vid] = nextIndex++
      }
    }

    // Create atomic integer array for counts
    val countsArray = AtomicIntegerArray(vidToIndex.size)

    // Count occurrences using atomic operations in parallel
    coroutineScope {
      val chunkSize =
          (vidsList.size / Runtime.getRuntime().availableProcessors()).coerceAtLeast(1000)
      vidsList
          .chunked(chunkSize)
          .map { chunk ->
            async(Dispatchers.Default) {
              for (vid in chunk) {
                val index = vidToIndex[vid]!!
                countsArray.incrementAndGet(index)
              }
            }
          }
          .awaitAll()
    }

    // Cap each count at `maxFrequency` and sum.
    var total = 0L
    for (i in 0 until countsArray.length()) {
      val count = countsArray.get(i)
      total += count.coerceAtMost(maxFrequency).toLong()
    }
    return total
  }

  /** Computes impression using the "deterministic count" methodology. */
  fun computeImpression(filteredVids: Iterable<Long>, maxFrequency: Int): Long {
    val eventsPerVid: Map<Long, Int> = filteredVids.groupingBy { it }.eachCount()
    // Cap each count at `maxFrequency`.
    return eventsPerVid.values.sumOf { count -> count.coerceAtMost(maxFrequency).toLong() }
  }

  /** Computes population using the "deterministic count" methodology. */
  fun computePopulation(
      populationInfo: PopulationInfo,
      program: Program,
      typeRegistry: TypeRegistry,
  ): Long {
    return PopulationRequisitionFulfiller.computePopulation(populationInfo, program, typeRegistry)
  }

  /** Computes reach and frequency using FrequencyVectorBuilder (taken from EdpSimulator). */
  fun computeReachAndFrequencyWithBuilder(
      populationSpec: PopulationSpec,
      measurementSpec: MeasurementSpec,
      vidIndices: Iterable<Int>,
      vidCount: Int,
      parallelThreads: Int = 1,
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()

    // Convert to IntArray and build frequency array directly
    val vidIndicesArray =
        when (vidIndices) {
          is IntArray -> vidIndices
          else -> vidIndices.toList().toIntArray()
        }
    val vidCounts = IntArray(vidCount)
    for (i in 0 until vidCount) {
      vidCounts[i] = 0;
    }

    // if one Vid spans across chunk boundary, we have a race condition
    // todo: calculate boundaries
    runBlocking {
      coroutineScope {
        val chunkSize = (vidIndicesArray.size / parallelThreads).coerceAtLeast(1000)
        vidIndicesArray
            .toList()
            .chunked(chunkSize)
          .map { chunk ->
              async(Dispatchers.Default) {
                for (vidIndex in chunk) {
                  vidCounts[vidIndex] += 1
                }
              }
            }
            .awaitAll()
      }
    }

    // Create FrequencyVectorBuilder and populate it directly
    val frequencyVectorBuilder =
        FrequencyVectorBuilder(populationSpec, measurementSpec, vidCounts, strict = false)
    val frequencyVector = frequencyVectorBuilder.build()

    val reach = frequencyVector.dataList.count { it > 0 }

    val maxFrequency =
        if (measurementSpec.hasReachAndFrequency()) {
          measurementSpec.reachAndFrequency.maximumFrequency
        } else {
          1
        }

    val frequencyHistogram = IntArray(maxFrequency)
    for (frequency in frequencyVector.dataList) {
      if (frequency > 0) {
        val bucket = frequency.coerceAtMost(maxFrequency)
        frequencyHistogram[bucket - 1]++
      }
    }

    val frequencyDistribution: Map<Int, Double> =
        frequencyHistogram
            .withIndex()
            .associateBy({ it.index + 1 }, { it.value.toDouble() / reach })

    val totalDuration = System.currentTimeMillis() - startTime
    println(
        "Profile: computeReachAndFrequencyWithBuilder took ${totalDuration}ms (reach: $reach, maxFreq: $maxFrequency, threads: $parallelThreads)")

    return ReachAndFrequency(reach, frequencyDistribution)
  }

  /**
   * Computes reach and frequency using RoaringBitmap for efficient memory usage. This
   * implementation uses bitmaps to track VID occurrences and frequency distribution.
   */
  suspend fun computeReachAndFrequencyBitmap(
      filteredVids: Flow<Long>,
      maxFrequency: Int,
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()

    // Drain the flow entirely into a list first
    val drainStartTime = System.currentTimeMillis()
    val vidsList = filteredVids.toList()
    val drainDuration = System.currentTimeMillis() - drainStartTime
    println("Profile: Flow draining took ${drainDuration}ms for ${vidsList.size} total VIDs")

    // Create bitmaps for each frequency level (1 to maxFrequency)
    val frequencyBitmaps = List(maxFrequency) { BitSet() }

    // Map to track VID counts
    val vidCounts = mutableMapOf<Long, Int>()

    // Count occurrences
    val countingStartTime = System.currentTimeMillis()
    for (vid in vidsList) {
      vidCounts[vid] = vidCounts.getOrDefault(vid, 0) + 1
    }
    val countingDuration = System.currentTimeMillis() - countingStartTime
    println("Profile: VID counting took ${countingDuration}ms for ${vidCounts.size} unique VIDs")

    val reach = vidCounts.size

    // If the filtered VIDs is empty, return empty distribution
    if (reach == 0) {
      val totalDuration = System.currentTimeMillis() - startTime
      println("Profile: computeReachAndFrequencyBitmap (empty) took ${totalDuration}ms")
      return ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency bitmaps
    val bitmapStartTime = System.currentTimeMillis()
    vidCounts.entries.forEachIndexed { index, (_, count) ->
      val frequency = count.coerceAtMost(maxFrequency)
      frequencyBitmaps[frequency - 1].set(index)
    }

    val bitmapDuration = System.currentTimeMillis() - bitmapStartTime
    println("Profile: Bitmap creation took ${bitmapDuration}ms")

    // Calculate frequency distribution
    val distributionStartTime = System.currentTimeMillis()
    val frequencyDistribution =
        frequencyBitmaps
            .mapIndexed { index, bitmap ->
              (index + 1) to (bitmap.cardinality().toDouble() / reach)
            }
            .toMap()
    val distributionDuration = System.currentTimeMillis() - distributionStartTime
    println("Profile: Frequency distribution calculation took ${distributionDuration}ms")

    val totalDuration = System.currentTimeMillis() - startTime
    println(
        "Profile: computeReachAndFrequencyBitmap total took ${totalDuration}ms (reach: $reach, maxFreq: $maxFrequency)")

    return ReachAndFrequency(reach, frequencyDistribution)
  }

  /**
   * Computes reach and frequency using RoaringBitmap for efficient memory usage. Synchronous
   * wrapper version.
   */
  fun computeReachAndFrequencyBitmap(
      filteredVids: Iterable<Long>,
      maxFrequency: Int
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()
    val result = runBlocking { computeReachAndFrequencyBitmap(filteredVids.asFlow(), maxFrequency) }
    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeReachAndFrequencyBitmap (sync wrapper) took ${totalDuration}ms")
    return result
  }

  /**
   * Computes impression using RoaringBitmap for efficient memory usage. This implementation uses
   * bitmaps to track VID occurrences.
   */
  suspend fun computeImpressionBitmap(filteredVids: Flow<Long>, maxFrequency: Int): Long {
    val startTime = System.currentTimeMillis()

    // Drain the flow into a list
    val vidsList = filteredVids.toList()

    // Count occurrences
    val vidCounts = mutableMapOf<Long, Int>()
    for (vid in vidsList) {
      vidCounts[vid] = vidCounts.getOrDefault(vid, 0) + 1
    }

    // Cap each count at maxFrequency and sum
    val total = vidCounts.values.sumOf { count -> count.coerceAtMost(maxFrequency).toLong() }

    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeImpressionBitmap took ${totalDuration}ms")

    return total
  }

  /**
   * Computes impression using RoaringBitmap for efficient memory usage. Synchronous wrapper
   * version.
   */
  fun computeImpressionBitmap(filteredVids: Iterable<Long>, maxFrequency: Int): Long {
    return runBlocking { computeImpressionBitmap(filteredVids.asFlow(), maxFrequency) }
  }

  /**
   * Computes reach using HyperLogLog for efficient cardinality estimation. Provides approximate
   * results with configurable precision.
   */
  suspend fun computeReachHyperLogLog(filteredVids: Flow<Long>, precision: Int = 12): Long {
    val startTime = System.currentTimeMillis()
    val hll = HyperLogLog(precision)

    filteredVids.collect { vid -> hll.add(vid.toInt()) }

    val result = hll.estimate()
    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeReachHyperLogLog took ${totalDuration}ms (estimated reach: $result)")

    return result
  }

  /**
   * Computes reach using HyperLogLog for efficient cardinality estimation. Synchronous wrapper
   * version.
   */
  fun computeReachHyperLogLog(filteredVids: Iterable<Long>, precision: Int = 12): Long {
    val startTime = System.currentTimeMillis()
    val hll = HyperLogLog(precision)

    filteredVids.forEach { vid -> hll.add(vid.toInt()) }

    val result = hll.estimate()
    val totalDuration = System.currentTimeMillis() - startTime
    println(
        "Profile: computeReachHyperLogLog (sync) took ${totalDuration}ms (estimated reach: $result)")

    return result
  }

  /**
   * Computes reach and frequency using HyperLogLog for reach estimation. Provides approximate reach
   * with exact frequency distribution.
   */
  suspend fun computeReachAndFrequencyHyperLogLog(
      filteredVids: Flow<Long>,
      maxFrequency: Int,
      precision: Int = 12
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()

    val calculator = ReachFrequencyCalculator(precision)

    // Drain the flow and process impressions
    val drainStartTime = System.currentTimeMillis()
    val vidsList = filteredVids.toList()
    val drainDuration = System.currentTimeMillis() - drainStartTime
    println("Profile: Flow draining took ${drainDuration}ms for ${vidsList.size} total VIDs")

    // Disable exact tracking for large datasets to save memory
    if (vidsList.size > 100000) {
      calculator.disableExactTracking()
    }

    // Process all VIDs
    val processingStartTime = System.currentTimeMillis()
    calculator.processImpressions(vidsList.map { it.toInt() })
    val processingDuration = System.currentTimeMillis() - processingStartTime
    println("Profile: HyperLogLog processing took ${processingDuration}ms")

    val estimatedReach = calculator.getEstimatedReach().toInt()

    if (estimatedReach == 0) {
      val totalDuration = System.currentTimeMillis() - startTime
      println("Profile: computeReachAndFrequencyHyperLogLog (empty) took ${totalDuration}ms")
      return ReachAndFrequency(estimatedReach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency distribution from exact tracking if available
    val frequencyDistribution =
        calculator.getFrequencyDistribution()?.let { dist ->
          // Convert to relative frequency distribution
          val total = dist.values.sum()
          (1..maxFrequency).associateWith { freq -> dist.getOrDefault(freq, 0).toDouble() / total }
        }
            ?: run {
              // If exact tracking is disabled, use estimated average frequency
              val avgFreq = calculator.getEstimatedAverageFrequency()
              // Simple distribution approximation based on average frequency
              (1..maxFrequency).associateWith { freq -> if (freq == 1) 1.0 / avgFreq else 0.0 }
            }

    val totalDuration = System.currentTimeMillis() - startTime
    println(
        "Profile: computeReachAndFrequencyHyperLogLog total took ${totalDuration}ms (estimated reach: $estimatedReach, maxFreq: $maxFrequency)")

    return ReachAndFrequency(estimatedReach, frequencyDistribution)
  }

  /**
   * Computes reach and frequency using HyperLogLog for reach estimation. Synchronous wrapper
   * version.
   */
  fun computeReachAndFrequencyHyperLogLog(
      filteredVids: Iterable<Long>,
      maxFrequency: Int,
      precision: Int = 12
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()
    val result = runBlocking {
      computeReachAndFrequencyHyperLogLog(filteredVids.asFlow(), maxFrequency, precision)
    }
    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeReachAndFrequencyHyperLogLog (sync wrapper) took ${totalDuration}ms")
    return result
  }
}
