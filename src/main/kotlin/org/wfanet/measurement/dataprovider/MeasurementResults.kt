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
import java.util.concurrent.atomic.AtomicIntegerArray
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.projectnessie.cel.Program
import org.wfanet.measurement.populationdataprovider.PopulationInfo
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.FrequencyVectorBuilder
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec

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
      val chunkSize = (vidsList.size / Runtime.getRuntime().availableProcessors()).coerceAtLeast(1000)
      vidsList.chunked(chunkSize).map { chunk ->
        async(Dispatchers.Default) {
          for (vid in chunk) {
            val index = vidToIndex[vid]!!
            countsArray.incrementAndGet(index)
          }
        }
      }.awaitAll()
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
    println("Profile: computeReachAndFrequency total took ${totalDuration}ms (reach: $reach, maxFreq: $maxFrequency)")
    
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
      val chunkSize = (vidsList.size / Runtime.getRuntime().availableProcessors()).coerceAtLeast(1000)
      vidsList.chunked(chunkSize).map { chunk ->
        async(Dispatchers.Default) {
          for (vid in chunk) {
            val index = vidToIndex[vid]!!
            countsArray.incrementAndGet(index)
          }
        }
      }.awaitAll()
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

  /** Computes reach and frequency using FrequencyVectorBuilder (EdpSimulator algorithm). */
  fun computeReachAndFrequencyWithBuilder(
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
    vidIndices: Iterable<Int>,
    parallelThreads: Int = 1,
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()
    
    val frequencyVectorBuilder = FrequencyVectorBuilder(populationSpec, measurementSpec, strict = false)
    
    if (parallelThreads <= 1) {
      // Sequential processing
      vidIndices.forEach { vidIndex ->
        frequencyVectorBuilder.increment(vidIndex)
      }
    } else {
      // Parallel processing
      val vidIndicesList = vidIndices.toList()
      runBlocking {
        coroutineScope {
          val chunkSize = (vidIndicesList.size / parallelThreads).coerceAtLeast(1)
          vidIndicesList.chunked(chunkSize).map { chunk ->
            async(Dispatchers.Default) {
              chunk.forEach { vidIndex ->
                frequencyVectorBuilder.increment(vidIndex)
              }
            }
          }.awaitAll()
        }
      }
    }
    
    val frequencyVector = frequencyVectorBuilder.build()
    
    val reach = frequencyVector.dataList.count { it > 0 }
    
    if (reach == 0) {
      val maxFrequency = if (measurementSpec.hasReachAndFrequency()) {
        measurementSpec.reachAndFrequency.maximumFrequency
      } else {
        1
      }
      val totalDuration = System.currentTimeMillis() - startTime
      println("Profile: computeReachAndFrequencyWithBuilder (empty) took ${totalDuration}ms")
      return ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }
    
    val maxFrequency = if (measurementSpec.hasReachAndFrequency()) {
      measurementSpec.reachAndFrequency.maximumFrequency
    } else {
      1
    }
    
    val frequencyArray = IntArray(maxFrequency)
    for (frequency in frequencyVector.dataList) {
      if (frequency > 0) {
        val bucket = frequency.coerceAtMost(maxFrequency)
        frequencyArray[bucket - 1]++
      }
    }
    
    val frequencyDistribution: Map<Int, Double> =
      frequencyArray.withIndex().associateBy({ it.index + 1 }, { it.value.toDouble() / reach })
    
    val totalDuration = System.currentTimeMillis() - startTime
    println("Profile: computeReachAndFrequencyWithBuilder took ${totalDuration}ms (reach: $reach, maxFreq: $maxFrequency)")
    
    return ReachAndFrequency(reach, frequencyDistribution)
  }
}
