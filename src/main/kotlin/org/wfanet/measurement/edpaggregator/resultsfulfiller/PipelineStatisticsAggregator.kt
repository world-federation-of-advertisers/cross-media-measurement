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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger

/**
 * Thread-safe aggregator for collecting pipeline processing statistics.
 * 
 * This class collects statistics from multiple parallel processing threads
 * and provides aggregated metrics for monitoring and performance analysis.
 */
class PipelineStatisticsAggregator {
  
  private val totalEventsProcessed = AtomicLong(0)
  private val totalEventsFiltered = AtomicLong(0)
  private val totalProcessingTimeMs = AtomicLong(0)
  private val maxReach = AtomicLong(0)
  private val maxFrequency = AtomicInteger(0)
  private val batchCount = AtomicLong(0)
  
  /**
   * Adds statistics from a single batch processing operation.
   * 
   * @param batchStatistics The statistics from processing a single batch
   */
  fun addBatchStatistics(batchStatistics: SinkStatistics) {
    totalEventsProcessed.addAndGet(batchStatistics.eventsProcessed)
    totalEventsFiltered.addAndGet(batchStatistics.eventsFiltered)
    totalProcessingTimeMs.addAndGet(batchStatistics.processingTimeMs)
    
    // Update maximums
    updateMaxReach(batchStatistics.reach)
    updateMaxFrequency(batchStatistics.maxFrequency)
    
    batchCount.incrementAndGet()
  }
  
  /**
   * Gets the aggregated statistics across all batches.
   * 
   * @return Aggregated SinkStatistics
   */
  fun getAggregatedStatistics(): SinkStatistics {
    return SinkStatistics(
      eventsProcessed = totalEventsProcessed.get(),
      eventsFiltered = totalEventsFiltered.get(),
      processingTimeMs = totalProcessingTimeMs.get(),
      reach = maxReach.get(),
      maxFrequency = maxFrequency.get()
    )
  }
  
  /**
   * Gets the number of batches processed.
   * 
   * @return The total number of batches processed
   */
  fun getBatchCount(): Long = batchCount.get()
  
  /**
   * Gets the average processing time per batch.
   * 
   * @return Average processing time in milliseconds, or 0 if no batches processed
   */
  fun getAverageProcessingTimePerBatch(): Double {
    val batches = batchCount.get()
    return if (batches > 0) {
      totalProcessingTimeMs.get().toDouble() / batches
    } else {
      0.0
    }
  }
  
  /**
   * Gets the overall processing rate.
   * 
   * @return Events processed per second, or 0 if no processing time recorded
   */
  fun getEventsPerSecond(): Double {
    val timeMs = totalProcessingTimeMs.get()
    return if (timeMs > 0) {
      totalEventsProcessed.get() * 1000.0 / timeMs
    } else {
      0.0
    }
  }
  
  /**
   * Resets all statistics to zero.
   */
  fun reset() {
    totalEventsProcessed.set(0)
    totalEventsFiltered.set(0)
    totalProcessingTimeMs.set(0)
    maxReach.set(0)
    maxFrequency.set(0)
    batchCount.set(0)
  }
  
  private fun updateMaxReach(reach: Long) {
    var currentMax = maxReach.get()
    while (reach > currentMax && !maxReach.compareAndSet(currentMax, reach)) {
      currentMax = maxReach.get()
    }
  }
  
  private fun updateMaxFrequency(frequency: Int) {
    var currentMax = maxFrequency.get()
    while (frequency > currentMax && !maxFrequency.compareAndSet(currentMax, frequency)) {
      currentMax = maxFrequency.get()
    }
  }
}