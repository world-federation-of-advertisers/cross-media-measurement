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

import java.util.logging.Logger

/**
 * Aggregates and displays statistics from pipeline execution.
 * 
 * This class is responsible for:
 * - Calculating aggregated metrics across filters
 * - Formatting and displaying results
 * - Providing insights into reach and frequency patterns
 */
class PipelineStatisticsAggregator {
  
  companion object {
    private val logger = Logger.getLogger(PipelineStatisticsAggregator::class.java.name)
    
    private val DEMOGRAPHIC_FILTERS = listOf(
      "male_18_34", "male_35_54", "male_55_plus",
      "female_18_34", "female_35_54", "female_55_plus"
    )
    
    private val DEMOGRAPHIC_NAMES = mapOf(
      "male_18_34" to "Male 18-34",
      "male_35_54" to "Male 35-54",
      "male_55_plus" to "Male 55+",
      "female_18_34" to "Female 18-34",
      "female_35_54" to "Female 35-54",
      "female_55_plus" to "Female 55+"
    )
    
    private const val SEPARATOR_LENGTH = 60
    private const val SUBSECTION_SEPARATOR_LENGTH = 50
  }
  
  /**
   * Displays individual statistics for each filter.
   */
  fun displayFilterStatistics(statistics: Map<String, SinkStatistics>) {
    println("\nFilter Statistics:")
    println("=".repeat(SEPARATOR_LENGTH))
    
    statistics.forEach { (filterId, stats) ->
      displaySingleFilterStats(filterId, stats)
    }
  }
  
  /**
   * Displays aggregated metrics across different dimensions.
   */
  fun displayAggregatedMetrics(statistics: Map<String, SinkStatistics>, totalWeeks: Int) {
    println("\n" + "=".repeat(SEPARATOR_LENGTH))
    println("AGGREGATED REACH & FREQUENCY ANALYSIS")
    println("=".repeat(SEPARATOR_LENGTH))
    
    displayDemographicMetrics(statistics)
    displayTimeRangeMetrics(statistics, totalWeeks)
    displayDetailedBreakdown(statistics, totalWeeks)
    displayOverallTotals(statistics)
    
    println("\n" + "=".repeat(SEPARATOR_LENGTH))
  }
  
  /**
   * Displays execution summary with performance metrics.
   */
  fun displayExecutionSummary(
    totalDuration: Long,
    totalEvents: Long,
    pipelineType: String
  ) {
    println("\nPipeline Execution Summary:")
    println("=".repeat(SEPARATOR_LENGTH))
    println("Pipeline type: $pipelineType")
    println("Total duration: ${totalDuration}ms")
    println("Total events processed: $totalEvents")
    println("Average throughput: ${calculateThroughput(totalEvents, totalDuration)} events/sec")
    
    displayMemoryUsage()
  }
  
  private fun displaySingleFilterStats(filterId: String, stats: SinkStatistics) {
    println("Filter: $filterId")
    println("  Description: ${stats.description}")
    println("  Processed events: ${stats.processedEvents}")
    println("  Matched events: ${stats.matchedEvents} (${formatPercentage(stats.matchRate)}%)")
    println("  Errors: ${stats.errorCount} (${formatPercentage(stats.errorRate)}%)")
    println("  Reach: ${stats.reach}")
    println("  Total frequency: ${stats.totalFrequency}")
    println("  Average frequency: ${formatDouble(stats.averageFrequency)}")
    println()
  }
  
  private fun displayDemographicMetrics(statistics: Map<String, SinkStatistics>) {
    println("\n1. DEMOGRAPHIC REACH & FREQUENCY (All Time Ranges Combined)")
    println("-".repeat(SUBSECTION_SEPARATOR_LENGTH))
    
    DEMOGRAPHIC_FILTERS.forEach { demographic ->
      val demographicStats = statistics.filter { it.key.startsWith(demographic) }.values
      
      if (demographicStats.isNotEmpty()) {
        val metrics = calculateAggregatedMetrics(demographicStats)
        
        println("${DEMOGRAPHIC_NAMES[demographic]?.padEnd(15)}: " +
                "Reach = ${metrics.totalReach.toString().padStart(8)}, " +
                "Frequency = ${metrics.totalFrequency.toString().padStart(8)}, " +
                "Avg = ${formatDouble(metrics.averageFrequency, 6)}")
      }
    }
  }
  
  private fun displayTimeRangeMetrics(statistics: Map<String, SinkStatistics>, totalWeeks: Int) {
    println("\n2. TIME RANGE REACH & FREQUENCY (All Demographics Combined)")
    println("-".repeat(SUBSECTION_SEPARATOR_LENGTH))
    
    for (week in 1..totalWeeks) {
      val weekStats = statistics.filter { it.key.endsWith("week$week") }.values
      
      if (weekStats.isNotEmpty()) {
        val metrics = calculateAggregatedMetrics(weekStats)
        
        println("Week $week (cumulative)".padEnd(18) + ": " +
                "Reach = ${metrics.totalReach.toString().padStart(8)}, " +
                "Frequency = ${metrics.totalFrequency.toString().padStart(8)}, " +
                "Avg = ${formatDouble(metrics.averageFrequency, 6)}")
      }
    }
  }
  
  private fun displayDetailedBreakdown(statistics: Map<String, SinkStatistics>, totalWeeks: Int) {
    println("\n3. UNIQUE REACH BY DEMOGRAPHIC & TIME RANGE")
    println("-".repeat(SUBSECTION_SEPARATOR_LENGTH))
    println("(Note: These are individual filter reaches, not deduplicated unique reach)")
    
    DEMOGRAPHIC_FILTERS.forEach { demographic ->
      val demographicName = DEMOGRAPHIC_NAMES[demographic] ?: demographic
      println("\n$demographicName:")
      
      for (week in 1..totalWeeks) {
        val filterId = "${demographic}_week$week"
        val stats = statistics[filterId]
        
        if (stats != null) {
          println("  Week $week: " +
                  "Reach = ${stats.reach.toString().padStart(6)}, " +
                  "Frequency = ${stats.totalFrequency.toString().padStart(6)}, " +
                  "Avg = ${formatDouble(stats.averageFrequency, 5)}")
        }
      }
    }
  }
  
  private fun displayOverallTotals(statistics: Map<String, SinkStatistics>) {
    println("\n4. OVERALL TOTALS")
    println("-".repeat(SUBSECTION_SEPARATOR_LENGTH))
    
    val overallMetrics = calculateAggregatedMetrics(statistics.values)
    
    println("Total Filters: ${statistics.size}")
    println("Total Reach (sum): ${overallMetrics.totalReach}")
    println("Total Frequency (sum): ${overallMetrics.totalFrequency}")
    println("Average Frequency: ${formatDouble(overallMetrics.averageFrequency)}")
  }
  
  private fun displayMemoryUsage() {
    val runtime = Runtime.getRuntime()
    val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
    val maxMemory = runtime.maxMemory() / 1024 / 1024
    
    println("\nMemory Usage:")
    println("  Used: ${usedMemory} MB")
    println("  Max available: ${maxMemory} MB")
  }
  
  private fun calculateAggregatedMetrics(stats: Collection<SinkStatistics>): AggregatedMetrics {
    val totalReach = stats.sumOf { it.reach }
    val totalFrequency = stats.sumOf { it.totalFrequency }
    val averageFrequency = if (totalReach > 0) {
      totalFrequency.toDouble() / totalReach
    } else {
      0.0
    }
    
    return AggregatedMetrics(totalReach, totalFrequency, averageFrequency)
  }
  
  private fun calculateThroughput(events: Long, durationMs: Long): Long {
    return if (durationMs > 0) events * 1000 / durationMs else 0
  }
  
  private fun formatPercentage(value: Double): String {
    return String.format("%.2f", value)
  }
  
  private fun formatDouble(value: Double, width: Int = 0): String {
    val formatted = String.format("%.2f", value)
    return if (width > 0) formatted.padStart(width) else formatted
  }
  
  private data class AggregatedMetrics(
    val totalReach: Long,
    val totalFrequency: Long,
    val averageFrequency: Double
  )
}