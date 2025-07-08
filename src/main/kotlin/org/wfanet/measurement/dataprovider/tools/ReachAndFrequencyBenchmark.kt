// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider.tools

import java.util.concurrent.atomic.AtomicIntegerArray
import kotlin.random.Random
import kotlin.system.measureTimeMillis
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.dataprovider.MeasurementResults
import picocli.CommandLine

@CommandLine.Command(
    name = "reach-and-frequency-benchmark",
    description = ["Benchmark computeReachAndFrequency with various parameters"],
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
class ReachAndFrequencyBenchmark : Runnable {
  @CommandLine.Option(
      names = ["--impression-counts"],
      description = ["Comma-separated list of impression counts to test"],
      defaultValue = "100000000")
  private var impressionCounts: String = "100000000"

  @CommandLine.Option(
      names = ["--vid-counts"],
      description = ["Comma-separated list of VID counts to test"],
      defaultValue = "36000000")
  private var vidCounts: String = "36000000"

  @CommandLine.Option(
      names = ["--parallelism-levels"],
      description =
          ["Comma-separated list of parallelism levels to test, or 'auto' to auto-detect"],
      defaultValue = "1,2,4,8")
  private var parallelismLevels: String = "1,2,4,8"

  @CommandLine.Option(
      names = ["--max-frequency"],
      description = ["Maximum frequency to track"],
      defaultValue = "10")
  private var maxFrequency: Int = 10

  @CommandLine.Option(
      names = ["--warmup-runs"],
      description = ["Number of warmup runs before measurement"],
      defaultValue = "0")
  private var warmupRuns: Int = 0

  @CommandLine.Option(
      names = ["--benchmark-runs"],
      description = ["Number of benchmark runs to average"],
      defaultValue = "1")
  private var benchmarkRuns: Int = 1

  @CommandLine.Option(names = ["--seed"], description = ["Random seed for reproducible results"])
  private var seed: Long? = null

  @CommandLine.Option(
      names = ["--output-format"],
      description = ["Output format: table, csv, or json"],
      defaultValue = "table")
  private var outputFormat: String = "table"

  @CommandLine.Option(
      names = ["--no-color"], description = ["Disable colored output"], defaultValue = "false")
  private var noColor: Boolean = false

  @CommandLine.Option(
      names = ["--algorithm"],
      description = ["Algorithm to test: atomic_array, hyperloglog, builder, or all"],
      defaultValue = "all")
  private var algorithm: String = "all"

  @CommandLine.Option(
      names = ["--validation-margin"],
      description =
          ["Margin for validating reach and frequency results (as percentage, e.g., 0.01 for 1%)"],
      defaultValue = "0.01")
  private var validationMargin: Double = 0.01

  @CommandLine.Option(
      names = ["--deterministic-data"],
      description = ["Use deterministic data generation with known reach/frequency for validation"],
      defaultValue = "true")
  private var deterministicData: Boolean = true

  @CommandLine.Option(
      names = ["--hyperloglog-precision"],
      description =
          ["HyperLogLog precision level (10-16, higher is more accurate but uses more memory)"],
      defaultValue = "12")
  private var hyperloglogPrecision: Int = 12

  data class BenchmarkResult(
      val impressionCount: Int,
      val vidCount: Int,
      val parallelism: Int,
      val algorithm: String,
      val avgTimeMs: Double,
      val minTimeMs: Long,
      val maxTimeMs: Long,
      val throughputMps: Double,
      val reach: Int,
      val avgFrequency: Double
  )

  override fun run() = runBlocking {
    val random = seed?.let { Random(it) } ?: Random.Default
    val impressionList = impressionCounts.split(",").map { it.trim().toInt() }
    val vidList = vidCounts.split(",").map { it.trim().toInt() }

    val availableProcessors = Runtime.getRuntime().availableProcessors()

    val parallelismList =
        if (parallelismLevels.trim().lowercase() == "auto") {
          // Auto-detect optimal parallelism levels based on available processors
          val levels = mutableListOf(1)
          var level = 2
          while (level <= availableProcessors * 2) {
            levels.add(level)
            level *= 2
          }
          // Add the exact processor count if not already included
          if (!levels.contains(availableProcessors)) {
            levels.add(availableProcessors)
            levels.sort()
          }
          println("Auto-detected parallelism levels: ${levels.joinToString(", ")}")
          levels
        } else {
          parallelismLevels.split(",").map { it.trim().toInt() }
        }

    println("Starting Reach and Frequency Benchmark")
    println("=====================================")
    println("System Information:")
    println("  Available processors: $availableProcessors")
    println("  Max heap memory: ${formatMemory(Runtime.getRuntime().maxMemory())}")
    println()
    println("Parameters:")
    println("  Impression counts: ${impressionList.joinToString(", ") { formatLargeNumber(it) }}")
    println("  VID counts: ${vidList.joinToString(", ") { formatLargeNumber(it) }}")
    println("  Parallelism levels: ${parallelismList.joinToString(", ")}")
    if (parallelismList.any { it > availableProcessors }) {
      println(
          "  ⚠️  Warning: Testing parallelism levels higher than available processors ($availableProcessors)")
    }
    println("  Max frequency: $maxFrequency")
    println("  Algorithm: $algorithm")
    if (algorithm.contains("hyperloglog", ignoreCase = true)) {
      println("  HyperLogLog precision: $hyperloglogPrecision")
    }
    println("  Validation margin: ${(validationMargin * 100)}%")
    println("  Deterministic data: $deterministicData")
    println("  Warmup runs: $warmupRuns")
    println("  Benchmark runs: $benchmarkRuns")
    seed?.let { println("  Seed: $it") }
    println()

    val results = mutableListOf<BenchmarkResult>()

    for (impressions in impressionList) {
      for (vids in vidList) {
        val algorithmsToTest =
            when (algorithm.lowercase()) {
              "atomic_array" -> listOf("atomic_array")
              "hyperloglog" -> listOf("hyperloglog")
              "builder" -> listOf("builder")
              "all" -> listOf("atomic_array", "builder")
              else -> listOf("atomic_array", "hyperloglog", "builder")
            }

        for (algorithmType in algorithmsToTest) {
          for (parallelism in parallelismList) {
            println(
                "Testing: impressions=$impressions, vids=$vids, parallelism=$parallelism, algorithm=$algorithmType")

            // Generate test data
            val testData = generateTestData(impressions, vids, random)

            // Warmup runs
            repeat(warmupRuns) { runBenchmark(testData, maxFrequency, parallelism, algorithmType, vids) }

            // Benchmark runs
            val times = mutableListOf<Long>()
            var finalResult: MeasurementResults.ReachAndFrequency? = null

            repeat(benchmarkRuns) {
              val (time, result) = runBenchmark(testData, maxFrequency, parallelism, algorithmType, vids)
              times.add(time)
              finalResult = result
            }

            // Calculate statistics
            val avgTime = times.average()
            val minTime = times.minOrNull() ?: 0L
            val maxTime = times.maxOrNull() ?: 0L
            val throughput = (impressions.toDouble() / avgTime) * 1000 // millions per second

            // Calculate average frequency
            val avgFrequency =
                finalResult?.let { result ->
                  result.relativeFrequencyDistribution.entries
                      .sumOf { (freq, prop) -> freq * (prop * result.reach) }
                      .toDouble() / result.reach
                } ?: 0.0

            results.add(
                BenchmarkResult(
                    impressionCount = impressions,
                    vidCount = vids,
                    parallelism = parallelism,
                    algorithm = algorithmType,
                    avgTimeMs = avgTime,
                    minTimeMs = minTime,
                    maxTimeMs = maxTime,
                    throughputMps = throughput,
                    reach = finalResult?.reach ?: 0,
                    avgFrequency = avgFrequency))

            println("  Average time: ${String.format("%.2f", avgTime)}ms")
            println("  Throughput: ${String.format("%.2f", throughput)} M impressions/sec")

            // Validate against expected results if available
            lastGeneratedData?.let { expected ->
              validateSingleResult(finalResult!!, expected, validationMargin)
            }

            println()
          }
        }
      }
    }

    // Validate results if multiple algorithms were tested
    if (algorithm.lowercase() in listOf("both", "all")) {
      validateResults(results, validationMargin)
    }

    // Output results
    when (outputFormat.lowercase()) {
      "csv" -> outputCsv(results)
      "json" -> outputJson(results)
      else -> outputTable(results)
    }
  }

  data class GeneratedDataWithExpectedResults(
      val impressions: List<Long>,
      val expectedReach: Int,
      val expectedFrequencyDistribution:
          Map<Int, Int> // frequency -> count of VIDs with that frequency
  )

  private suspend fun generateTestData(
      impressionCount: Int,
      vidCount: Int,
      random: Random
  ): List<Long> {
    return if (deterministicData) {
      // Generate deterministic data with known reach and frequency distribution
      val result = generateDeterministicTestData(impressionCount, vidCount, maxFrequency)

      // Store expected values for validation
      lastGeneratedData = result

      result.impressions
    } else {
      // Clear expected data when using random generation
      lastGeneratedData = null

      // Use original random generation
      generateRandomTestData(impressionCount, vidCount, random)
    }
  }

  private var lastGeneratedData: GeneratedDataWithExpectedResults? = null

  private fun generateDeterministicTestData(
      impressionCount: Int,
      vidCount: Int,
      maxFreq: Int
  ): GeneratedDataWithExpectedResults {
    val vidPool = mutableListOf<Long>()
    val frequencyDistribution = mutableMapOf<Int, Int>()

    // Calculate base frequency for even distribution
    val baseFrequency = impressionCount / vidCount
    var remainingImpressions = impressionCount
    var remainingVids = vidCount

    // Assign frequencies to VIDs in a deterministic way
    var currentVid = 1L

    // First, assign base frequency to all VIDs
    while (currentVid < vidCount && remainingImpressions > 0) {
      val frequency =
          if (remainingVids > 0) {
            // Ensure we don't exceed maxFrequency
            val targetFreq = (remainingImpressions / remainingVids).coerceAtMost(maxFreq)
            targetFreq.coerceAtLeast(1)
          } else {
            1
          }

      // Add this VID with its frequency
      repeat(frequency) { vidPool.add(currentVid) }

      // Track frequency distribution
      frequencyDistribution[frequency] = (frequencyDistribution[frequency] ?: 0) + 1

      remainingImpressions -= frequency
      remainingVids--
      currentVid++
    }

    // If we have remaining impressions, distribute them to create varied frequencies
    if (remainingImpressions > 0) {
      // Add remaining impressions to early VIDs to create frequency variation
      var vidIndex = 1L
      while (remainingImpressions > 0 && vidIndex < vidCount) {
        // Count current frequency of this VID
        val currentFreq = vidPool.count { it == vidIndex }
        if (currentFreq < maxFreq) {
          vidPool.add(vidIndex)

          // Update frequency distribution
          if (currentFreq > 0) {
            frequencyDistribution[currentFreq] = frequencyDistribution[currentFreq]!! - 1
            if (frequencyDistribution[currentFreq] == 0) {
              frequencyDistribution.remove(currentFreq)
            }
          }
          frequencyDistribution[currentFreq + 1] = (frequencyDistribution[currentFreq + 1] ?: 0) + 1

          remainingImpressions--
        }
        vidIndex++
        if (vidIndex > vidCount) vidIndex = 1L
      }
    }

    // Shuffle to simulate random arrival
    vidPool.shuffle()

    return GeneratedDataWithExpectedResults(
        impressions = vidPool,
        expectedReach = vidCount,
        expectedFrequencyDistribution = frequencyDistribution)
  }

  private fun generateRandomTestData(
      impressionCount: Int,
      vidCount: Int,
      random: Random
  ): List<Long> {
    val vidPool = mutableListOf<Long>()
    val uniqueVids = (1..vidCount).map { it.toLong() }

    // Generate impressions with exponential distribution for realistic frequency patterns
    val avgFrequency = impressionCount.toDouble() / vidCount
    uniqueVids.forEach { vid ->
      val frequency = (-Math.log(random.nextDouble()) * avgFrequency).toInt().coerceAtLeast(1)
      repeat(frequency.coerceAtMost(impressionCount - vidPool.size)) { vidPool.add(vid) }
    }

    // Fill remaining impressions randomly
    while (vidPool.size < impressionCount) {
      vidPool.add(uniqueVids.random(random))
    }

    // Shuffle to simulate random arrival
    vidPool.shuffle(random)
    return vidPool
  }

  private suspend fun runBenchmark(
      data: List<Long>,
      maxFrequency: Int,
      parallelism: Int,
      algorithmType: String,
      vidCount: Int
  ): Pair<Long, MeasurementResults.ReachAndFrequency> {
    // Override system property for available processors to control parallelism
    val originalProcessors =
        System.getProperty("java.util.concurrent.ForkJoinPool.common.parallelism")
    System.setProperty(
        "java.util.concurrent.ForkJoinPool.common.parallelism", parallelism.toString())

    var result: MeasurementResults.ReachAndFrequency? = null
    val time = measureTimeMillis {
      withContext(Dispatchers.Default) {
        result =
            when (algorithmType) {
              "atomic_array" -> computeReachAndFrequencyFromList(data, maxFrequency, parallelism)
              "hyperloglog" -> computeReachAndFrequencyHyperLogLog(data, maxFrequency, parallelism)
              "builder" -> computeReachAndFrequencyWithBuilder(data, maxFrequency, parallelism, vidCount)
              else -> throw IllegalArgumentException("Unknown algorithm type: $algorithmType")
            }
      }
    }

    // Restore original property
    if (originalProcessors != null) {
      System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", originalProcessors)
    } else {
      System.clearProperty("java.util.concurrent.ForkJoinPool.common.parallelism")
    }

    return Pair(time, result!!)
  }

  private suspend fun computeReachAndFrequencyFromList(
      vidsList: List<Long>,
      maxFrequency: Int,
      parallelism: Int
  ): MeasurementResults.ReachAndFrequency {
    // Build VID to index mapping
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
    coroutineScope {
      val chunkSize = (vidsList.size / parallelism).coerceAtLeast(1000)
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

    val reach: Int = vidToIndex.size

    // If the filtered VIDs is empty, set the distribution with all 0s up to maxFrequency.
    if (reach == 0) {
      return MeasurementResults.ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency histogram as a 0-based array.
    val frequencyArray = IntArray(maxFrequency)
    for (i in 0 until countsArray.length()) {
      val count = countsArray.get(i)
      val bucket = count.coerceAtMost(maxFrequency)
      frequencyArray[bucket - 1]++
    }

    val frequencyDistribution: Map<Int, Double> =
        frequencyArray.withIndex().associateBy({ it.index + 1 }, { it.value.toDouble() / reach })

    return MeasurementResults.ReachAndFrequency(reach, frequencyDistribution)
  }

  private fun computeReachAndFrequencyHyperLogLog(
      data: List<Long>,
      maxFrequency: Int,
      parallelism: Int
  ): MeasurementResults.ReachAndFrequency {
    return MeasurementResults.computeReachAndFrequencyHyperLogLog(
        data, maxFrequency, hyperloglogPrecision)
  }

  private suspend fun computeReachAndFrequencyWithBuilder(
      data: List<Long>,
      maxFrequency: Int,
      parallelism: Int,
      vidCount: Int
  ): MeasurementResults.ReachAndFrequency {
    // Create a simple PopulationSpec and MeasurementSpec for testing
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = vidCount.toLong() - 1
        }
      }
    }

    val measurementSpec = measurementSpec {
      reachAndFrequency = reachAndFrequency { maximumFrequency = maxFrequency }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
    }

    // Convert VIDs to indices (assuming VIDs are already indices in this benchmark)
    val vidIndices = data.map { it.toInt() }

    return MeasurementResults.computeReachAndFrequencyWithBuilder(
        populationSpec, measurementSpec, vidIndices, vidCount,parallelism)
  }

  private fun outputTable(results: List<BenchmarkResult>) {
    val reset = if (noColor) "" else "\u001B[0m"
    val bold = if (noColor) "" else "\u001B[1m"
    val green = if (noColor) "" else "\u001B[32m"
    val red = if (noColor) "" else "\u001B[31m"
    val blue = if (noColor) "" else "\u001B[34m"
    val yellow = if (noColor) "" else "\u001B[33m"
    val cyan = if (noColor) "" else "\u001B[36m"

    println(
        "\n${bold}╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗${reset}")
    println(
        "${bold}║${cyan}                                          REACH AND FREQUENCY BENCHMARK RESULTS                                         ${bold}║${reset}")

    println(
        "${bold}╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣${reset}")

    // Header
    println(
        "${bold}║ " +
            String.format(
                "${blue}%-12s${reset}${bold} │ ${blue}%-12s${reset}${bold} │ ${blue}%-11s${reset}${bold} │ ${blue}%-9s${reset}${bold} │ ${blue}%-13s${reset}${bold} │ ${blue}%-13s${reset}${bold} │ ${blue}%-13s${reset}${bold} │ ${blue}%-12s${reset}${bold} │ ${blue}%-10s${reset}",
                "Impressions",
                "VIDs",
                "Parallelism",
                "Algorithm",
                "Avg Time (ms)",
                "Min Time (ms)",
                "Max Time (ms)",
                "Throughput",
                "Avg Freq") +
            "${bold} ║${reset}")
    println(
        "${bold}║ " +
            String.format(
                "%-12s │ %-12s │ %-11s │ %-9s │ %-13s │ %-13s │ %-13s │ ${yellow}%-12s${reset}${bold} │ %-10s",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "(M imp/sec)",
                "") +
            " ║${reset}")
    println(
        "${bold}╟──────────────┼──────────────┼─────────────┼───────────┼───────────────┼───────────────┼───────────────┼──────────────┼────────────╢${reset}")

    // Group results by impression/vid combination for better readability
    val groupedResults = results.groupBy { it.impressionCount to it.vidCount }

    groupedResults.entries.forEachIndexed { groupIndex, (key, groupResults) ->
      groupResults.forEachIndexed { index, result ->
        val impressionsStr = if (index == 0) formatLargeNumber(result.impressionCount) else ""
        val vidsStr = if (index == 0) formatLargeNumber(result.vidCount) else ""

        // Color code throughput - green for high, yellow for medium, red for low
        val throughputColor =
            when {
              result.throughputMps >= 100 -> green
              result.throughputMps >= 50 -> yellow
              else -> red
            }

        println(
            "║ " +
                String.format(
                    "${cyan}%-12s${reset} │ ${cyan}%-12s${reset} │ %-11d │ %-9s │ %13.2f │ %13d │ %13d │ ${throughputColor}%12.2f${reset} │ %10.2f",
                    impressionsStr,
                    vidsStr,
                    result.parallelism,
                    result.algorithm,
                    result.avgTimeMs,
                    result.minTimeMs,
                    result.maxTimeMs,
                    result.throughputMps,
                    result.avgFrequency) +
                " ║")
      }

      // Add separator between groups except for the last one
      if (groupIndex < groupedResults.size - 1) {
        println(
            "╟──────────────┼──────────────┼─────────────┼───────────┼───────────────┼───────────────┼───────────────┼──────────────┼────────────╢")
      }
    }

    println(
        "${bold}╚══════════════╧══════════════╧═════════════╧═══════════╧═══════════════╧═══════════════╧═══════════════╧══════════════╧════════════╝${reset}")

    // Summary statistics
    println(
        "\n${bold}╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗${reset}")
    println(
        "${bold}║${cyan}                                                    SUMMARY STATISTICS                                                   ${bold}║${reset}")
    println(
        "${bold}╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣${reset}")

    val bestThroughput = results.maxByOrNull { it.throughputMps }
    val worstThroughput = results.minByOrNull { it.throughputMps }
    val avgThroughput = results.map { it.throughputMps }.average()

    if (bestThroughput != null && worstThroughput != null) {
      println(
          "${bold}║${reset} ${green}Best Performance:${reset}  ${String.format("%-97s",
        "${formatLargeNumber(bestThroughput.impressionCount)} impressions, ${formatLargeNumber(bestThroughput.vidCount)} VIDs, " +
        "${bestThroughput.parallelism} threads, ${bestThroughput.algorithm} → ${green}${String.format("%.2f", bestThroughput.throughputMps)} M imp/sec${reset}"
      )} ${bold}║${reset}")
      println(
          "${bold}║${reset} ${red}Worst Performance:${reset} ${String.format("%-97s",
        "${formatLargeNumber(worstThroughput.impressionCount)} impressions, ${formatLargeNumber(worstThroughput.vidCount)} VIDs, " +
        "${worstThroughput.parallelism} threads, ${worstThroughput.algorithm} → ${red}${String.format("%.2f", worstThroughput.throughputMps)} M imp/sec${reset}"
      )} ${bold}║${reset}")
      println(
          "${bold}║${reset} ${blue}Average Throughput:${reset} ${String.format("%-96s", "${blue}${String.format("%.2f", avgThroughput)} M impressions/sec${reset}")} ${bold}║${reset}")
    }

    println(
        "${bold}╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝${reset}")
  }

  private fun formatLargeNumber(num: Int): String {
    return when {
      num >= 1_000_000_000 -> String.format("%.1fB", num / 1_000_000_000.0)
      num >= 1_000_000 -> String.format("%.1fM", num / 1_000_000.0)
      num >= 1_000 -> String.format("%.1fK", num / 1_000.0)
      else -> num.toString()
    }
  }

  private fun formatMemory(bytes: Long): String {
    return when {
      bytes >= 1_073_741_824 -> String.format("%.1f GB", bytes / 1_073_741_824.0)
      bytes >= 1_048_576 -> String.format("%.1f MB", bytes / 1_048_576.0)
      bytes >= 1_024 -> String.format("%.1f KB", bytes / 1_024.0)
      else -> "$bytes bytes"
    }
  }

  private fun validateSingleResult(
      actual: MeasurementResults.ReachAndFrequency,
      expected: GeneratedDataWithExpectedResults,
      margin: Double
  ) {
    val reset = if (noColor) "" else "\u001B[0m"
    val green = if (noColor) "" else "\u001B[32m"
    val red = if (noColor) "" else "\u001B[31m"
    val yellow = if (noColor) "" else "\u001B[33m"

    // Validate reach
    val reachDiff = Math.abs(actual.reach - expected.expectedReach)
    val reachMargin = expected.expectedReach * margin
    val reachPassed = reachDiff <= reachMargin

    val reachStatus = if (reachPassed) "${green}✓${reset}" else "${red}✗${reset}"
    println(
        "  Reach validation: expected=${expected.expectedReach}, actual=${actual.reach}, diff=$reachDiff $reachStatus")

    // Validate frequency distribution
    val expectedFreqDist = expected.expectedFrequencyDistribution
    var freqValidationPassed = true

    println("  Frequency distribution validation:")
    expectedFreqDist.forEach { (frequency, expectedCount) ->
      val actualProportion = actual.relativeFrequencyDistribution[frequency] ?: 0.0
      val actualCount = (actualProportion * actual.reach).toInt()
      val countDiff = Math.abs(actualCount - expectedCount)
      val countMargin = Math.max(1, (expectedCount * margin).toInt())
      val countPassed = countDiff <= countMargin

      if (!countPassed) freqValidationPassed = false

      val status = if (countPassed) "${green}✓${reset}" else "${red}✗${reset}"
      println(
          "    Frequency $frequency: expected=$expectedCount VIDs, actual=$actualCount VIDs, diff=$countDiff $status")
    }

    // Overall validation status
    val overallPassed = reachPassed && freqValidationPassed
    val overallStatus =
        if (overallPassed) {
          "${green}✓ All validations passed${reset}"
        } else {
          "${red}✗ Some validations failed${reset}"
        }
    println("  Overall validation: $overallStatus")
  }

  private fun validateResults(results: List<BenchmarkResult>, margin: Double) {
    val reset = if (noColor) "" else "\u001B[0m"
    val bold = if (noColor) "" else "\u001B[1m"
    val green = if (noColor) "" else "\u001B[32m"
    val red = if (noColor) "" else "\u001B[31m"
    val yellow = if (noColor) "" else "\u001B[33m"

    println(
        "\n${bold}╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗${reset}")
    println(
        "${bold}║${yellow}                                                    VALIDATION RESULTS                                                   ${bold}║${reset}")
    println(
        "${bold}╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣${reset}")

    // Group results by impression/vid/parallelism combination
    val groupedResults = results.groupBy { Triple(it.impressionCount, it.vidCount, it.parallelism) }

    var allPassed = true
    var testCount = 0
    var passCount = 0

    for ((key, algorithmResults) in groupedResults) {
      val (impressions, vids, parallelism) = key

      // Find results for each algorithm
      val parallelResult = algorithmResults.find { it.algorithm == "parallel" }
      val hyperloglogResult = algorithmResults.find { it.algorithm == "hyperloglog" }
      val builderResult = algorithmResults.find { it.algorithm == "builder" }

      // Use parallel as the baseline for comparisons
      if (parallelResult != null) {
        // Compare with HyperLogLog
        if (hyperloglogResult != null) {
          testCount++

          // Validate reach - HyperLogLog provides approximate results, so use larger margin
          val reachDiff = Math.abs(parallelResult.reach - hyperloglogResult.reach)
          val reachMargin =
              Math.max(parallelResult.reach, hyperloglogResult.reach) *
                  (margin * 10) // 10x margin for HyperLogLog approximation
          val reachPassed = reachDiff <= reachMargin

          // Validate average frequency - also use larger margin for HyperLogLog
          val freqDiff = Math.abs(parallelResult.avgFrequency - hyperloglogResult.avgFrequency)
          val freqMargin =
              Math.max(parallelResult.avgFrequency, hyperloglogResult.avgFrequency) *
                  (margin * 5) // 5x margin for frequency
          val freqPassed = freqDiff <= freqMargin

          val testPassed = reachPassed && freqPassed
          if (testPassed) passCount++ else allPassed = false

          val statusIcon = if (testPassed) "${green}✓${reset}" else "${red}✗${reset}"
          val statusText = if (testPassed) "${green}PASSED${reset}" else "${red}FAILED${reset}"

          println(
              "${bold}║${reset} ${formatLargeNumber(impressions)} imp, ${formatLargeNumber(vids)} VIDs, ${parallelism} threads (parallel vs hyperloglog): $statusText")
          println(
              "${bold}║${reset}   Reach: parallel=${parallelResult.reach}, hyperloglog=${hyperloglogResult.reach} (diff=$reachDiff, margin=${"%.2f".format(reachMargin)}) $statusIcon")
          println(
              "${bold}║${reset}   Avg Freq: parallel=${"%.4f".format(parallelResult.avgFrequency)}, hyperloglog=${"%.4f".format(hyperloglogResult.avgFrequency)} (diff=${"%.4f".format(freqDiff)}, margin=${"%.4f".format(freqMargin)}) $statusIcon")
        }

        // Compare with Builder
        if (builderResult != null) {
          testCount++

          // Builder should be exact, so use normal margin
          val reachDiff = Math.abs(parallelResult.reach - builderResult.reach)
          val reachMargin = Math.max(parallelResult.reach, builderResult.reach) * margin
          val reachPassed = reachDiff <= reachMargin

          val freqDiff = Math.abs(parallelResult.avgFrequency - builderResult.avgFrequency)
          val freqMargin =
              Math.max(parallelResult.avgFrequency, builderResult.avgFrequency) * margin
          val freqPassed = freqDiff <= freqMargin

          val testPassed = reachPassed && freqPassed
          if (testPassed) passCount++ else allPassed = false

          val statusIcon = if (testPassed) "${green}✓${reset}" else "${red}✗${reset}"
          val statusText = if (testPassed) "${green}PASSED${reset}" else "${red}FAILED${reset}"

          println(
              "${bold}║${reset} ${formatLargeNumber(impressions)} imp, ${formatLargeNumber(vids)} VIDs, ${parallelism} threads (parallel vs builder): $statusText")
          println(
              "${bold}║${reset}   Reach: parallel=${parallelResult.reach}, builder=${builderResult.reach} (diff=$reachDiff, margin=${"%.2f".format(reachMargin)}) $statusIcon")
          println(
              "${bold}║${reset}   Avg Freq: parallel=${"%.4f".format(parallelResult.avgFrequency)}, builder=${"%.4f".format(builderResult.avgFrequency)} (diff=${"%.4f".format(freqDiff)}, margin=${"%.4f".format(freqMargin)}) $statusIcon")
        }
      }
    }

    println(
        "${bold}╟────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╢${reset}")

    val summary =
        if (allPassed) {
          "${green}All validation tests passed! ($passCount/$testCount)${reset}"
        } else {
          "${red}Some validation tests failed! ($passCount/$testCount passed)${reset}"
        }

    println("${bold}║${reset} ${bold}Summary:${reset} $summary")
    println("${bold}║${reset} ${bold}Validation margin:${reset} ${(margin * 100)}%")

    println(
        "${bold}╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝${reset}")
  }

  private fun outputCsv(results: List<BenchmarkResult>) {
    println(
        "impressions,vids,parallelism,algorithm,avg_time_ms,min_time_ms,max_time_ms,throughput_mps,reach,avg_frequency")
    results.forEach { result ->
      println(
          "${result.impressionCount},${result.vidCount},${result.parallelism},${result.algorithm}," +
              "${result.avgTimeMs},${result.minTimeMs},${result.maxTimeMs}," +
              "${result.throughputMps},${result.reach},${result.avgFrequency}")
    }
  }

  private fun outputJson(results: List<BenchmarkResult>) {
    println("[")
    results.forEachIndexed { index, result ->
      println("  {")
      println("    \"impressions\": ${result.impressionCount},")
      println("    \"vids\": ${result.vidCount},")
      println("    \"parallelism\": ${result.parallelism},")
      println("    \"algorithm\": \"${result.algorithm}\",")
      println("    \"avg_time_ms\": ${result.avgTimeMs},")
      println("    \"min_time_ms\": ${result.minTimeMs},")
      println("    \"max_time_ms\": ${result.maxTimeMs},")
      println("    \"throughput_mps\": ${result.throughputMps},")
      println("    \"reach\": ${result.reach},")
      println("    \"avg_frequency\": ${result.avgFrequency}")
      print("  }")
      if (index < results.size - 1) println(",") else println()
    }
    println("]")
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(ReachAndFrequencyBenchmark(), args)
  }
}

fun main(args: Array<String>) {
  ReachAndFrequencyBenchmark.main(args)
}
