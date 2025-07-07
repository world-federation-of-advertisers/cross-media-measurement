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

import kotlin.random.Random
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.dataprovider.MeasurementResults
import picocli.CommandLine

@CommandLine.Command(
  name = "reach-and-frequency-random-flow",
  description = ["Generate random VIDs flow and compute reach and frequency"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class ReachAndFrequencyRandomFlow : Runnable {
  @CommandLine.Option(
    names = ["--vid-count", "-v"],
    description = ["Number of VIDs to generate"],
    defaultValue = "1000"
  )
  private var vidCount: Int = 1000

  @CommandLine.Option(
    names = ["--impression-count", "-i"],
    description = ["Total number of impressions to generate"],
    defaultValue = "5000"
  )
  private var impressionCount: Int = 5000

  @CommandLine.Option(
    names = ["--max-frequency", "-f"],
    description = ["Maximum frequency to track"],
    defaultValue = "10"
  )
  private var maxFrequency: Int = 10

  @CommandLine.Option(
    names = ["--max-vid-value", "-m"],
    description = ["Maximum VID value to generate"],
    defaultValue = "1000000"
  )
  private var maxVidValue: Long = 1_000_000L

  @CommandLine.Option(
    names = ["--seed", "-s"],
    description = ["Random seed for reproducible results"]
  )
  private var seed: Long? = null

  override fun run() = runBlocking {
    val random = seed?.let { Random(it) } ?: Random.Default

    println("Generating random VIDs flow...")
    println("Parameters:")
    println("  VID count: $vidCount")
    println("  Impression count: $impressionCount")
    println("  Max frequency: $maxFrequency")
    println("  Max VID value: $maxVidValue")
    seed?.let { println("  Seed: $it") }
    println()

    // Generate a flow of random VIDs
    val startTime = System.currentTimeMillis()
    val vidsFlow: Flow<Long> = generateRandomVidsFlow(random)

    // Compute reach and frequency
    println("Computing reach and frequency...")
    val result = MeasurementResults.computeReachAndFrequency(vidsFlow, maxFrequency)
    val computeTime = System.currentTimeMillis() - startTime

    // Display results
    println("\nResults:")
    println("  Reach: ${result.reach}")
    println("\nFrequency Distribution:")
    result.relativeFrequencyDistribution.forEach { (frequency, proportion) ->
      val percentage = proportion * 100
      val bar = "█".repeat((percentage / 2).toInt())
      println("  Frequency $frequency: ${String.format("%6.2f%%", percentage)} $bar")
    }

    println("\nComputation time: ${computeTime}ms")

    // Additional statistics
    val totalImpressions = result.relativeFrequencyDistribution.entries.sumOf { (freq, prop) ->
      freq * (prop * result.reach)
    }.toInt()
    val avgFrequency = if (result.reach > 0) totalImpressions.toDouble() / result.reach else 0.0

    println("\nAdditional Statistics:")
    println("  Total impressions (estimated): $totalImpressions")
    println("  Average frequency: ${String.format("%.2f", avgFrequency)}")
  }

  private fun generateRandomVidsFlow(random: Random): Flow<Long> = flow {
    // First, generate a pool of VIDs with their frequencies
    val vidPool = mutableListOf<Long>()
    val usedVids = mutableSetOf<Long>()

    // Generate unique VIDs
    while (usedVids.size < vidCount) {
      val vid = random.nextLong(1, maxVidValue + 1)
      usedVids.add(vid)
    }

    // Create a weighted distribution of frequencies
    val vidList = usedVids.toList()
    var remainingImpressions = impressionCount

    // Assign frequencies to VIDs with some distribution
    vidList.forEach { vid ->
      val remainingVids = vidCount - vidPool.size
      if (remainingVids > 0) {
        // Use a simple distribution for frequency patterns
        val avgFrequencyRemaining = remainingImpressions.toDouble() / remainingVids
        val frequency = (random.nextDouble() * avgFrequencyRemaining * 2).toInt().coerceIn(1, remainingImpressions)
        
        repeat(frequency) {
          vidPool.add(vid)
        }
        remainingImpressions -= frequency
      }
    }

    // Add any remaining impressions randomly
    while (remainingImpressions > 0) {
      vidPool.add(vidList.random(random))
      remainingImpressions--
    }

    // Shuffle the pool to simulate random arrival of impressions
    vidPool.shuffle(random)

    // Emit all VIDs from the pool
    vidPool.forEach { vid ->
      emit(vid)
    }
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ReachAndFrequencyRandomFlow(), args)
  }
}

fun main(args: Array<String>) {
  ReachAndFrequencyRandomFlow.main(args)
}