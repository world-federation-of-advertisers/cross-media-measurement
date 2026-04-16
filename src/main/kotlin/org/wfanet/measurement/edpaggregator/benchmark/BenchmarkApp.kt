/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.protobuf.TextFormat
import java.io.File
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.core.labeler.Labeler
import picocli.CommandLine

@CommandLine.Command(
  name = "benchmark_app",
  description = ["Benchmark for memoized VID assignment pipeline"],
)
class BenchmarkApp : Runnable {
  @CommandLine.Mixin
  private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--model-path"],
    description = ["Path to the compiled model textproto file"],
    required = true,
  )
  private lateinit var modelPath: String

  @CommandLine.Option(
    names = ["--scale-factor"],
    description = ["Scale factor for data volume (1.0 = 30M reach, 0.001 = 30K for dev)"],
    defaultValue = "0.001",
  )
  private var scaleFactor: Double = 0.001

  @CommandLine.Option(
    names = ["--days"],
    description = ["Number of days to simulate"],
    defaultValue = "7",
  )
  private var days: Int = 7

  @CommandLine.Option(
    names = ["--db-batch-size"],
    description = ["Number of rows per DB batch operation"],
    defaultValue = "10000",
  )
  private var dbBatchSize: Int = 10000

  @CommandLine.Option(
    names = ["--data-provider"],
    description = ["DataProvider resource ID for rank table scoping"],
    defaultValue = "dataProviders/benchmark-dp",
  )
  private var dataProvider: String = "dataProviders/benchmark-dp"

  @CommandLine.Option(
    names = ["--model-release"],
    description = ["Model release identifier for rank table scoping"],
    defaultValue = "benchmark-model-v1",
  )
  private var modelRelease: String = "benchmark-model-v1"

  @CommandLine.Option(
    names = ["--num-pools"],
    description = ["Number of VID pools to simulate"],
    defaultValue = "6",
  )
  private var numPools: Int = 6

  override fun run() {
    runBlocking {
      println("Loading model from $modelPath...")
      val modelText = File(modelPath).readText()
      val compiledNode =
        CompiledNode.newBuilder()
          .also { TextFormat.merge(modelText, it) }
          .build()
      val labeler = Labeler.build(compiledNode)
      println("Model loaded successfully.")

      spannerFlags.usingSpanner { spanner ->
        val databaseClient = spanner.databaseClient
        val dataGenerator = DataGenerator(scaleFactor)
        val pipeline =
          Pipeline(
            databaseClient = databaseClient,
            labeler = labeler,
            dataProvider = dataProvider,
            modelRelease = modelRelease,
            numPools = numPools,
            dbBatchSize = dbBatchSize,
          )

        println()
        println("=== Memoized VID Assignment Benchmark ===")
        println(
          "Scale: ${scaleFactor}x | Days: $days | DB batch: $dbBatchSize | Pools: $numPools"
        )
        println(
          "Total reach: ${dataGenerator.totalReach} | Total impressions: ${dataGenerator.totalImpressions}"
        )
        println()

        pipeline.initializePoolCounters()

        val allDayResults = mutableListOf<DayResult>()
        for (day in 1..days) {
          val dayData = dataGenerator.generateDay(day)
          println(
            "--- Day $day: ${dayData.newAccountIds.size} new, " +
              "${dayData.returningAccountIds.size} returning, " +
              "${dayData.totalImpressions} impressions ---"
          )

          val result = pipeline.runDay(dayData)
          allDayResults.add(result)
          printDayResult(result)
          println()
        }

        println("=== Summary ===")
        printSummary(allDayResults)
      }
    }
  }

  private fun printDayResult(result: DayResult) {
    println("  Bulk Lookup:      ${result.bulkLookupMs} ms (${result.lookupCount} fingerprints)")
    println("  Pass 1 (Labeler): ${result.pass1Ms} ms (${result.pass1Count} labels)")
    println("  Rank Allocation:  ${result.rankAllocMs} ms (${result.rankAllocPoolCount} pools)")
    println("  Write Ranks:      ${result.writeRanksMs} ms (${result.writeCount} entries)")
    println("  Pass 2 (Labeler): ${result.pass2Ms} ms (${result.pass2Count} impressions)")
    val totalMs = result.totalMs
    println("  Total:            $totalMs ms")
    if (result.pass2Count > 0 && totalMs > 0) {
      println("  Throughput:       ${result.pass2Count * 1000L / totalMs} impressions/sec")
    }
  }

  private fun printSummary(results: List<DayResult>) {
    val totalMs = results.sumOf { it.totalMs }
    val totalLabelerMs = results.sumOf { it.pass1Ms + it.pass2Ms }
    val totalDbMs = results.sumOf { it.bulkLookupMs + it.rankAllocMs + it.writeRanksMs }
    val totalImpressions = results.sumOf { it.pass2Count }

    println("  Total time:       $totalMs ms (${totalMs / 1000} sec)")
    if (totalMs > 0) {
      println("  Labeler time:     $totalLabelerMs ms (${totalLabelerMs * 100 / totalMs}%)")
      println("  DB time:          $totalDbMs ms (${totalDbMs * 100 / totalMs}%)")
      println("  Throughput:       ${totalImpressions * 1000L / totalMs} impressions/sec")
    }
    println("  Total impressions labeled: $totalImpressions")
    println("  Total unique accounts:     ${results.sumOf { it.lookupCount }}")
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(BenchmarkApp(), args)
  }
}
