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

import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.SpannerOptions
import com.google.protobuf.TextFormat
import java.io.File
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.core.labeler.Labeler
import picocli.CommandLine

enum class StorageBackend {
  SPANNER,
  BIGTABLE,
}

@CommandLine.Command(
  name = "benchmark_app",
  description = ["Benchmark for memoized VID assignment pipeline"],
)
class BenchmarkApp : Runnable {
  @CommandLine.Option(
    names = ["--spanner-project"],
    description = ["Spanner project (required unless --skip-db)"],
    defaultValue = "",
  )
  private var spannerProject: String = ""

  @CommandLine.Option(
    names = ["--spanner-instance"],
    description = ["Spanner instance (required unless --skip-db)"],
    defaultValue = "",
  )
  private var spannerInstance: String = ""

  @CommandLine.Option(
    names = ["--spanner-database"],
    description = ["Spanner database (required unless --skip-db)"],
    defaultValue = "",
  )
  private var spannerDatabase: String = ""

  @CommandLine.Option(
    names = ["--model-path"],
    description = ["Path to the compiled model textproto file"],
    required = true,
  )
  private lateinit var modelPath: String

  @CommandLine.Option(
    names = ["--total-reach"],
    description = ["Total unique accounts to simulate across all days"],
    required = true,
  )
  private var totalReach: Int = 0

  @CommandLine.Option(
    names = ["--total-impressions"],
    description = ["Total impressions to simulate across all days"],
    required = true,
  )
  private var totalImpressions: Int = 0

  @CommandLine.Option(
    names = ["--days"],
    description = ["Number of days to simulate"],
    defaultValue = "7",
  )
  private var days: Int = 7

  @CommandLine.Option(
    names = ["--db-lookup-batch-size"],
    description = ["Fingerprints per Spanner lookup query (no mutation limit, can be large)"],
    defaultValue = "50000",
  )
  private var dbLookupBatchSize: Int = 50_000

  @CommandLine.Option(
    names = ["--db-write-batch-size"],
    description = ["Mutations per Spanner write commit (keep <=5000 to avoid StackOverflow)"],
    defaultValue = "5000",
  )
  private var dbWriteBatchSize: Int = 5_000

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

  @CommandLine.Option(
    names = ["--parallelism"],
    description = ["CPU workers for labeler and fingerprinting (0 = auto-detect cores)"],
    defaultValue = "0",
  )
  private var parallelism: Int = 0

  @CommandLine.Option(
    names = ["--db-read-parallelism"],
    description = ["Concurrent Spanner read-only operations (safe to set high)"],
    defaultValue = "32",
  )
  private var dbReadParallelism: Int = 32

  @CommandLine.Option(
    names = ["--db-write-parallelism"],
    description = ["Concurrent Spanner write operations (keep low to avoid contention)"],
    defaultValue = "8",
  )
  private var dbWriteParallelism: Int = 8

  @CommandLine.Option(
    names = ["--io-parallelism"],
    description = ["Concurrent GCS file read/write operations"],
    defaultValue = "16",
  )
  private var ioParallelism: Int = 16

  @CommandLine.Option(
    names = ["--gcs-bucket"],
    description = ["GCS bucket for impression I/O"],
    defaultValue = "secure-computation-storage-dev-bucket",
  )
  private var gcsBucket: String = "secure-computation-storage-dev-bucket"

  @CommandLine.Option(
    names = ["--gcs-prefix"],
    description = ["GCS path prefix for benchmark data"],
    defaultValue = "vid-labeler-benchmark",
  )
  private var gcsPrefix: String = "vid-labeler-benchmark"

  @CommandLine.Option(
    names = ["--start-date"],
    description = ["Start date for campaign simulation (YYYY-MM-DD)"],
    defaultValue = "2020-01-01",
  )
  private var startDate: String = "2020-01-01"

  @CommandLine.Option(
    names = ["--skip-data-generation"],
    description = ["Skip generating and uploading impressions to GCS (use existing data)"],
    defaultValue = "false",
  )
  private var skipDataGeneration: Boolean = false

  @CommandLine.Option(
    names = ["--storage-backend"],
    description = ["Storage backend: SPANNER or BIGTABLE"],
    defaultValue = "SPANNER",
  )
  private var storageBackend: StorageBackend = StorageBackend.SPANNER

  @CommandLine.Option(
    names = ["--bigtable-project"],
    description = ["GCP project for Bigtable (required if --storage-backend=BIGTABLE)"],
    defaultValue = "",
  )
  private var bigtableProject: String = ""

  @CommandLine.Option(
    names = ["--bigtable-instance"],
    description = ["Bigtable instance ID (required if --storage-backend=BIGTABLE)"],
    defaultValue = "",
  )
  private var bigtableInstance: String = ""

  @CommandLine.Option(
    names = ["--bigtable-rank-table"],
    description = ["Bigtable table ID for rank data"],
    defaultValue = "rank-table",
  )
  private var bigtableRankTable: String = "rank-table"

  @CommandLine.Option(
    names = ["--bigtable-counter-table"],
    description = ["Bigtable table ID for pool counters"],
    defaultValue = "pool-counter",
  )
  private var bigtableCounterTable: String = "pool-counter"

  @CommandLine.Option(
    names = ["--skip-db"],
    description = ["Skip all database operations; every account is treated as new each day"],
    defaultValue = "false",
  )
  private var skipDb: Boolean = false

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

      val workers =
        if (parallelism > 0) parallelism else Runtime.getRuntime().availableProcessors()

      println()
      println("=== Memoized VID Assignment Benchmark ===")
      println("  Reach: $totalReach | Impressions: $totalImpressions | Days: $days")
      println("  Storage backend: ${if (skipDb) "NONE (--skip-db)" else storageBackend.name}")
      println(
        "  CPU workers: $workers | DB read: $dbReadParallelism | DB write: $dbWriteParallelism | I/O: $ioParallelism"
      )
      println(
        "  DB lookup batch: $dbLookupBatchSize | DB write batch: $dbWriteBatchSize | Pools: $numPools"
      )
      println()

      val storageCloseable: AutoCloseable
      val storage: RankTableStorage

      if (skipDb) {
        println("Database operations DISABLED (--skip-db). All accounts treated as new.")
        storage = NoOpRankTableStorage()
        storageCloseable = storage
      } else when (storageBackend) {
        StorageBackend.BIGTABLE -> {
          require(bigtableProject.isNotEmpty()) {
            "--bigtable-project is required when --storage-backend=BIGTABLE"
          }
          require(bigtableInstance.isNotEmpty()) {
            "--bigtable-instance is required when --storage-backend=BIGTABLE"
          }
          println("Connecting to Bigtable: $bigtableProject / $bigtableInstance")
          val settings =
            BigtableDataSettings.newBuilder()
              .setProjectId(bigtableProject)
              .setInstanceId(bigtableInstance)
              .build()
          val btClient = BigtableDataClient.create(settings)
          val btStorage =
            BigtableRankTableStorage(
              btClient,
              rankTableId = bigtableRankTable,
              counterTableId = bigtableCounterTable,
            )
          storage = btStorage
          storageCloseable = btStorage
        }
        StorageBackend.SPANNER -> {
          require(spannerProject.isNotEmpty()) {
            "--spanner-project is required when not using --skip-db"
          }
          require(spannerInstance.isNotEmpty()) {
            "--spanner-instance is required when not using --skip-db"
          }
          require(spannerDatabase.isNotEmpty()) {
            "--spanner-database is required when not using --skip-db"
          }
          println("Connecting to Spanner...")
          val spannerService =
            SpannerOptions.newBuilder()
              .setProjectId(spannerProject)
              .build()
              .service
          val dbClient =
            spannerService.getDatabaseClient(
              DatabaseId.of(
                spannerProject,
                spannerInstance,
                spannerDatabase,
              )
            )
          storage = SpannerRankTableStorage(dbClient)
          storageCloseable = spannerService
        }
      }

      storageCloseable.use {
        val dataGenerator = DataGenerator(totalReach, totalImpressions)

        val pipeline =
          Pipeline(
            storage = storage,
            labeler = labeler,
            dataProvider = dataProvider,
            modelRelease = modelRelease,
            numPools = numPools,
            dbLookupBatchSize = dbLookupBatchSize,
            dbWriteBatchSize = dbWriteBatchSize,
            parallelism = workers,
            dbReadParallelism = dbReadParallelism,
            dbWriteParallelism = dbWriteParallelism,
            ioParallelism = ioParallelism,
          )

        pipeline.initializePoolCounters()

        val gcsIo = GcsIo(gcsBucket)

        // === Data Generation Phase (not benchmarked) ===
        if (!skipDataGeneration) {
          println()
          println("=== Data Generation -- uploading impressions to GCS ===")
          for (day in 1..days) {
            val dayData = dataGenerator.generateDay(day)
            val date = LocalDate.parse(startDate).plusDays((day - 1).toLong())
            val dayPrefix = "$gcsPrefix/$date"

            gcsIo.deletePrefix(dayPrefix)

            val allAccountIds = dayData.newAccountIds + dayData.returningAccountIds
            val totalDayImpressions = dayData.totalImpressions

            var written = 0
            var fileIndex = 0
            while (written < totalDayImpressions) {
              val batchSize = minOf(IMPRESSIONS_PER_FILE, totalDayImpressions - written)
              val batch =
                (0 until batchSize).map { i ->
                  val idx = (written + i) % allAccountIds.size
                  val userId = allAccountIds[idx]
                  val accountIndex = userId.removePrefix("user_").toInt()
                  DataGenerator.buildLabelerInput(userId, accountIndex)
                }
              val filePath =
                "$dayPrefix/impressions_${String.format("%06d", fileIndex)}.pb"
              gcsIo.writeImpressions(filePath, batch)
              written += batchSize
              fileIndex++
              if (fileIndex % 10 == 0) {
                print(
                  "\r  Day $day: $written/$totalDayImpressions impressions ($fileIndex files)"
                )
              }
            }
            println(
              "\r  Day $day: $written/$totalDayImpressions impressions ($fileIndex files) -- done"
            )
          }
          println()
        } else {
          println()
          println("=== Data Generation -- SKIPPED (using existing GCS data) ===")
        }

        // Reset the data generator for the benchmark run
        val dataGenerator2 = DataGenerator(totalReach, totalImpressions)

        val allDayResults = mutableListOf<DayResult>()
        for (day in 1..days) {
          val dayData = dataGenerator2.generateDay(day)
          val date = LocalDate.parse(startDate).plusDays((day - 1).toLong())
          val dayGcsPrefix = "$gcsPrefix/$date"
          println(
            "--- Day $day: ${dayData.newAccountIds.size} new, " +
              "${dayData.returningAccountIds.size} returning, " +
              "${dayData.totalImpressions} impressions ---"
          )

          val result = pipeline.runDay(dayData, gcsIo, dayGcsPrefix)
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
    println("  Phase Timing:")
    println(
      "    GCS Scan:         ${result.readGcsMs} ms (${result.readGcsFileCount} files)"
    )
    println(
      "    Fingerprints:     ${result.fingerprintMs} ms (${result.fingerprintCount} accounts)"
    )
    println(
      "    Bulk Lookup:      ${result.bulkLookupMs} ms (${result.lookupCount} fingerprints)"
    )
    println(
      "    Pass 1 (Labeler): ${result.pass1Ms} ms (${result.pass1Count} labels)"
    )
    println(
      "    Rank Allocation:  ${result.rankAllocMs} ms (${result.rankAllocPoolCount} pools)"
    )
    println(
      "    Write Ranks:      ${result.writeRanksMs} ms (${result.writeCount} entries)"
    )
    println(
      "    Pass 2 + Write:   ${result.pass2Ms} ms" +
        " (${result.pass2Count} impressions, ${result.writeGcsFileCount} files)"
    )
    println(
      "      Cache: ${result.pass2CacheHits} hits," +
        " ${result.pass2CacheMisses} misses" +
        " (${if (result.pass2Count > 0) result.pass2CacheHits * 100 / result.pass2Count else 0}% hit rate)"
    )
    val totalMs = result.totalMs
    println("    Total:            $totalMs ms")
    if (result.pass2Count > 0 && totalMs > 0) {
      println(
        "    Throughput:       ${result.pass2Count * 1000L / totalMs} impressions/sec"
      )
    }
  }

  private fun printSummary(results: List<DayResult>) {
    val totalMs = results.sumOf { it.totalMs }
    val stage1Ms = results.sumOf { it.readGcsMs + it.fingerprintMs }
    val stage2Ms =
      results.sumOf {
        it.bulkLookupMs + it.pass1Ms + it.rankAllocMs + it.writeRanksMs
      }
    val stage3Ms = results.sumOf { it.pass2Ms }
    val totalDbMs =
      results.sumOf { it.bulkLookupMs + it.rankAllocMs + it.writeRanksMs }
    val totalLabelerMs = results.sumOf { it.pass1Ms } + stage3Ms
    val totalImpressions = results.sumOf { it.pass2Count }
    val totalCacheHits = results.sumOf { it.pass2CacheHits }
    val totalCacheMisses = results.sumOf { it.pass2CacheMisses }

    println("  Total time:         $totalMs ms (${totalMs / 1000} sec)")
    if (totalMs > 0) {
      println(
        "  Stage 1 (Discovery):  $stage1Ms ms (${stage1Ms * 100 / totalMs}%)" +
          " [GCS scan + fingerprinting]"
      )
      println(
        "  Stage 2 (Ranks):      $stage2Ms ms (${stage2Ms * 100 / totalMs}%)" +
          " [lookup + Pass 1 + alloc + write]"
      )
      println(
        "  Stage 3 (Label+Out):  $stage3Ms ms (${stage3Ms * 100 / totalMs}%)" +
          " [Pass 2 cached + GCS output]"
      )
      println("  ---")
      println(
        "  DB time:              $totalDbMs ms (${totalDbMs * 100 / totalMs}%)" +
          " [lookup + alloc + write ranks]"
      )
      println(
        "  Labeler time (approx):$totalLabelerMs ms" +
          " [Pass 1 + Stage 3 (includes GCS I/O)]"
      )
      println(
        "  Cache:                $totalCacheHits hits, $totalCacheMisses misses" +
          " (${if (totalImpressions > 0) totalCacheHits * 100L / totalImpressions else 0}% hit rate)"
      )
      println(
        "  Throughput:           ${totalImpressions * 1000L / totalMs} impressions/sec"
      )
    }
    println("  Total impressions:    $totalImpressions")
    println(
      "  Total unique accounts:${results.sumOf { it.lookupCount }}"
    )
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(BenchmarkApp(), args)
  }
}
