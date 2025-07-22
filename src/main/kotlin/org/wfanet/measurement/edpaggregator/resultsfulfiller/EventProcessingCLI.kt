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

import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.type.Interval
import com.google.protobuf.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

/**
 * Command-line interface for the event processing pipeline.
 * Handles argument parsing and delegates to the pipeline orchestrator.
 */
@CommandLine.Command(
  name = "ResultsFulfillerPipeline",
  description = ["Event processing pipeline with synthetic data generation, filtering and measurement"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class EventProcessingCLI : Runnable {

  companion object {
    private val logger = Logger.getLogger(EventProcessingCLI::class.java.name)

    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(EventProcessingCLI(), args)
  }

  @CommandLine.Option(
    names = ["--start-date"],
    description = ["Start date for event range (YYYY-MM-DD)"],
    defaultValue = "2025-01-01"
  )
  private lateinit var startDateStr: String

  @CommandLine.Option(
    names = ["--end-date"],
    description = ["End date for event range (YYYY-MM-DD)"],
    defaultValue = "2025-01-02"
  )
  private lateinit var endDateStr: String

  @CommandLine.Option(
    names = ["--batch-size"],
    description = ["Event batch size for processing"],
    defaultValue = "10000"
  )
  private var batchSize: Int = 10000

  @CommandLine.Option(
    names = ["--channel-capacity"],
    description = ["Channel capacity for backpressure control"],
    defaultValue = "100"
  )
  private var channelCapacity: Int = 100

  @CommandLine.Option(
    names = ["--max-vid-range"],
    description = ["Maximum VID value for PopulationSpec"],
    defaultValue = "10000000"
  )
  private var maxVidRange: Long = 10_000_000L

  @CommandLine.Option(
    names = ["--total-events"],
    description = ["Total number of synthetic events to generate"],
    defaultValue = "10000000"
  )
  private var totalEvents: Long = 10_000_000L

  @CommandLine.Option(
    names = ["--synthetic-unique-vids"],
    description = ["Number of unique VIDs in synthetic data"],
    defaultValue = "1000000"
  )
  private var syntheticUniqueVids: Int = 1_000_000

  @CommandLine.Option(
    names = ["--use-parallel-pipeline"],
    description = ["Use parallel batched pipeline instead of single pipeline"],
    defaultValue = "false"
  )
  private var useParallelPipeline: Boolean = false

  @CommandLine.Option(
    names = ["--parallel-batch-size"],
    description = ["Batch size for parallel pipeline"],
    defaultValue = "100000"
  )
  private var parallelBatchSize: Int = 100000

  @CommandLine.Option(
    names = ["--parallel-workers"],
    description = ["Number of worker threads for parallel pipeline (0 = use all CPU cores)"],
    defaultValue = "0"
  )
  private var parallelWorkers: Int = 0

  @CommandLine.Option(
    names = ["--thread-pool-size"],
    description = ["Custom thread pool size (0 = 4x CPU cores)"],
    defaultValue = "0"
  )
  private var threadPoolSize: Int = 0

  @CommandLine.Option(
    names = ["--collection-start-time"],
    description = ["Collection interval start time (ISO-8601 format, e.g., 2025-01-01T00:00:00Z)"],
    defaultValue = ""
  )
  private var collectionStartTime: String = ""

  @CommandLine.Option(
    names = ["--collection-end-time"],
    description = ["Collection interval end time (ISO-8601 format, e.g., 2025-01-02T00:00:00Z)"],
    defaultValue = ""
  )
  private var collectionEndTime: String = ""

  override fun run() = runBlocking {
    logger.info("Starting Event Processing Pipeline")
    println("Event Processing Pipeline")
    println("Structure: Parallel Event Readers -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks")
    println()

    try {
      initializeTink()

      val config = buildConfiguration()
      val orchestrator = EventProcessingOrchestrator()

      orchestrator.run(config)
    } catch (e: Exception) {
      logger.severe("Pipeline execution failed: ${e.message}")
      throw e
    }
  }

  private fun initializeTink() {
    logger.info("Initializing Tink crypto")
    AeadConfig.register()
    StreamingAeadConfig.register()
    logger.info("Tink initialization completed successfully")
    println("Tink initialization complete")
  }

  private fun buildConfiguration(): PipelineConfiguration {
    val startDate = parseDate(startDateStr, "start date")
    val endDate = parseDate(endDateStr, "end date")

    val collectionInterval = parseCollectionInterval()

    return PipelineConfiguration(
      startDate = startDate,
      endDate = endDate,
      batchSize = batchSize,
      channelCapacity = channelCapacity,
      maxVidRange = maxVidRange,
      totalEvents = totalEvents,
      syntheticUniqueVids = syntheticUniqueVids,
      useParallelPipeline = useParallelPipeline,
      parallelBatchSize = parallelBatchSize,
      parallelWorkers = if (parallelWorkers == 0) Runtime.getRuntime().availableProcessors() else parallelWorkers,
      threadPoolSize = if (threadPoolSize == 0) Runtime.getRuntime().availableProcessors() * 8 else threadPoolSize,
      collectionInterval = collectionInterval
    )
  }

  private fun parseDate(dateStr: String, description: String): LocalDate {
    return try {
      LocalDate.parse(dateStr)
    } catch (e: Exception) {
      logger.severe("Failed to parse $description '$dateStr': ${e.message}")
      throw IllegalArgumentException("Invalid $description: $dateStr", e)
    }
  }

  private fun parseCollectionInterval(): Interval? {
    return if (collectionStartTime.isNotEmpty() && collectionEndTime.isNotEmpty()) {
      try {
        val startInstant = Instant.parse(collectionStartTime)
        val endInstant = Instant.parse(collectionEndTime)

        Interval.newBuilder()
          .setStartTime(Timestamp.newBuilder()
            .setSeconds(startInstant.epochSecond)
            .setNanos(startInstant.nano))
          .setEndTime(Timestamp.newBuilder()
            .setSeconds(endInstant.epochSecond)
            .setNanos(endInstant.nano))
          .build()
      } catch (e: Exception) {
        logger.severe("Failed to parse collection interval: ${e.message}")
        throw IllegalArgumentException("Invalid collection interval", e)
      }
    } else {
      null
    }
  }
}
