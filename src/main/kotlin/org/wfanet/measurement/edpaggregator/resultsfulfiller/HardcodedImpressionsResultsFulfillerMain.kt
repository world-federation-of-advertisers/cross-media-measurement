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

import com.google.crypto.tink.*
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any as ProtobufAny
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.type.interval
import java.io.FileInputStream
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.logging.Logger
import java.util.logging.Level
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Video
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidNotFoundException
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.dataprovider.VidFilter
import org.wfanet.sampling.VidSampler
import picocli.CommandLine
import kotlin.system.measureTimeMillis

/**
 * SIMPLIFIED DEMO PIPELINE: CEL-ONLY FILTERING WITH SYNTHETIC DATA
 *
 * This implementation demonstrates a streamlined event processing pipeline with the following architecture:
 *
 * PIPELINE STRUCTURE:
 * [Synthetic Event Generator] -> [Batching] -> [Fan Out] -> [CEL Filtering] -> [Frequency Vector Sinks]
 *
 * KEY DESIGN PRINCIPLES:
 * 1. **Simplified Architecture**: Only CEL filtering, no complex masking operations
 * 2. **Synthetic Data Generation**: Configurable synthetic event generation for load testing
 * 3. **Batched Processing**: Events are processed in configurable batch sizes
 * 4. **Fan-Out to Multiple CEL Filters**: Each batch is processed by multiple CEL filter coroutines
 * 5. **Memory-Efficient Storage**: Striped byte arrays for frequency vectors
 * 6. **Demonstration Focus**: Two CEL filters (person.gender == 1 [MALE] and person.gender == 2 [FEMALE])
 *
 * PERFORMANCE CHARACTERISTICS:
 * - High-throughput synthetic event generation
 * - Realistic VID frequency distribution (Zipf-like)
 * - Configurable batch sizes for optimal memory/throughput balance
 * - Multiple CEL filter coroutines processing batches concurrently
 * - Lock-striped frequency counting for high-concurrency updates
 */
/**
 * Thread-safe, memory-efficient frequency vector using striped byte arrays.
 */
class StripedByteFrequencyVector(private val size: Int) {
    private val stripeCount = 1024
    private val stripeSize = (size + stripeCount - 1) / stripeCount
    private val data = ByteArray(size)
    private val locks = Array(stripeCount) { Any() }

    private fun getStripe(index: Int) = index / stripeSize

    fun incrementByIndex(index: Int) {
        if (index >= 0 && index < size) {
            synchronized(locks[getStripe(index)]) {
                val current = data[index].toInt() and 0xFF
                if (current < 255) {
                    data[index] = (current + 1).toByte()
                }
            }
        }
    }

    fun computeStatistics(): Pair<Double, Long> {
        var totalFrequency = 0L
        var nonZeroCount = 0L

        for (i in 0 until stripeCount) {
            synchronized(locks[i]) {
                val start = i * stripeSize
                val end = minOf(start + stripeSize, size)
                for (j in start until end) {
                    val count = data[j].toInt() and 0xFF
                    if (count > 0) {
                        totalFrequency += count
                        nonZeroCount++
                    }
                }
            }
        }

        val averageFrequency = if (nonZeroCount > 0) totalFrequency.toDouble() / nonZeroCount else 0.0
        return Pair(averageFrequency, nonZeroCount)
    }

    fun getTotalCount(): Long {
        var total = 0L
        for (i in 0 until stripeCount) {
            synchronized(locks[i]) {
                val start = i * stripeSize
                val end = minOf(start + stripeSize, size)
                for (j in start until end) {
                    total += data[j].toInt() and 0xFF
                }
            }
        }
        return total
    }
}

/**
 * Simple event batch container for efficient processing.
 */
data class EventBatch(
    val events: List<LabeledEvent<TestEvent>>,
    val batchId: Long,
    val timestamp: Long = System.currentTimeMillis()
) {
    val size: Int get() = events.size
}

/**
 * CEL filter processor for event filtering.
 *
 * This processor compiles a CEL expression once and reuses it for batch processing.
 * It processes events sequentially through the CEL filter and identifies matching events.
 */
class CelFilterProcessor(
    val filterId: String,
    val celExpression: String,
    private val eventMessageDescriptor: Descriptors.Descriptor,
    private val typeRegistry: TypeRegistry
) {
    companion object {
        private val logger = Logger.getLogger(CelFilterProcessor::class.java.name)
    }
    private val program: Program = if (celExpression.isEmpty()) {
        Program { Program.newEvalResult(BoolT.True, null) }
    } else {
        EventFilters.compileProgram(eventMessageDescriptor, celExpression)
    }

    /**
     * Processes a batch of events and returns the matching events.
     */
    suspend fun processBatch(batch: EventBatch): List<LabeledEvent<TestEvent>> {
        return batch.events.filter { event ->
            try {
                EventFilters.matches(event.message, program)
            } catch (e: Exception) {
                logger.warning("CEL filter evaluation failed for event with VID ${event.vid} (filter: $filterId): ${e.message}")
                false
            }
        }
    }
}

/**
 * Frequency vector sink that receives filtered events and updates frequency counts.
 *
 * Each sink corresponds to a specific CEL filter and maintains its own frequency vector.
 */
class FrequencyVectorSink(
    val sinkId: String,
    val description: String,
    private val vidIndexMap: VidIndexMap
) {
    companion object {
        private val logger = Logger.getLogger(FrequencyVectorSink::class.java.name)
    }
    private val frequencyVector = StripedByteFrequencyVector(vidIndexMap.size.toInt())
    private val processedCount = AtomicLong(0)
    private val matchedCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)

    suspend fun processMatchedEvents(matchedEvents: List<LabeledEvent<TestEvent>>, totalProcessed: Int) {
        processedCount.addAndGet(totalProcessed.toLong())
        matchedCount.addAndGet(matchedEvents.size.toLong())

        matchedEvents.forEach { event ->
            try {
                val index = vidIndexMap[event.vid]
                frequencyVector.incrementByIndex(index)
            } catch (e: VidNotFoundException) {
                errorCount.incrementAndGet()
                logger.warning("VID not found in index map: ${event.vid} (sink: $sinkId)")
            }
        }
    }

    fun getStatistics(): SinkStatistics {
        val (avgFreq, nonZeroCount) = frequencyVector.computeStatistics()
        val totalFrequency = frequencyVector.getTotalCount()

        return SinkStatistics(
            sinkId = sinkId,
            description = description,
            processedEvents = processedCount.get(),
            matchedEvents = matchedCount.get(),
            errorCount = errorCount.get(),
            reach = nonZeroCount,
            totalFrequency = totalFrequency,
            averageFrequency = avgFreq
        )
    }
}

/**
 * Statistics for a frequency vector sink.
 */
data class SinkStatistics(
    val sinkId: String,
    val description: String,
    val processedEvents: Long,
    val matchedEvents: Long,
    val errorCount: Long,
    val reach: Long,
    val totalFrequency: Long,
    val averageFrequency: Double
) {
    val matchRate: Double = if (processedEvents > 0) matchedEvents * 100.0 / processedEvents else 0.0
    val errorRate: Double = if (processedEvents > 0) errorCount * 100.0 / processedEvents else 0.0
}

/**
 * Simplified pipeline processor that orchestrates CEL-only event processing.
 *
 * PIPELINE FLOW:
 * 1. **Parallel Event Readers**: Read events from multiple date partitions concurrently
 * 2. **Batching**: Group events into configurable batch sizes
 * 3. **Fan Out**: Distribute batches to multiple CEL filter processors
 * 4. **CEL Filtering**: Apply CEL filters to identify matching events
 * 5. **Frequency Vector Sinks**: Update frequency counts for matching events
 *
 * The pipeline uses channels for coordination and maintains backpressure through bounded capacities.
 */
class SimplifiedEventProcessingPipeline {

    /**
     * Main processing method that implements the simplified CEL-only pipeline.
     */
    suspend fun processEvents(
        eventFlow: Flow<LabeledEvent<TestEvent>>,
        batchSize: Int,
        channelCapacity: Int,
        vidIndexMap: VidIndexMap,
        celFilters: Map<String, String>,
        eventMessageDescriptor: Descriptors.Descriptor,
        typeRegistry: TypeRegistry
    ): Map<String, SinkStatistics> = coroutineScope {

        println("Starting simplified CEL-only pipeline with:")
        println("  Batch size: $batchSize")
        println("  Channel capacity: $channelCapacity")
        println("  CEL filters: ${celFilters.values}")
        println()

        // Create CEL filter processors
        val celProcessors = celFilters.map { (filterId, expression) ->
            CelFilterProcessor(filterId, expression, eventMessageDescriptor, typeRegistry)
        }

        // Create frequency vector sinks (one per CEL filter)
        val sinks = celFilters.map { (filterId, expression) ->
            filterId to FrequencyVectorSink(
                sinkId = filterId,
                description = "CEL: $expression",
                vidIndexMap = vidIndexMap
            )
        }.toMap()

        // Channel for batched events
        val batchChannel = Channel<EventBatch>(capacity = channelCapacity)

        // Statistics tracking
        val totalBatches = AtomicLong(0)
        val totalEvents = AtomicLong(0)
        val startTime = System.currentTimeMillis()

        // Stage 1: Parallel Event Readers -> Batching
        val batchingJob = launch(Dispatchers.IO) {
            println("Batching stage started, waiting for events...")
            var batchId = 0L
            eventFlow
                .onStart {
                    println("Event flow started in batching stage")
                }
                .chunked(batchSize)
                .collect { eventList ->
                    val batch = EventBatch(eventList, batchId++)
                    batchChannel.send(batch)
                    totalBatches.incrementAndGet()
                    totalEvents.addAndGet(eventList.size.toLong())

                    if (batchId % 10 == 0L) {
                        val elapsed = System.currentTimeMillis() - startTime
                        val eventsPerSec = if (elapsed > 0) totalEvents.get() * 1000 / elapsed else 0
                        println("Processed ${totalEvents.get()} events in ${totalBatches.get()} batches ($eventsPerSec events/sec)")
                    }
                }
            println("Batching stage completed. Total events processed: ${totalEvents.get()}, Total batches: ${totalBatches.get()}")
            batchChannel.close()
        }

        // Stage 2: Fan Out -> CEL Filtering -> Frequency Vector Sinks (Fully Asynchronous)
        val processingJobs = (1..3).map { workerId ->
            launch(Dispatchers.Default) {
                for (batch in batchChannel) {
                    // Launch independent processing for each CEL filter
                    // No synchronization bottlenecks - each filter processes and updates sink immediately
                    celProcessors.map { processor ->
                        launch {
                            val matchedEvents = processor.processBatch(batch)
                            sinks[processor.filterId]?.processMatchedEvents(matchedEvents, batch.size)
                        }
                    }
                }
            }
        }

        // Wait for all processing to complete
        batchingJob.join()
        processingJobs.forEach { it.join() }

        val endTime = System.currentTimeMillis()
        val totalProcessingTime = endTime - startTime

        println("\nPipeline completed:")
        println("  Total events: ${totalEvents.get()}")
        println("  Total batches: ${totalBatches.get()}")
        println("  Processing time: $totalProcessingTime ms")
        println("  Throughput: ${if (totalProcessingTime > 0) totalEvents.get() * 1000 / totalProcessingTime else 0} events/sec")
        println()

        // Return statistics from all sinks
        sinks.mapValues { (_, sink) -> sink.getStatistics() }
    }
}

/**
 * Enhanced command-line tool demonstrating the simplified CEL filtering pipeline with synthetic data.
 */
@CommandLine.Command(
  name = "SimplifiedEventProcessingPipeline",
  description = ["Simplified pipeline with synthetic data: Synthetic Generator -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class HardcodedImpressionsResultsFulfillerMain : Runnable {

  companion object {
    private val logger = Logger.getLogger(HardcodedImpressionsResultsFulfillerMain::class.java.name)

    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(HardcodedImpressionsResultsFulfillerMain(), args)
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
    names = ["--cel-filter-1"],
    description = ["First CEL filter expression"],
    defaultValue = "person.gender == 1"
  )
  private lateinit var celFilter1: String

  @CommandLine.Option(
    names = ["--cel-filter-2"],
    description = ["Second CEL filter expression"],
    defaultValue = "person.gender == 2"
  )
  private lateinit var celFilter2: String

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

  override fun run() = runBlocking {
    logger.info("Starting Simplified Event Processing Pipeline")
    println("Simplified Event Processing Pipeline")
    println("Structure: Parallel Event Readers -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks")
    println("CEL Filters: '$celFilter1' (MALE), '$celFilter2' (FEMALE)")
    println()

    try {
      // Initialize Tink crypto
      AeadConfig.register()
      StreamingAeadConfig.register()
      logger.info("Tink initialization completed successfully")
      println("Tink initialization complete")
    } catch (e: Exception) {
      logger.severe("Failed to initialize Tink crypto: ${e.message}")
      throw e
    }

    // Create custom thread pool executors for maximum performance
    val actualThreadPoolSize = if (threadPoolSize == 0) Runtime.getRuntime().availableProcessors() * 4 else threadPoolSize
    logger.info("Creating custom thread pools: default=$actualThreadPoolSize, I/O=${actualThreadPoolSize * 2}")

    val customThreadPool = try {
      Executors.newFixedThreadPool(actualThreadPoolSize) { r ->
        Thread(r, "CustomPool-${Thread.activeCount()}").apply {
          isDaemon = false
          priority = Thread.NORM_PRIORITY
        }
      }
    } catch (e: Exception) {
      logger.severe("Failed to create custom thread pool: ${e.message}")
      throw e
    }
    val customDispatcher = customThreadPool.asCoroutineDispatcher()

    val ioThreadPoolSize = actualThreadPoolSize * 2
    val ioThreadPool = try {
      Executors.newFixedThreadPool(ioThreadPoolSize) { r ->
        Thread(r, "CustomIO-${Thread.activeCount()}").apply {
          isDaemon = false
          priority = Thread.NORM_PRIORITY
        }
      }
    } catch (e: Exception) {
      logger.severe("Failed to create I/O thread pool: ${e.message}")
      customThreadPool.shutdown()
      throw e
    }
    val customIODispatcher = ioThreadPool.asCoroutineDispatcher()

    logger.info("Custom thread pools initialized successfully")
    println("Custom thread pools initialized:")
    println("  Default thread pool: $actualThreadPoolSize threads")
    println("  I/O thread pool: $ioThreadPoolSize threads")

    // Parse configuration
    val startDate = try {
      LocalDate.parse(startDateStr)
    } catch (e: Exception) {
      logger.severe("Failed to parse start date '$startDateStr': ${e.message}")
      throw e
    }

    val endDate = try {
      LocalDate.parse(endDateStr)
    } catch (e: Exception) {
      logger.severe("Failed to parse end date '$endDateStr': ${e.message}")
      throw e
    }

    val timeRange = OpenEndTimeRange.fromClosedDateRange(startDate..endDate)
    logger.info("Time range configured: $startDate to $endDate")

    println("Configuration:")
    println("  Date range: $startDate to $endDate")
    println("  Max VID range: $maxVidRange")
    println("  Batch size: $batchSize")
    println("  Channel capacity: $channelCapacity")
    println()
    println("Synthetic Data Configuration:")
    println("  Total events: $totalEvents")
    println("  Unique VIDs: $syntheticUniqueVids")
    println()
    println("Pipeline Configuration:")
    println("  Use parallel pipeline: $useParallelPipeline")
    if (useParallelPipeline) {
      val actualWorkers = if (parallelWorkers == 0) Runtime.getRuntime().availableProcessors() else parallelWorkers
      println("  Parallel batch size: $parallelBatchSize")
      println("  Parallel workers: $parallelWorkers (actual: $actualWorkers)")
      println("  Available CPU cores: ${Runtime.getRuntime().availableProcessors()}")
    }
    println()

    // Create population spec and VID index map
    println("Creating PopulationSpec and VID index map...")
    logger.info("Creating PopulationSpec with max VID range: $maxVidRange")

    val populationSpec = try {
      createPopulationSpec(maxVidRange)
    } catch (e: Exception) {
      logger.severe("Failed to create PopulationSpec: ${e.message}")
      throw e
    }

    val vidIndexMap = try {
      InMemoryVidIndexMap.build(populationSpec)
    } catch (e: Exception) {
      logger.severe("Failed to build VID index map: ${e.message}")
      throw e
    }

    logger.info("VID index map created successfully with ${vidIndexMap.size} entries")
    println("VID index map created with ${vidIndexMap.size} entries")
    println()

    // Set up event infrastructure
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    // Define CEL filters for demo
    val celFilters = mapOf(
      "filter_1" to celFilter1,
      "filter_2" to celFilter2
    )

    // Create synthetic event flow with custom dispatcher
    println("Creating synthetic event flow...")
    val eventFlow = createSyntheticEventFlow(
      timeRange = timeRange,
      totalEvents = totalEvents,
      uniqueVids = syntheticUniqueVids,
      customDispatcher = customDispatcher
    )

    // Choose pipeline based on CLI option
    val pipelineStartTime = System.currentTimeMillis()

    val statistics = if (useParallelPipeline) {
      println("Running parallel batched pipeline with synthetic data...")
      val actualWorkers = if (parallelWorkers == 0) Runtime.getRuntime().availableProcessors() else parallelWorkers
      runParallelPipeline(
        eventFlow = eventFlow,
        vidIndexMap = vidIndexMap,
        celFilters = celFilters,
        batchSize = parallelBatchSize,
        workers = actualWorkers,
        customDispatcher = customDispatcher,
        ioDispatcher = customIODispatcher
      )
    } else {
      println("Running single end-to-end pipeline with synthetic data...")
      runSinglePipeline(
        eventFlow = eventFlow,
        vidIndexMap = vidIndexMap,
        celFilters = celFilters,
        customDispatcher = customDispatcher
      )
    }

    // Cleanup thread pools
    logger.info("Shutting down thread pools...")
    try {
      customThreadPool.shutdown()
      ioThreadPool.shutdown()

      if (!customThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.warning("Custom thread pool did not terminate gracefully, forcing shutdown")
        customThreadPool.shutdownNow()
      }

      if (!ioThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.warning("I/O thread pool did not terminate gracefully, forcing shutdown")
        ioThreadPool.shutdownNow()
      }

      logger.info("Thread pools shut down successfully")
      println("Thread pools shut down successfully")
    } catch (e: Exception) {
      logger.severe("Error shutting down thread pools: ${e.message}")
      println("Error shutting down thread pools: ${e.message}")
    }

    val pipelineEndTime = System.currentTimeMillis()
    val totalDuration = pipelineEndTime - pipelineStartTime

    println("\nPipeline Execution Summary:")
    println("===========================")
    println("Total duration: ${totalDuration}ms")
    println()

    // Display statistics for each CEL filter
    statistics.forEach { (filterId, stats) ->
      println("Filter: $filterId")
      println("  Description: ${stats.description}")
      println("  Processed events: ${stats.processedEvents}")
      println("  Matched events: ${stats.matchedEvents} (${String.format("%.2f", stats.matchRate)}%)")
      println("  Errors: ${stats.errorCount} (${String.format("%.2f", stats.errorRate)}%)")
      println("  Reach: ${stats.reach}")
      println("  Total frequency: ${stats.totalFrequency}")
      println("  Average frequency: ${String.format("%.2f", stats.averageFrequency)}")
      println()
    }

    // Memory usage
    val runtime = Runtime.getRuntime()
    val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
    println("\nMemory used: ${usedMemory} MB")
  }

  private fun createPopulationSpec(maxVid: Long): PopulationSpec {
    return populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = maxVid
        }
      }
    }
  }


  private fun createSyntheticEventFlow(
    timeRange: OpenEndTimeRange,
    totalEvents: Long,
    uniqueVids: Int,
    customDispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Flow<LabeledEvent<TestEvent>> = channelFlow {
    val startTime = timeRange.start
    val endTime = timeRange.endExclusive
    val duration = java.time.Duration.between(startTime, endTime)

    println("Generating synthetic events:")
    println("  Start time: $startTime")
    println("  End time: $endTime")
    println("  Duration: ${duration.seconds} seconds")
    println("  Total events to generate: $totalEvents")
    println("  Unique VIDs: $uniqueVids")
    println("  Distribution: Uniform across time range")

    // Pre-compute shared data for all threads
    val durationSecondsDouble = duration.seconds.toDouble()
    val vidArray = LongArray(uniqueVids) { (it + 1).toLong() }
    val genders = arrayOf(Person.Gender.MALE, Person.Gender.FEMALE)
    val validAgeGroups = arrayOf(
      Person.AgeGroup.YEARS_18_TO_34,
      Person.AgeGroup.YEARS_35_TO_54,
      Person.AgeGroup.YEARS_55_PLUS
    )

    // Parallel generation using multiple coroutines - utilize all CPU cores
    val numThreads = Runtime.getRuntime().availableProcessors() * 10
    val eventsPerThread = (totalEvents / numThreads).toLong()
    val remainingEvents = (totalEvents % numThreads).toInt()

    println("Using $numThreads parallel generators (${Runtime.getRuntime().availableProcessors()} CPU cores), $eventsPerThread events per thread")

    // Launch parallel event generators
    (0 until numThreads).map { threadId ->
      launch(customDispatcher) {
        val random = kotlin.random.Random(System.nanoTime() + threadId)
        val eventsToGenerate = eventsPerThread + (if (threadId < remainingEvents) 1 else 0)
        val batchSize = 10000
        var generated = 0L

        while (generated < eventsToGenerate) {
          val currentBatchSize = minOf(batchSize, (eventsToGenerate - generated).toInt())

          // Generate batch of events
          repeat(currentBatchSize) {
            val vid = vidArray[random.nextInt(uniqueVids)]
            val randomSeconds = (random.nextDouble() * durationSecondsDouble).toLong()
            val randomTimestamp = startTime.plusSeconds(randomSeconds)

            val person = Person.newBuilder().apply {
              gender = if (random.nextDouble() < 0.6) Person.Gender.MALE else Person.Gender.FEMALE
              ageGroup = validAgeGroups[random.nextInt(validAgeGroups.size)]
            }.build()

            val event = TestEvent.newBuilder().apply {
              this.person = person
            }.build()

            send(LabeledEvent(randomTimestamp, vid, event))
          }

          generated += currentBatchSize

          if (generated % 50000 == 0L) {
            println("Thread $threadId: Generated $generated/$eventsToGenerate events")
          }
        }

        println("Thread $threadId completed: $eventsToGenerate events")
      }
    }.forEach { it.join() }

    println("Synthetic event generation completed. Total events: $totalEvents")
  }

  private fun generateVidFrequencyDistribution(
    uniqueVids: Int,
    averageFrequency: Int,
    random: SecureRandom
  ): Map<Long, Int> {
    val distribution = mutableMapOf<Long, Int>()

    // Use Zipf-like distribution for more realistic frequency patterns
    // Some VIDs appear many times, most appear few times
    for (i in 1..uniqueVids) {
      val vid = i.toLong()
      val frequency = when {
        i <= uniqueVids * 0.01 -> averageFrequency * 10  // Top 1% are power users
        i <= uniqueVids * 0.1 -> averageFrequency * 3   // Next 9% are regular users
        i <= uniqueVids * 0.5 -> averageFrequency       // Next 40% are average users
        else -> 1                                        // Remaining 50% are light users
      }
      distribution[vid] = frequency + random.nextInt(3) - 1 // Add some randomness
    }

    return distribution
  }

  private suspend fun runParallelPipeline(
    eventFlow: Flow<LabeledEvent<TestEvent>>,
    vidIndexMap: VidIndexMap,
    celFilters: Map<String, String>,
    batchSize: Int,
    workers: Int,
    customDispatcher: kotlinx.coroutines.CoroutineDispatcher,
    ioDispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Map<String, SinkStatistics> = coroutineScope {
    // Create CEL filter processors
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    val celProcessors = celFilters.map { (filterId, expression) ->
      CelFilterProcessor(filterId, expression, TestEvent.getDescriptor(), typeRegistry)
    }

    // Create frequency vector sinks
    val sinks = celFilters.map { (filterId, expression) ->
      filterId to FrequencyVectorSink(
        sinkId = filterId,
        description = "CEL: $expression",
        vidIndexMap = vidIndexMap
      )
    }.toMap()

    // Unlimited capacity channel for maximum throughput
    val batchChannel = Channel<EventBatch>(capacity = Channel.UNLIMITED)

    val totalBatches = AtomicLong(0)
    val totalEvents = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    println("Parallel pipeline configuration:")
    println("  Batch size: $batchSize")
    println("  Workers: $workers")
    println("  Channel: UNLIMITED capacity")

    // Stage 1: High-throughput batching
    val batchingJob = launch(ioDispatcher) {
      println("Parallel batching stage started...")
      var batchId = 0L
      eventFlow
        .chunked(batchSize)
        .collect { eventList ->
          batchChannel.send(EventBatch(eventList, batchId++))
          totalBatches.incrementAndGet()
          totalEvents.addAndGet(eventList.size.toLong())

          if (batchId % 10 == 0L) {
            val elapsed = System.currentTimeMillis() - startTime
            val rate = if (elapsed > 0) totalEvents.get() * 1000 / elapsed else 0
            println("Batched ${totalEvents.get()} events in ${totalBatches.get()} batches ($rate events/sec)")
          }
        }
      batchChannel.close()
      println("Parallel batching completed: ${totalEvents.get()} events in ${totalBatches.get()} batches")
    }

    // Stage 2: Massive fan-out processing (parallel workers)
    val processedEvents = AtomicLong(0)
    val processingJobs = (1..workers).map { workerId ->
      launch(customDispatcher) {
        for (batch in batchChannel) {
          // KEY OPTIMIZATION: No awaitAll() - fully async
          celProcessors.map { processor ->
            launch(customDispatcher) {
              val matchedEvents = processor.processBatch(batch)
              sinks[processor.filterId]?.processMatchedEvents(matchedEvents, batch.size)
            }
          }

          val processed = processedEvents.addAndGet(batch.size.toLong())
          if (processed % 1000000 == 0L) {
            val elapsed = System.currentTimeMillis() - startTime
            val rate = if (elapsed > 0) processed * 1000 / elapsed else 0
            println("Sink processed $processed events ($rate events/sec)")
          }
        }
      }
    }

    // Wait for completion
    batchingJob.join()
    processingJobs.forEach { it.join() }

    val endTime = System.currentTimeMillis()
    val totalTime = endTime - startTime

    println("\nParallel pipeline completed:")
    println("  Total events: ${totalEvents.get()}")
    println("  Total batches: ${totalBatches.get()}")
    println("  Processing time: ${totalTime} ms")
    println("  Throughput: ${if (totalTime > 0) totalEvents.get() * 1000 / totalTime else 0} events/sec")

    // Return statistics
    sinks.mapValues { (_, sink) -> sink.getStatistics() }
  }

  private suspend fun runSinglePipeline(
    eventFlow: Flow<LabeledEvent<TestEvent>>,
    vidIndexMap: VidIndexMap,
    celFilters: Map<String, String>,
    customDispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Map<String, SinkStatistics> = coroutineScope {
    // Create CEL filter processors
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    val celProcessors = celFilters.map { (filterId, expression) ->
      CelFilterProcessor(filterId, expression, TestEvent.getDescriptor(), typeRegistry)
    }

    // Create frequency vector sinks
    val sinks = celFilters.map { (filterId, expression) ->
      filterId to FrequencyVectorSink(
        sinkId = filterId,
        description = "CEL: $expression",
        vidIndexMap = vidIndexMap
      )
    }.toMap()

    val totalEvents = AtomicLong(0)
    val startTime = System.currentTimeMillis()

    // Single pipeline: Generate -> Filter -> Count in one flow
    eventFlow
      .onStart { println("Single pipeline started") }
      .flowOn(customDispatcher)
      .collect { event ->
        totalEvents.incrementAndGet()

        // Process event through all CEL filters immediately
        celProcessors.forEach { processor ->
          val batch = EventBatch(listOf(event), totalEvents.get())
          val matchedEvents = processor.processBatch(batch)
          val sink = sinks[processor.filterId]!!
          sink.processMatchedEvents(matchedEvents, 1)
        }

        if (totalEvents.get() % 100000 == 0L) {
          val elapsed = System.currentTimeMillis() - startTime
          val rate = if (elapsed > 0) totalEvents.get() * 1000 / elapsed else 0
          println("Processed ${totalEvents.get()} events ($rate events/sec)")
        }
      }

    val endTime = System.currentTimeMillis()
    val totalTime = endTime - startTime

    println("\nSingle pipeline completed:")
    println("  Total events: ${totalEvents.get()}")
    println("  Processing time: ${totalTime} ms")
    println("  Throughput: ${if (totalTime > 0) totalEvents.get() * 1000 / totalTime else 0} events/sec")

    // Return statistics
    sinks.mapValues { (_, sink) -> sink.getStatistics() }
  }

  private fun selectVidByFrequency(
    vidFrequencies: Map<Long, Int>,
    random: SecureRandom
  ): Long {
    // Create cumulative distribution for weighted random selection
    val totalWeight = vidFrequencies.values.sum()
    val randomValue = random.nextInt(totalWeight)

    var cumulative = 0
    for ((vid, frequency) in vidFrequencies) {
      cumulative += frequency
      if (randomValue < cumulative) {
        return vid
      }
    }

    // Fallback (should not reach here)
    return vidFrequencies.keys.first()
  }

}

