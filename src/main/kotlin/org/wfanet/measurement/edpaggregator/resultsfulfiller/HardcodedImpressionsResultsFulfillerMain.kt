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
import java.nio.file.Path
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
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
 * SIMPLIFIED DEMO PIPELINE: CEL-ONLY FILTERING WITH BATCHED PROCESSING
 * 
 * This implementation demonstrates a streamlined event processing pipeline with the following architecture:
 * 
 * PIPELINE STRUCTURE:
 * [Parallel Event Readers] -> [Batching] -> [Fan Out] -> [CEL Filtering] -> [Frequency Vector Sinks]
 * 
 * KEY DESIGN PRINCIPLES:
 * 1. **Simplified Architecture**: Only CEL filtering, no complex masking operations
 * 2. **Batched Processing**: Events are processed in configurable batch sizes
 * 3. **Fan-Out to Multiple CEL Filters**: Each batch is processed by multiple CEL filter coroutines
 * 4. **Memory-Efficient Storage**: Striped byte arrays for frequency vectors
 * 5. **Demonstration Focus**: Two CEL filters (person.gender == 0 and person.gender == 1)
 * 
 * PERFORMANCE CHARACTERISTICS:
 * - Parallel event reading from multiple date partitions
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
            var batchId = 0L
            eventFlow
                .chunked(batchSize)
                .collect { eventList ->
                    val batch = EventBatch(eventList, batchId++)
                    batchChannel.send(batch)
                    totalBatches.incrementAndGet()
                    totalEvents.addAndGet(eventList.size.toLong())
                    
                    if (batchId % 100 == 0L) {
                        val elapsed = System.currentTimeMillis() - startTime
                        val eventsPerSec = if (elapsed > 0) totalEvents.get() * 1000 / elapsed else 0
                        println("Processed ${totalEvents.get()} events in ${totalBatches.get()} batches ($eventsPerSec events/sec)")
                    }
                }
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
 * Enhanced command-line tool demonstrating the simplified CEL filtering pipeline.
 */
@CommandLine.Command(
  name = "SimplifiedEventProcessingPipeline",
  description = ["Simplified pipeline: Parallel Event Readers -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class HardcodedImpressionsResultsFulfillerMain : Runnable {

  @CommandLine.Option(
    names = ["--impressions-path"],
    description = ["Path to impressions data directory"],
    required = true
  )
  private lateinit var impressionsPath: Path


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
    names = ["--event-group-reference-id"],
    description = ["Event group reference ID"],
    defaultValue = "edpa-eg-reference-id-1"
  )
  private lateinit var eventGroupReferenceId: String

  @CommandLine.Option(
    names = ["--impressions-uri"],
    description = ["URI for impressions data"],
    defaultValue = "file://storage/impressions"
  )
  private lateinit var impressionsUri: String

  @CommandLine.Option(
    names = ["--parallelism"],
    description = ["Number of parallel threads for processing"],
    defaultValue = "4"
  )
  private var parallelism: Int = 4

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
    defaultValue = "100000000"
  )
  private var maxVidRange: Long = 100_000_000L

  @CommandLine.Option(
    names = ["--cel-filter-1"],
    description = ["First CEL filter expression"],
    defaultValue = "person.gender == 0"
  )
  private lateinit var celFilter1: String

  @CommandLine.Option(
    names = ["--cel-filter-2"],
    description = ["Second CEL filter expression"],
    defaultValue = "person.gender == 1"
  )
  private lateinit var celFilter2: String

  override fun run() = runBlocking {
    println("Simplified Event Processing Pipeline")
    println("Structure: Parallel Event Readers -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks")
    println("CEL Filters: '$celFilter1', '$celFilter2'")
    println()

    // Initialize Tink crypto
    AeadConfig.register()
    StreamingAeadConfig.register()
    println("Tink initialization complete")

    // Parse configuration
    val startDate = LocalDate.parse(startDateStr)
    val endDate = LocalDate.parse(endDateStr)
    val timeRange = OpenEndTimeRange.fromClosedDateRange(startDate..endDate)
    
    println("Configuration:")
    println("  Date range: $startDate to $endDate")
    println("  Max VID range: $maxVidRange")
    println("  Parallelism: $parallelism")
    println("  Batch size: $batchSize")
    println("  Channel capacity: $channelCapacity")
    println()

    // Create population spec and VID index map
    println("Creating PopulationSpec and VID index map...")
    val populationSpec = createPopulationSpec(maxVidRange)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)
    println("VID index map created with ${vidIndexMap.size} entries")
    println()

    // Set up event infrastructure
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    val impressionsFile = impressionsPath.toFile()
    val eventReader = EventReader(
      null,
      StorageConfig(rootDirectory = impressionsFile),
      StorageConfig(rootDirectory = impressionsFile),
      impressionsUri,
      typeRegistry
    )

    // Define CEL filters for demo
    val celFilters = mapOf(
      "filter_1" to celFilter1,
      "filter_2" to celFilter2
    )

    // Create parallel event flow
    println("Creating parallel event reader flow...")
    val eventFlow = createParallelEventFlow(timeRange, eventReader)

    // Process through simplified pipeline
    println("Processing through simplified CEL-only pipeline...")
    val pipeline = SimplifiedEventProcessingPipeline()
    
    val sinkStatistics = pipeline.processEvents(
      eventFlow = eventFlow,
      batchSize = batchSize,
      channelCapacity = channelCapacity,
      vidIndexMap = vidIndexMap,
      celFilters = celFilters,
      eventMessageDescriptor = TestEvent.getDescriptor(),
      typeRegistry = typeRegistry
    )

    // Display results
    println("SIMPLIFIED PIPELINE RESULTS:")
    println("=".repeat(60))
    sinkStatistics.forEach { (filterId, stats) ->
      println("Filter: $filterId")
      println("  Description: ${stats.description}")
      println("  Events: ${stats.processedEvents} processed, ${stats.matchedEvents} matched (${String.format("%.2f", stats.matchRate)}%)")
      println("  Reach: ${stats.reach}")
      println("  Total Frequency: ${stats.totalFrequency}")
      println("  Average Frequency: ${String.format("%.2f", stats.averageFrequency)}")
      println("  Errors: ${stats.errorCount} (${String.format("%.2f", stats.errorRate)}%)")
      println()
    }

    // Comparison between filters
    if (sinkStatistics.size >= 2) {
      val stats1 = sinkStatistics["filter_1"]!!
      val stats2 = sinkStatistics["filter_2"]!!
      
      println("FILTER COMPARISON:")
      println("  Filter 1 vs Filter 2 Reach Ratio: ${if (stats2.reach > 0) "%.2f".format(stats1.reach.toDouble() / stats2.reach) else "N/A"}")
      println("  Filter 1 vs Filter 2 Match Rate Ratio: ${if (stats2.matchRate > 0) "%.2f".format(stats1.matchRate / stats2.matchRate) else "N/A"}")
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

  private suspend fun createParallelEventFlow(
    timeRange: OpenEndTimeRange,
    eventReader: EventReader
  ): Flow<LabeledEvent<TestEvent>> {
    val startDate = LocalDate.ofInstant(timeRange.start, ZoneId.systemDefault())
    val endDate = LocalDate.ofInstant(timeRange.endExclusive, ZoneId.systemDefault())
    
    val datesList = generateSequence(startDate) { date ->
      if (date < endDate) date.plusDays(1) else null
    }.toList()
    
    println("Setting up parallel event readers for ${datesList.size} date partitions")
    
    // Parallel event readers with controlled concurrency
    return datesList.asFlow()
      .flatMapMerge(concurrency = parallelism) { date ->
        eventReader.getLabeledEvents(date, eventGroupReferenceId)
          .flowOn(Dispatchers.IO)
      }
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(HardcodedImpressionsResultsFulfillerMain(), args)
  }
}

/**
 * Placeholder EventReader for demonstration purposes.
 */
class EventReader(
  private val kmsClient: FakeKmsClient?,
  private val storageConfig: StorageConfig,
  private val backupStorageConfig: StorageConfig,
  private val impressionsUri: String,
  private val typeRegistry: TypeRegistry
) {
  fun getLabeledEvents(date: LocalDate, eventGroupReferenceId: String): Flow<LabeledEvent<TestEvent>> {
    // Implementation would read from storage, decrypt, and deserialize events
    return emptyFlow<LabeledEvent<TestEvent>>()
  }
}
