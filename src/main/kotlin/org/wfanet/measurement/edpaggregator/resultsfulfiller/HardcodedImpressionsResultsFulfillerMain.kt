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

// import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import com.google.crypto.tink.*
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.io.FileInputStream
import java.lang.Runnable
import java.lang.Runtime
import java.lang.System
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.Future
import java.util.concurrent.Callable
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.ranges.coerceAtLeast
import kotlin.ranges.coerceAtMost
import kotlin.ranges.rangeTo
import kotlin.ranges.until
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.asFlow
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
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
import org.wfanet.sampling.VidSampler
import picocli.CommandLine

/**
 * Command-line tool for running hardcoded impressions results fulfiller.
 *
 * This tool creates a test requisition with hardcoded data and processes it
 * through the ResultsFulfiller without needing a full gRPC service setup.
 */
@CommandLine.Command(
  name = "HardcodedImpressionsResultsFulfiller",
  description = ["Process hardcoded impressions and generate measurement results"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class HardcodedImpressionsResultsFulfillerMain : Runnable {

  data class FrequencyVector(
    val counts: Map<Long, Int>
  )

  data class BenchmarkResult(
    val parallelism: Int,
    val impressionCount: Long,
    val vidCount: Long,
    val readDurationMs: Long,
    val filterDurationMs: Long,
    val frequencyDurationMs: Long,
    val mergeDurationMs: Long,
    val distributionDurationMs: Long,
    val totalDurationMs: Long,
    val readThroughput: Long,
    val filterThroughput: Long,
    val frequencyThroughput: Long,
    val mergeThroughput: Long,
    val distributionThroughput: Long,
    val totalThroughput: Long
  )

  @CommandLine.Option(
    names = ["--impressions-path"],
    description = ["Path to impressions data directory"],
    required = true
  )
  private lateinit var impressionsPath: Path

  @CommandLine.Option(
    names = ["--master-key-file"],
    description = ["Path to master key file"],
    required = false
  )
  private var masterKeyFile: Path? = null

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

  override fun run() {
    // Initialize Tink
    AeadConfig.register()
    StreamingAeadConfig.register()

    // Create thread pool executor for parallel processing
    val corePoolSize = Runtime.getRuntime().availableProcessors()
    val maximumPoolSize = corePoolSize * 4

    val startDate = LocalDate.parse(startDateStr)
    val endDate = LocalDate.parse(endDateStr)
    val timeRange = OpenEndTimeRange.fromClosedDateRange(startDate..endDate)
    val measurementSpec = measurementSpec {
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
    }
    val requisitionSpec = requisitionSpec {
      events = events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value = RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = interval {
              startTime = timeRange.start.toProtoTime()
              endTime = timeRange.endExclusive.toProtoTime()
            }
            filter = eventFilter { expression = "person.gender==1" }
          }
        }
      }
      nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
    }


    // Write requisitions proto to storage
    val groupedRequisitions = groupedRequisitions {
      modelLine = "test-model-line"
      eventGroupMap += eventGroupMapEntry {
        eventGroup = EVENT_GROUP_NAME
        details = eventGroupDetails {
          this.eventGroupReferenceId = this@HardcodedImpressionsResultsFulfillerMain.eventGroupReferenceId
          collectionIntervals += interval {
            startTime = timeRange.start.toProtoTime()
            endTime = timeRange.endExclusive.toProtoTime()
          }
        }
      }
      requisitions += requisitionEntry {
        requisition = Any.pack(createRequisition(requisitionSpec, timeRange))
      }
    }

    // Set up KMS
    val kmsClient: FakeKmsClient? = if (masterKeyFile == null) null
    else {
      val kmsClient = FakeKmsClient()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"

      // Load master key from file
      val kmsKeyHandle = FileInputStream(masterKeyFile!!.toFile()).use { inputStream ->
        val keysetReader = BinaryKeysetReader.withInputStream(inputStream)
        CleartextKeysetHandle.read(keysetReader)
      }
      kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      kmsClient
    }

    // Set up type registry
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    // Set up event reader
    val impressionsFile = impressionsPath.toFile()
    val eventReader = EventReader(
      kmsClient,
      StorageConfig(rootDirectory = impressionsFile),
      StorageConfig(rootDirectory = impressionsFile),
      impressionsUri
    )

    // Process requisitions directly without ResultsFulfiller dependencies
    println("Processing requisitions...")
    println("\n=== Parallelism Benchmark ===\n")

    // Benchmark different parallelism levels
    val parallelismLevels = listOf(1, 2, 4, 8)
    val benchmarkResults = mutableMapOf<Int, BenchmarkResult>()

    // Print thread pool info
    println("Available processors: ${Runtime.getRuntime().availableProcessors()}")
    println("Thread pool core size: $corePoolSize")
    println("Thread pool max size: $maximumPoolSize")

    for (parallelism in parallelismLevels) {
      println("Testing with parallelism = $parallelism...")

      // Measure time to read impressions only
      val readStartTime = System.currentTimeMillis()

      val impressions = getSampledVids(
        requisitionSpec,
        groupedRequisitions.eventGroupMapList.associate { it.eventGroup to it.details.eventGroupReferenceId },
        eventReader,
        ZoneId.systemDefault(),
        parallelism
      )

      // Collect all impressions to force reading from disk
      val impressionCount = impressions.sumOf { it.size }.toLong()

      val readEndTime = System.currentTimeMillis()
      val readDuration = readEndTime - readStartTime

      println("  Read stage completed in ${readDuration}ms (${impressionCount} impressions)")

      // Measure time to filter impressions
      val filterStartTime = System.currentTimeMillis()

      val allVids = filterImpressions(
        impressions,
        requisitionSpec,
        measurementSpec.vidSamplingInterval,
        typeRegistry,
        parallelism
      )

      // All VIDs are already collected
      val totalVidCount = allVids.size.toLong()

      val filterEndTime = System.currentTimeMillis()
      val filterDuration = filterEndTime - filterStartTime

      println("  Filter stage completed in ${filterDuration}ms (${totalVidCount} VIDs)")

      // Group VIDs by dates for frequency vector stage
      val vidsPerDate = mutableListOf<List<Long>>()
      val datesCount = impressions.size
      val vidsPerDateCount = if (datesCount > 0) (totalVidCount / datesCount).toInt() else 0
      repeat(datesCount) { dateIndex ->
        val startIdx = dateIndex * vidsPerDateCount
        val endIdx = if (dateIndex == datesCount - 1) allVids.size else (dateIndex + 1) * vidsPerDateCount
        if (startIdx < allVids.size) {
          vidsPerDate.add(allVids.subList(startIdx, minOf(endIdx, allVids.size)))
        }
      }

      // Measure time to compute frequency vectors
      val frequencyStartTime = System.currentTimeMillis()

      val vectorList = computeFrequencyVectors(vidsPerDate, parallelism)
      val vectorCount = vectorList.size.toLong()

      val frequencyEndTime = System.currentTimeMillis()
      val frequencyDuration = frequencyEndTime - frequencyStartTime

      println("  Frequency stage completed in ${frequencyDuration}ms (${vectorCount} vectors)")

      // Measure time to merge frequency vectors
      val mergeStartTime = System.currentTimeMillis()

      val mergedVector = mergeFrequencyVectors(vectorList)

      val mergeEndTime = System.currentTimeMillis()
      val mergeDuration = mergeEndTime - mergeStartTime

      println("  Merge stage completed in ${mergeDuration}ms (${mergedVector.counts.size} unique VIDs)")

      // Measure time to compute frequency distribution
      val distributionStartTime = System.currentTimeMillis()

      val averageFrequency = computeFrequencyDistribution(mergedVector)

      val distributionEndTime = System.currentTimeMillis()
      val distributionDuration = distributionEndTime - distributionStartTime
      val totalDuration = readDuration + filterDuration + frequencyDuration + mergeDuration + distributionDuration

      println("  Distribution stage completed in ${distributionDuration}ms (average frequency: ${String.format("%.2f", averageFrequency)})")

      benchmarkResults[parallelism] = BenchmarkResult(
        parallelism = parallelism,
        impressionCount = impressionCount,
        vidCount = totalVidCount,
        readDurationMs = readDuration,
        filterDurationMs = filterDuration,
        frequencyDurationMs = frequencyDuration,
        mergeDurationMs = mergeDuration,
        distributionDurationMs = distributionDuration,
        totalDurationMs = totalDuration,
        readThroughput = if (readDuration > 0) (impressionCount * 1000.0 / readDuration).toLong() else 0,
        filterThroughput = if (filterDuration > 0) (impressionCount * 1000.0 / filterDuration).toLong() else 0,
        frequencyThroughput = if (frequencyDuration > 0) (impressionCount * 1000.0 / frequencyDuration).toLong() else 0,
        mergeThroughput = if (mergeDuration > 0) (impressionCount * 1000.0 / mergeDuration).toLong() else 0,
        distributionThroughput = if (distributionDuration > 0) (impressionCount * 1000.0 / distributionDuration).toLong() else 0,
        totalThroughput = if (totalDuration > 0) (impressionCount * 1000.0 / totalDuration).toLong() else 0
      )

      println("  Total completed in ${totalDuration}ms")
      println()
    }

    // Print comparison table
    println("\n=== Benchmark Results ===")
    println("%-12s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s %-15s".format("Parallelism", "Read (ms)", "Filter (ms)", "Freq (ms)", "Merge (ms)", "Dist (ms)", "Total (ms)", "Read/sec", "Filter/sec", "Freq/sec", "Merge/sec", "Dist/sec", "Impressions/sec"))
    println("-".repeat(190))

    val baselineReadTime = benchmarkResults[1]?.readDurationMs ?: 1
    val baselineFilterTime = benchmarkResults[1]?.filterDurationMs ?: 1
    val baselineFrequencyTime = benchmarkResults[1]?.frequencyDurationMs ?: 1
    val baselineMergeTime = benchmarkResults[1]?.mergeDurationMs ?: 1
    val baselineDistributionTime = benchmarkResults[1]?.distributionDurationMs ?: 1
    val baselineTotalTime = benchmarkResults[1]?.totalDurationMs ?: 1

    for ((parallelism, result) in benchmarkResults.entries.sortedBy { it.key }) {
      println("%-12d %-15d %-15d %-15d %-15d %-15d %-15d %-15d %-15d %-15d %-15d %-15d %-15d".format(
        parallelism,
        result.readDurationMs,
        result.filterDurationMs,
        result.frequencyDurationMs,
        result.mergeDurationMs,
        result.distributionDurationMs,
        result.totalDurationMs,
        result.readThroughput,
        result.filterThroughput,
        result.frequencyThroughput,
        result.mergeThroughput,
        result.distributionThroughput,
        result.totalThroughput
      ))
    }

    println("\n=== Speedup Analysis ===")
    println("%-12s %-15s %-15s %-15s %-15s %-15s %-15s".format("Parallelism", "Read Speedup", "Filter Speedup", "Freq Speedup", "Merge Speedup", "Dist Speedup", "Total Speedup"))
    println("-".repeat(120))

    for ((parallelism, result) in benchmarkResults.entries.sortedBy { it.key }) {
      val readSpeedup = baselineReadTime.toDouble() / result.readDurationMs
      val filterSpeedup = baselineFilterTime.toDouble() / result.filterDurationMs
      val frequencySpeedup = baselineFrequencyTime.toDouble() / result.frequencyDurationMs
      val mergeSpeedup = baselineMergeTime.toDouble() / result.mergeDurationMs
      val distributionSpeedup = baselineDistributionTime.toDouble() / result.distributionDurationMs
      val totalSpeedup = baselineTotalTime.toDouble() / result.totalDurationMs
      println("%-12d %-15.2fx %-15.2fx %-15.2fx %-15.2fx %-15.2fx %-15.2fx".format(
        parallelism,
        readSpeedup,
        filterSpeedup,
        frequencySpeedup,
        mergeSpeedup,
        distributionSpeedup,
        totalSpeedup
      ))
    }

    println("\nTotal impressions processed: ${benchmarkResults[1]?.impressionCount ?: 0}")
    println("Total VIDs processed: ${benchmarkResults[1]?.vidCount ?: 0}")
    println("Done!")
  }

  private fun createRequisition(requisitionSpec: RequisitionSpec, timeRange: OpenEndTimeRange): Requisition {
    return requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol {
          direct = ProtocolConfigKt.direct {
            noiseMechanisms += listOf(
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
              ProtocolConfig.NoiseMechanism.NONE,
            )
            deterministicCountDistinct =
              ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
            deterministicDistribution =
              ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
          }
        }
      }
    }
  }

  companion object {
    private val RANDOM = SecureRandom.getInstance("SHA1PRNG").apply {
      setSeed(byteArrayOf(1, 1, 1, 1, 1, 1, 1, 1))
    }

    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(HardcodedImpressionsResultsFulfillerMain(), args)
  }

  /**
   * Data class for reach and frequency results
   */
  data class ReachAndFrequency(val reach: Int, val relativeFrequencyDistribution: Map<Int, Double>)

  /**
   * Extracts impressions from impression data (reading only), grouped by dates
   */
  private fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    eventGroupMap: Map<String, String>,
    eventReader: EventReader,
    zoneId: ZoneId,
    parallelism: Int = 8
  ): List<List<LabeledImpression>> {
    val allDateImpressions = mutableListOf<List<LabeledImpression>>()

    for (eventGroup in requisitionSpec.events.eventGroupsList) {
      val collectionInterval = eventGroup.value.collectionInterval
      val startDate = LocalDate.ofInstant(collectionInterval.startTime.toInstant(), zoneId)
      val endDate = LocalDate.ofInstant(collectionInterval.endTime.toInstant(), zoneId)

      val datesList = generateSequence(startDate) { date ->
        if (date < endDate) date.plusDays(1) else null
      }.toList()

      println("Processing ${datesList.size} dates from $startDate to ${datesList.lastOrNull() ?: endDate} with parallelism=$parallelism")

      // Create thread pool for this batch of dates
      val executor = ThreadPoolExecutor(
        parallelism,
        parallelism,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>()
      )

      try {
        // Process dates in parallel using thread pool
        val futures = datesList.map { date ->
          executor.submit(Callable<List<LabeledImpression>> {
            // Each thread runs its own coroutine context
            runBlocking {
              val impressionsFlow = eventReader.getLabeledImpressions(date, eventGroupMap.getValue(eventGroup.key))
              impressionsFlow.toList()
            }
          })
        }

        // Collect results from all futures
        val dateImpressions = futures.map { future ->
          future.get() // This blocks until the task completes
        }

        allDateImpressions.addAll(dateImpressions)
      } finally {
        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.MINUTES)
      }
    }

    return allDateImpressions
  }


  /**
   * Computes frequency vector for a list of VIDs without concurrency
   */
  private fun computeFrequencyVector(vids: List<Long>): FrequencyVector {
    val counts = mutableMapOf<Long, Int>()

    // Count occurrences of each VID
    for (vid in vids) {
      counts[vid] = counts.getOrDefault(vid, 0) + 1
    }

    return FrequencyVector(counts)
  }

  /**
   * Filters impressions and extracts VIDs, processing dates in parallel
   */
  private fun filterImpressions(
    impressionsByDate: List<List<LabeledImpression>>,
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    parallelism: Int = 8
  ): List<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width
    val allVids = ConcurrentLinkedQueue<Long>()

    for (eventGroup in requisitionSpec.events.eventGroupsList) {
      val collectionInterval = eventGroup.value.collectionInterval
      
      // Create thread pool for filtering
      val executor = ThreadPoolExecutor(
        parallelism,
        parallelism,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>()
      )

      try {
        // Process each date's impressions in parallel
        val futures = impressionsByDate.map { dateImpressions ->
          executor.submit(Callable<List<Long>> {
            runBlocking {
              VidFilter.filterAndExtractVids(
                dateImpressions.asFlow(),
                vidSamplingIntervalStart,
                vidSamplingIntervalWidth,
                eventGroup.value.filter,
                collectionInterval,
                typeRegistry
              ).toList()
            }
          })
        }

        // Collect results from all futures
        futures.forEach { future ->
          allVids.addAll(future.get())
        }
      } finally {
        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.MINUTES)
      }
    }

    return allVids.toList()
  }

  /**
   * Computes frequency vectors for each date in parallel
   */
  private fun computeFrequencyVectors(
    vidsPerDate: List<List<Long>>,
    parallelism: Int = 8
  ): List<FrequencyVector> {
    // Create thread pool for frequency computation
    val executor = ThreadPoolExecutor(
      parallelism,
      parallelism,
      60L,
      TimeUnit.SECONDS,
      LinkedBlockingQueue<Runnable>()
    )

    try {
      // Process each date's VIDs in parallel
      val futures = vidsPerDate.map { vids ->
        executor.submit(Callable<FrequencyVector> {
          computeFrequencyVector(vids)
        })
      }

      // Collect results from all futures
      return futures.map { future ->
        future.get() // This blocks until the task completes
      }
    } finally {
      executor.shutdown()
      executor.awaitTermination(5, TimeUnit.MINUTES)
    }
  }

  /**
   * Merges frequency vectors across dates by combining counts for same VIDs
   */
  private fun mergeFrequencyVectors(frequencyVectors: List<FrequencyVector>): FrequencyVector {
    val mergedCounts = mutableMapOf<Long, Int>()

    // Combine counts for all VIDs across all vectors
    for (vector in frequencyVectors) {
      for ((vid, count) in vector.counts) {
        mergedCounts[vid] = mergedCounts.getOrDefault(vid, 0) + count
      }
    }

    return FrequencyVector(mergedCounts)
  }

  /**
   * Computes frequency distribution from a frequency vector (average frequency)
   */
  private fun computeFrequencyDistribution(frequencyVector: FrequencyVector): Double {
    if (frequencyVector.counts.isEmpty()) {
      return 0.0
    }

    val totalFrequency = frequencyVector.counts.values.sum()
    val uniqueVids = frequencyVector.counts.size

    return totalFrequency.toDouble() / uniqueVids
  }
}
