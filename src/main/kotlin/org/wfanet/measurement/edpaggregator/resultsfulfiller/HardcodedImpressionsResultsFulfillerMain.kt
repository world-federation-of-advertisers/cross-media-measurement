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
import kotlin.ranges.coerceAtLeast
import kotlin.ranges.coerceAtMost
import kotlin.ranges.rangeTo
import kotlin.ranges.until
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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

  data class BenchmarkResult(
    val parallelism: Int,
    val vidCount: Long,
    val durationMs: Long,
    val vidsPerSecond: Long
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

  @CommandLine.Option(
    names = ["--enable-filtering"],
    description = ["Enable VidFilter for filtering impressions"],
    defaultValue = "false"
  )
  private var enableFiltering: Boolean = false
  override fun run() = runBlocking {
    // Initialize Tink
    AeadConfig.register()
    StreamingAeadConfig.register()

    // Force Dispatchers.Default to use exactly the max parallelism threads
    System.setProperty("kotlinx.coroutines.scheduler.core.pool.size", "64")
    System.setProperty("kotlinx.coroutines.scheduler.max.pool.size", "256")

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
    println("\n=== Parallelism Benchmark (Filtering: ${if (enableFiltering) "ENABLED" else "DISABLED"}) ===\n")

    // Benchmark different parallelism levels
    val parallelismLevels = listOf(1, 8)
    val benchmarkResults = mutableMapOf<Int, BenchmarkResult>()

    // Print thread pool info
    println("Available processors: ${Runtime.getRuntime().availableProcessors()}")
    println("Coroutine thread pool size: ${System.getProperty("kotlinx.coroutines.scheduler.core.pool.size")}")

    for (parallelism in parallelismLevels) {
      println("Testing with parallelism = $parallelism...")

      // Measure time to read event data only
      val readStartTime = System.currentTimeMillis()

      val sampledVids = getSampledVids(
        requisitionSpec,
        groupedRequisitions.eventGroupMapList.associate { it.eventGroup to it.details.eventGroupReferenceId },
        measurementSpec.vidSamplingInterval,
        typeRegistry,
        eventReader,
        ZoneId.systemDefault(),
        parallelism,
        enableFiltering
      )

      // Collect all VIDs to force reading from disk
      val vidCount = sampledVids.count().toLong()

      val readEndTime = System.currentTimeMillis()
      val readDuration = readEndTime - readStartTime

      benchmarkResults[parallelism] = BenchmarkResult(
        parallelism = parallelism,
        vidCount = vidCount,
        durationMs = readDuration,
        vidsPerSecond = if (readDuration > 0) (vidCount * 1000.0 / readDuration).toLong() else 0
      )

      println("  Completed in ${readDuration}ms")
      println()
    }

    // Print comparison table
    println("\n=== Benchmark Results (Filtering: ${if (enableFiltering) "ENABLED" else "DISABLED"}) ===")
    println("%-12s %-15s %-15s %-15s %-15s".format("Parallelism", "Time (ms)", "Time (s)", "VIDs/second", "Speedup"))
    println("-".repeat(80))

    val baselineTime = benchmarkResults[1]?.durationMs ?: 1
    for ((parallelism, result) in benchmarkResults.entries.sortedBy { it.key }) {
      val speedup = baselineTime.toDouble() / result.durationMs
      println("%-12d %-15d %-15.2f %-15d %-15.2fx".format(
        parallelism,
        result.durationMs,
        result.durationMs / 1000.0,
        result.vidsPerSecond,
        speedup
      ))
    }

    println("\nTotal VIDs processed: ${benchmarkResults[1]?.vidCount ?: 0}")
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
    private const val EDP_DISPLAY_NAME = "edp1"

    private val SECRET_FILES_PATH: Path =
      Paths.get(System.getProperty("user.dir"), "src", "main", "k8s", "testing", "secretfiles")

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
   * Processes requisitions and builds measurement results directly
   */
  private suspend fun processRequisitions(
    groupedRequisitions: GroupedRequisitions,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    eventReader: EventReader,
    typeRegistry: TypeRegistry,
  ): Measurement.Result {
    // Extract VIDs from impressions
    val sampledVids = getSampledVids(
      requisitionSpec,
      groupedRequisitions.eventGroupMapList.associate { it.eventGroup to it.details.eventGroupReferenceId },
      measurementSpec.vidSamplingInterval,
      typeRegistry,
      eventReader,
      ZoneId.systemDefault(),
      parallelism = 8,
      enableFiltering = enableFiltering
    )

    // Compute reach and frequency
    val maxFrequency = measurementSpec.reachAndFrequency.maximumFrequency
    val reachAndFrequency = computeReachAndFrequency(sampledVids, maxFrequency)

    // Build the measurement result
    return Measurement.Result.newBuilder().apply {
      reach = Measurement.Result.Reach.newBuilder().apply {
        value = reachAndFrequency.reach.toLong()
      }.build()

      frequency = Measurement.Result.Frequency.newBuilder().apply {
        putAllRelativeFrequencyDistribution(
          reachAndFrequency.relativeFrequencyDistribution.mapKeys { it.key.toLong() }
        )
      }.build()
    }.build()
  }

  /**
   * Extracts and samples VIDs from impression data
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  private suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    eventGroupMap: Map<String, String>,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    eventReader: EventReader,
    zoneId: ZoneId,
    parallelism: Int = 8,
    enableFiltering: Boolean = false
  ): Flow<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width

    return requisitionSpec.events.eventGroupsList.asFlow().flatMapConcat { eventGroup ->
      val collectionInterval = eventGroup.value.collectionInterval
      val startDate = LocalDate.ofInstant(collectionInterval.startTime.toInstant(), zoneId)
      val endDate = LocalDate.ofInstant(collectionInterval.endTime.toInstant(), zoneId)

      val datesList = generateSequence(startDate) { date ->
        if (date < endDate) date.plusDays(1) else null
      }.toList()

      println("Processing ${datesList.size} dates from $startDate to ${datesList.lastOrNull() ?: endDate} with parallelism=$parallelism")

      // Process dates in parallel
      datesList.asFlow()
        .flatMapMerge(parallelism) { date ->
          flow {
            val impressionsFlow = eventReader.getLabeledImpressions(date, eventGroupMap.getValue(eventGroup.key))
            val filteredImpressions = if (enableFiltering) VidFilter.filterAndExtractVids(
              impressionsFlow,
              vidSamplingIntervalStart,
              vidSamplingIntervalWidth,
              eventGroup.value.filter,
              collectionInterval,
              typeRegistry
            ) else impressionsFlow.map { i -> i.vid }
            emitAll(filteredImpressions)
          }
        }
    }
  }


  /**
   * Computes reach and frequency using deterministic count distinct methodology
   */
  private suspend fun computeReachAndFrequency(
    filteredVids: Flow<Long>,
    maxFrequency: Int
  ): ReachAndFrequency {
    val startTime = System.currentTimeMillis()

    // Drain the flow entirely into a list first
    val vidsList = filteredVids.toList()
    println("Profile: Flow draining took ${System.currentTimeMillis() - startTime}ms for ${vidsList.size} total VIDs")

    // Count occurrences of each VID
    val vidToIndex = mutableMapOf<Long, Int>()
    var nextIndex = 0

    // Build VID to index mapping
    for (vid in vidsList) {
      if (!vidToIndex.containsKey(vid)) {
        vidToIndex[vid] = nextIndex++
      }
    }

    // Create atomic integer array for counts
    val countsArray = AtomicIntegerArray(vidToIndex.size)

    // Count occurrences using atomic operations in parallel
    // Use a fixed thread pool with exactly the number of available processors
    val countingDispatcher = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()) { runnable ->
      Thread(runnable).apply {
        name = "VidCounter-${Thread.currentThread().id}"
        isDaemon = true
      }
    }.asCoroutineDispatcher()

    coroutineScope {
      val chunkSize = (vidsList.size / Runtime.getRuntime().availableProcessors()).coerceAtLeast(1000)
      vidsList
        .chunked(chunkSize)
        .map { chunk ->
          async(countingDispatcher) {
            for (vid in chunk) {
              val index = vidToIndex[vid]!!
              countsArray.incrementAndGet(index)
            }
          }
        }
        .awaitAll()
    }

    countingDispatcher.close()

    val reach: Int = vidToIndex.size

    // If empty, return zero distribution
    if (reach == 0) {
      return ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency histogram
    val frequencyArray = IntArray(maxFrequency)
    for (i in 0 until countsArray.length()) {
      val count = countsArray.get(i)
      val bucket = count.coerceAtMost(maxFrequency)
      frequencyArray[bucket - 1]++
    }

    // Calculate relative frequency distribution
    val frequencyDistribution: Map<Int, Double> =
      frequencyArray.withIndex().associateBy({ it.index + 1 }, { it.value.toDouble() / reach })

    println("Profile: computeReachAndFrequency total took ${System.currentTimeMillis() - startTime}ms (reach: $reach, maxFreq: $maxFrequency)")

    return ReachAndFrequency(reach, frequencyDistribution)
  }
}
