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
import com.google.protobuf.Any as ProtobufAny
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import com.google.type.interval
import java.io.FileInputStream
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
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
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.dataprovider.VidFilter
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
    names = ["--parallelism"],
    description = ["Number of parallel threads for processing"],
    defaultValue = "4"
  )
  private var parallelism: Int = 4

  override fun run() = runBlocking {
    println("Starting HardcodedImpressionsResultsFulfillerMain.run()")
    println("Using parallelism level: $parallelism")
    
    // Create a dispatcher with the specified parallelism
    val dispatcher = Dispatchers.IO.limitedParallelism(parallelism)

    // Initialize Tink
    println("Initializing Tink crypto libraries...")
    AeadConfig.register()
    StreamingAeadConfig.register()
    println("Tink initialization complete")

    println("Parsing date range: $startDateStr to $endDateStr")
    val startDate = LocalDate.parse(startDateStr)
    val endDate = LocalDate.parse(endDateStr)
    val timeRange = OpenEndTimeRange.fromClosedDateRange(startDate..endDate)
    println("Date range parsed successfully: $startDate to $endDate")

    println("Building measurement spec...")
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
    println("Measurement spec built successfully")

    println("Building requisition spec...")
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
    println("Requisition spec built successfully")

    println("Building grouped requisitions...")
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
        requisition = ProtobufAny.pack(createRequisition(requisitionSpec, timeRange))
      }
    }
    println("Grouped requisitions built successfully")

    // Set up KMS
    println("Setting up KMS client...")
    val kmsClient: FakeKmsClient? = if (masterKeyFile == null) null
    else {
      val kmsClient = FakeKmsClient()
      val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"

      val kmsKeyHandle = FileInputStream(masterKeyFile!!.toFile()).use { inputStream ->
        val keysetReader = BinaryKeysetReader.withInputStream(inputStream)
        CleartextKeysetHandle.read(keysetReader)
      }
      kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      kmsClient
    }
    if (kmsClient != null) {
      println("KMS client set up successfully")
    } else {
      println("No KMS client configured (master key file not provided)")
    }

    // Set up type registry
    println("Setting up type registry...")
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    println("Type registry configured")

    // Set up event reader
    println("Setting up event reader with impressions path: $impressionsPath")
    val impressionsFile = impressionsPath.toFile()
    val eventReader = EventReader(
      kmsClient,
      StorageConfig(rootDirectory = impressionsFile),
      StorageConfig(rootDirectory = impressionsFile),
      impressionsUri,
      typeRegistry
    )
    println("Event reader configured successfully")

    // Process requisitions
    println("Processing requisitions...")

    // Read impressions from all date partitions as parallel flows
    val sampledEvents = getSampledVids(
      requisitionSpec,
      groupedRequisitions.eventGroupMapList.associate { it.eventGroup to it.details.eventGroupReferenceId },
      eventReader,
      ZoneId.systemDefault(),
      dispatcher
    )

    // Filter impressions and extract VIDs
    val filteredVids = filterImpressions(
      sampledEvents,
      requisitionSpec,
      measurementSpec.vidSamplingInterval,
      typeRegistry,
      dispatcher
    )

    val vidCount = filteredVids.count()
    println("Filtered $vidCount VIDs from impressions")

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
   * Extracts impressions from impression data as a flow of flows (one per date partition)
   */
  private suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    eventGroupMap: Map<String, String>,
    eventReader: EventReader,
    zoneId: ZoneId,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Flow<Flow<LabeledEvent<*>>> =
    requisitionSpec.events.eventGroupsList.asFlow()
      .flatMapConcat { eventGroup ->
        val collectionInterval = eventGroup.value.collectionInterval
        val startDate = LocalDate.ofInstant(collectionInterval.startTime.toInstant(), zoneId)
        val endDate = LocalDate.ofInstant(collectionInterval.endTime.toInstant(), zoneId)

        val datesList = generateSequence(startDate) { date ->
          if (date < endDate) date.plusDays(1) else null
        }.toList()

        println("Processing ${datesList.size} dates from $startDate to ${datesList.lastOrNull() ?: endDate}")

        // Create flows for each date partition
        datesList.asFlow()
          .map { date ->
            eventReader.getLabeledEvents(date, eventGroupMap.getValue(eventGroup.key))
              .flowOn(dispatcher)
          }
      }



  /**
   * Filters impressions and extracts VIDs using flows
   */
  private suspend fun filterImpressions(
    impressionFlows: Flow<Flow<LabeledEvent<*>>>,
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    dispatcher: kotlinx.coroutines.CoroutineDispatcher
  ): Flow<Long> =
    requisitionSpec.events.eventGroupsList.asFlow()
      .flatMapConcat { eventGroup ->
        val collectionInterval = eventGroup.value.collectionInterval
        val vidSamplingIntervalStart = vidSamplingInterval.start
        val vidSamplingIntervalWidth = vidSamplingInterval.width

        // Process each date partition flow in parallel
        impressionFlows
          .flatMapMerge(concurrency = parallelism) { dateFlow ->
            @Suppress("UNCHECKED_CAST")
            VidFilter.filterAndExtractVids(
              dateFlow as Flow<LabeledEvent<Message>>,
              vidSamplingIntervalStart,
              vidSamplingIntervalWidth,
              eventGroup.value.filter,
              collectionInterval,
              typeRegistry
            ).flowOn(dispatcher)
          }
      }

}
