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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import com.google.protobuf.Any
import com.google.type.interval
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.resultsfulfiller.PipelineConfiguration
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageImpressionMetadataService
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import picocli.CommandLine
import picocli.CommandLine.Option

/**
 * Performance test for [ResultsFulfiller] that processes requisitions without actually fulfilling
 * them.
 *
 * This utility creates test requisitions and runs them through the ResultsFulfiller pipeline using
 * a [NoOpFulfillerSelector] to measure performance without making actual RPC calls.
 *
 * Uses the same event group setup as EdpAggregatorCorrectnessTest for realistic testing.
 */
@CommandLine.Command(name = "results_fulfiller_performance_test")
class ResultsFulfillerPerformanceTest : Runnable {

  @CommandLine.Option(
    names = ["--impressions-storage-root"],
    description = ["Root directory for impressions storage"],
    required = true,
  )
  private lateinit var impressionsStorageRoot: String

  @CommandLine.Option(
    names = ["--impressions-metadata-storage-root"],
    description = ["Root directory for impressions metadata storage"],
    required = true,
  )
  private lateinit var impressionsMetadataStorageRoot: String

  @CommandLine.Option(
    names = ["--batch-size"],
    description = ["Pipeline batch size"],
    defaultValue = "256",
  )
  private var batchSize: Int = 256

  @CommandLine.Option(
    names = ["--channel-capacity"],
    description = ["Pipeline channel capacity"],
    defaultValue = "128",
  )
  private var channelCapacity: Int = 128

  @CommandLine.Option(
    names = ["--thread-pool-size"],
    description = ["Pipeline thread pool size"],
    defaultValue = "-1",
  )
  private var threadPoolSize: Int = -1

  @CommandLine.Option(
    names = ["--workers"],
    description = ["Number of pipeline workers"],
    defaultValue = "-1",
  )
  private var workers: Int = -1
  
  private var numRequisitions: Int = 12  // 12 weeks of requisitions
  private var modelLine = "model-line"

  override fun run() {
    runBlocking { runPerformanceTest() }
  }

  private suspend fun runPerformanceTest() {
    logger.info("=== Starting ResultsFulfiller performance test ===")

    // Use available CPU cores if -1 is specified for threadPoolSize or workers
    val availableCores = Runtime.getRuntime().availableProcessors()
    if (threadPoolSize == -1) {
      threadPoolSize = availableCores
      logger.info("  Using available CPU cores for threadPoolSize: $threadPoolSize")
    }
    if (workers == -1) {
      workers = availableCores
      logger.info("  Using available CPU cores for workers: $workers")
    }

    logger.info("Configuration:")
    logger.info("  Impressions storage: $impressionsStorageRoot")
    logger.info("  Impressions metadata storage: $impressionsMetadataStorageRoot")
    logger.info("  Number of requisitions: $numRequisitions")
    logger.info(
      "  Pipeline config: batchSize=$batchSize, workers=$workers, threadPoolSize=$threadPoolSize, channelCapacity=$channelCapacity"
    )

    // Create storage configurations
    logger.info("[1/8] Creating storage configurations...")
    val impressionsStorageConfig = StorageConfig(rootDirectory = File(impressionsStorageRoot))
    logger.info("  - Impressions storage config created: $impressionsStorageRoot")
    val impressionsMetadataStorageConfig =
      StorageConfig(rootDirectory = File(impressionsMetadataStorageRoot))
    logger.info("  - Metadata storage config created: $impressionsMetadataStorageRoot")
    val eventGroupReferenceId = "edpa-eg-reference-id-1"
    logger.info("  - Using event group reference ID: $eventGroupReferenceId")

    // Create impression metadata service
    logger.info("[2/8] Creating impression metadata service...")
    val impressionMetadataService =
      StorageImpressionMetadataService(
        impressionsMetadataStorageConfig = impressionsMetadataStorageConfig,
        impressionsBlobDetailsUriPrefix = "file:///impressions/",
        zoneIdForDates = ZoneOffset.UTC,
      )
    logger.info("  - Impression metadata service created successfully")

    // Build model line info map using synthetic data
    logger.info("[3/8] Building model line info map...")
    val startModelLine = System.currentTimeMillis()
    val modelLineInfoMap = buildSyntheticModelLineMap()
    val endModelLine = System.currentTimeMillis()
    logger.info("  - Model line info map built in ${endModelLine - startModelLine}ms")
    logger.info("  - Model lines in map: ${modelLineInfoMap.keys.joinToString()}")
    
    return

    // Create pipeline configuration
    logger.info("[4/8] Creating pipeline configuration...")
    val pipelineConfiguration =
      PipelineConfiguration(
        batchSize = batchSize,
        channelCapacity = channelCapacity,
        threadPoolSize = threadPoolSize,
        workers = workers,
      )
    logger.info("  - Pipeline configuration created")

    // Use NoOpFulfillerSelector to avoid actual fulfillment
    logger.info("[5/8] Creating NoOpFulfillerSelector...")
    val fulfillerSelector = NoOpFulfillerSelector()
    logger.info("  - NoOpFulfillerSelector created")

    // Generate test requisitions directly
    logger.info("[6/8] Generating test requisitions...")
    val startGen = System.currentTimeMillis()
    val groupedRequisitions = generateTestRequisitions()
    val endGen = System.currentTimeMillis()
    logger.info(
      "  - Generated ${groupedRequisitions.requisitionsCount} requisitions in ${endGen - startGen}ms"
    )
    logger.info("  - Event group map entries: ${groupedRequisitions.eventGroupMapCount}")

    // Create ResultsFulfiller
    logger.info("[7/8] Creating ResultsFulfiller...")
    val startFulfillerCreation = System.currentTimeMillis()
    val resultsFulfiller =
      ResultsFulfiller(
        privateEncryptionKey = DATA_PROVIDER_PRIVATE_KEY,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = modelLineInfoMap,
        pipelineConfiguration = pipelineConfiguration,
        impressionMetadataService = impressionMetadataService,
        kmsClient = null,
        impressionsStorageConfig = impressionsStorageConfig,
        fulfillerSelector = fulfillerSelector,
      )
    val endFulfillerCreation = System.currentTimeMillis()
    logger.info(
      "  - ResultsFulfiller created in ${endFulfillerCreation - startFulfillerCreation}ms"
    )

    try {
      logger.info("[8/8] Running ResultsFulfiller.fulfillRequisitions()...")
      logger.info("=== STARTING FULFILLMENT PROCESS ===")
      val startTime = System.currentTimeMillis()

      // Add periodic logging during fulfillment
      val loggingJob: Job =
        kotlinx.coroutines.GlobalScope.launch {
          var elapsed = 0
          try {
            while (true) {
              kotlinx.coroutines.delay(5000) // Log every 5 seconds
              elapsed += 5
              logger.info("[HEARTBEAT] Still processing... elapsed ${elapsed}s")
            }
          } catch (e: kotlinx.coroutines.CancellationException) {
            logger.info("[HEARTBEAT] Stopping heartbeat logging")
          }
        }

      logger.info("Calling resultsFulfiller.fulfillRequisitions()...")
      resultsFulfiller.fulfillRequisitions()
      loggingJob.cancel()

      val endTime = System.currentTimeMillis()
      val totalTime = endTime - startTime

      logger.info("=== FULFILLMENT COMPLETED ===")
      logger.info("Total execution time: ${totalTime}ms")
      logger.info("Average time per requisition: ${totalTime.toDouble() / numRequisitions}ms")
      logger.info("Throughput: ${numRequisitions * 1000.0 / totalTime} requisitions/sec")

      logger.info("Calling logFulfillmentStats()...")
      resultsFulfiller.logFulfillmentStats()
      logger.info("Stats logged successfully")
    } catch (e: Exception) {
      logger.severe("=== FULFILLMENT FAILED ===")
      logger.severe("Exception type: ${e.javaClass.name}")
      logger.severe("Exception message: ${e.message}")
      logger.severe("Stack trace:")
      e.printStackTrace()
      throw e
    }
  }

  private suspend fun buildSyntheticModelLineMap(): Map<String, ModelLineInfo> {
    logger.info("    - Loading test data from runtime path...")
    val testDataRuntimePath = getRuntimePath(TEST_DATA_PATH)!!
    logger.info("    - Test data path: $testDataRuntimePath")

    //    val specFile = testDataRuntimePath.resolve("small_population_spec.textproto").toFile()
    val specFile = testDataRuntimePath.resolve("360m_population_spec.textproto").toFile()
    logger.info("    - Loading synthetic population spec from: ${specFile.absolutePath}")
    logger.info(
      "    - File exists: ${specFile.exists()}, size: ${if (specFile.exists()) specFile.length() else 0} bytes"
    )

    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(specFile, SyntheticPopulationSpec.getDefaultInstance())
    logger.info(
      "    - Loaded synthetic population spec with ${syntheticPopulationSpec.subPopulationsList.size} subpopulations"
    )

    // Convert SyntheticPopulationSpec to PopulationSpec using inline converter
    logger.info("    - Converting synthetic spec to population spec...")
    val populationSpec = syntheticPopulationSpec.toPopulationSpecWithAttributes()
    logger.info(
      "    - Population spec created with ${populationSpec.subpopulationsCount} subpopulations"
    )

    // Use TestEvent descriptor
    logger.info("    - Getting TestEvent descriptor...")
    val eventDescriptor = TestEvent.getDescriptor()
    logger.info("    - Event descriptor obtained: ${eventDescriptor.name}")

    logger.info("    - Building VID index map...")
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec, useParallelSorting = true)
    logger.info("    - VID index map built successfully")

    val modelLineInfo =
      ModelLineInfo(
        populationSpec = populationSpec,
        vidIndexMap = vidIndexMap,
        eventDescriptor = eventDescriptor,
      )
    logger.info("    - ModelLineInfo created for model line: $modelLine")

    return mapOf(modelLine to modelLineInfo)
  }

  private suspend fun generateTestRequisitions(): GroupedRequisitions {
    logger.info("    - Starting generation of requisitions...")
    logger.info("    - Creating requisitions with weekly intervals (1 week to 12 weeks, capped at 2025-03-29)")
    logger.info("    - Each interval will have 7 filters (3 age groups + 2 genders + 1 combined + 1 all)")

    // Define the filters we want to test
    val filters = listOf(
      "person.age_group == 1" to "age-18-34",        // YEARS_18_TO_34
      "person.age_group == 2" to "age-35-54",        // YEARS_35_TO_54
      "person.age_group == 3" to "age-55-plus",      // YEARS_55_PLUS
      "person.gender == 1" to "gender-male",         // MALE
      "person.gender == 2" to "gender-female",       // FEMALE
      "person.age_group == 1 && person.gender == 1" to "young-males", // Combined filter
      "" to "all-events"                             // No filter - all events
    )

    // Generate test requisitions with progressive weekly intervals and different filters
    logger.info("    - Creating individual requisitions...")
    val requisitions = mutableListOf<Requisition>()
    
    for (weekNumber in 1..numRequisitions) {
      logger.info("      - Creating requisitions for week $weekNumber (${weekNumber * 7} days)")
      for ((filter, filterName) in filters) {
        requisitions.add(
          createTestRequisitionWithInterval(weekNumber, filter, filterName)
        )
      }
    }
    
    logger.info("    - All ${requisitions.size} requisitions created (${numRequisitions} weeks × ${filters.size} filters)")

    // Create grouped requisitions directly without using external groupers
    logger.info("    - Creating requisition entries...")
    val requisitionEntries =
      requisitions.mapIndexed { index, requisition ->
        if ((index + 1) % 100 == 0 || index < 10) {
          logger.info("      - Packing requisition ${index + 1}/$numRequisitions")
        }
        GroupedRequisitions.RequisitionEntry.newBuilder()
          .setRequisition(Any.pack(requisition))
          .build()
      }
    logger.info("    - All requisition entries created")

    logger.info("    - Building GroupedRequisitions proto...")
    val eventGroupMap = createEventGroupMap(GROUP_REFERENCE_ID_EDPA_EDP1, EVENT_GROUP_NAME)
    logger.info("    - Event group map created")

    val groupedRequisitions =
      GroupedRequisitions.newBuilder()
        .setModelLine(modelLine)
        .addAllRequisitions(requisitionEntries)
        .addEventGroupMap(eventGroupMap)
        .build()

    logger.info("    - GroupedRequisitions built successfully:")
    logger.info("      * Model line: ${groupedRequisitions.modelLine}")
    logger.info("      * Requisition count: ${groupedRequisitions.requisitionsCount}")
    logger.info("      * Event group map count: ${groupedRequisitions.eventGroupMapCount}")
    return groupedRequisitions
  }

  private fun createTestRequisitionWithInterval(weekNumber: Int, filterExpr: String, filterName: String): Requisition {
    // Calculate the time interval for this requisition
    val startDate = LocalDate.of(2025, 1, 1)
    val maxEndDate = LocalDate.of(2025, 3, 29)
    val calculatedEndDate = startDate.plusWeeks(weekNumber.toLong())
    val endDate = if (calculatedEndDate.isAfter(maxEndDate)) maxEndDate else calculatedEndDate
    val timeRange = OpenEndTimeRange.fromClosedDateRange(startDate..endDate.minusDays(1))
    
    // Create a custom requisition spec with the specific time interval and filter
    val requisitionSpec = requisitionSpec {
      events = events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = interval {
                startTime = timeRange.start.toProtoTime()
                endTime = timeRange.endExclusive.toProtoTime()
              }
              if (filterExpr.isNotEmpty()) {
                filter = eventFilter { expression = filterExpr }
              }
            }
        }
      }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
    }
    
    val encryptedSpec =
      encryptRequisitionSpec(
        signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )
    
    // Create requisition with weekly interval and filter
    return requisition {
      name = "$DATA_PROVIDER_NAME/requisitions/perf-test-requisition-week-$weekNumber-$filterName"
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/perf-test-measurement-week-$weekNumber-$filterName"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(createMeasurementSpec(modelLine), MC_SIGNING_KEY)
      encryptedRequisitionSpec = encryptedSpec
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                noiseMechanisms +=
                  listOf(
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
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE_NAME
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }
  }

  private fun createEventGroupMap(
    eventGroupReferenceId: String,
    eventGroupName: String,
  ): GroupedRequisitions.EventGroupMapEntry {
    return GroupedRequisitions.EventGroupMapEntry.newBuilder()
      .setEventGroup(eventGroupName)
      .setDetails(
        GroupedRequisitions.EventGroupDetails.newBuilder()
          .setEventGroupReferenceId(eventGroupReferenceId)
          .addCollectionIntervals(
            com.google.type.Interval.newBuilder()
              .setStartTime(
                com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(1735689600) // 2025-01-01
                  .build()
              )
              .setEndTime(
                com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(1743465600) // 2025-04-01 (covers 12 weeks from Jan 1)
                  .build()
              )
              .build()
          )
          .build()
      )
      .build()
  }

  companion object {
    private val logger = Logger.getLogger(ResultsFulfillerPerformanceTest::class.java.name)

    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )

    // Same group reference IDs as EdpAggregatorCorrectnessTest
    private const val GROUP_REFERENCE_ID_EDPA_EDP1 = "edpa-eg-reference-id-1"
    private const val GROUP_REFERENCE_ID_EDPA_EDP2 = "edpa-eg-reference-id-2"

    // Constants from ResultsFulfillerTest
    private val FIRST_EVENT_DATE = LocalDate.of(2025, 1, 1)
    private val LAST_EVENT_DATE = LocalDate.of(2025, 2, 1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private const val EDP_DISPLAY_NAME = "edp1"

    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/$EDP_ID"
    private const val DATA_PROVIDER_CERTIFICATE_NAME =
      "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAAg"

    private val MC_SIGNING_KEY =
      loadSigningKey(
        "${MEASUREMENT_CONSUMER_ID}_cs_cert.der",
        "${MEASUREMENT_CONSUMER_ID}_cs_private.der",
      )

    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val DATA_PROVIDER_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_private.tink").toFile())

    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val PERSON = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val REQUISITION_SPEC = requisitionSpec {
      events = events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = interval {
                startTime = TIME_RANGE.start.toProtoTime()
                endTime = TIME_RANGE.endExclusive.toProtoTime()
              }
              filter = eventFilter { expression = "person.gender==1" }
            }
        }
      }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
    }

    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private fun createMeasurementSpec(modelLine: String) = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
      this.modelLine = modelLine
    }

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ResultsFulfillerPerformanceTest(), args)
  }
}

/**
 * Extension function to convert SyntheticPopulationSpec to PopulationSpec with attributes. Based on
 * PopulationSpecConverter from loadtest.dataprovider package.
 */
private fun SyntheticPopulationSpec.toPopulationSpecWithAttributes(): PopulationSpec {
  return populationSpec {
    subpopulations +=
      subPopulationsList.map { it ->
        subPopulation {
          vidRanges += vidRange {
            startVid = it.vidSubRange.start
            endVidInclusive = (it.vidSubRange.endExclusive - 1)
          }
          attributes +=
            person {
                gender =
                  Person.Gender.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.gender"]).enumValue
                  )
                ageGroup =
                  Person.AgeGroup.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.age_group"]).enumValue
                  )
                socialGradeGroup =
                  Person.SocialGradeGroup.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.social_grade_group"])
                      .enumValue
                  )
              }
              .pack()
        }
      }
  }
}
