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

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.type.interval
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.Logger
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
    names = ["--private-key-path"],
    description = ["Path to private key file for decryption"],
    required = true,
  )
  private lateinit var privateKeyPath: String

  @CommandLine.Option(
    names = ["--impressions-blob-details-uri-prefix"],
    description = ["URI prefix for impressions blob details"],
    required = true,
  )
  private lateinit var impressionsBlobDetailsUriPrefix: String

  @CommandLine.Option(
    names = ["--model-line"],
    description = ["Model line resource name"],
    required = true,
  )
  private lateinit var modelLine: String

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
    defaultValue = "4",
  )
  private var threadPoolSize: Int = 4

  @CommandLine.Option(
    names = ["--workers"],
    description = ["Number of pipeline workers"],
    defaultValue = "4",
  )
  private var workers: Int = 4

  @CommandLine.Option(
    names = ["--num-requisitions"],
    description = ["Number of test requisitions to generate"],
    defaultValue = "10",
  )
  private var numRequisitions: Int = 10

  override fun run() {
    runBlocking { runPerformanceTest() }
  }

  private suspend fun runPerformanceTest() {
    logger.info("Starting ResultsFulfiller performance test...")
    logger.info("Configuration:")
    logger.info("  Impressions storage: $impressionsStorageRoot")
    logger.info("  Impressions metadata storage: $impressionsMetadataStorageRoot")
    logger.info("  Private key: $privateKeyPath")
    logger.info("  Impressions blob prefix: $impressionsBlobDetailsUriPrefix")
    logger.info("  Model line: $modelLine")
    logger.info("  Number of requisitions: $numRequisitions")
    logger.info(
      "  Pipeline config: batchSize=$batchSize, workers=$workers, threadPoolSize=$threadPoolSize"
    )

    // Load private key
    val privateKey = loadPrivateKey(File(privateKeyPath))

    // Create storage configurations
    val impressionsStorageConfig = StorageConfig(rootDirectory = File(impressionsStorageRoot))
    val impressionsMetadataStorageConfig =
      StorageConfig(rootDirectory = File(impressionsMetadataStorageRoot))

    // Create impression metadata service
    val impressionMetadataService =
      StorageImpressionMetadataService(
        impressionsMetadataStorageConfig = impressionsMetadataStorageConfig,
        impressionsBlobDetailsUriPrefix = impressionsBlobDetailsUriPrefix,
        zoneIdForDates = ZoneOffset.UTC,
      )

    // Build model line info map using synthetic data
    val modelLineInfoMap = buildSyntheticModelLineMap()

    // Create pipeline configuration
    val pipelineConfiguration =
      PipelineConfiguration(
        batchSize = batchSize,
        channelCapacity = channelCapacity,
        threadPoolSize = threadPoolSize,
        workers = workers,
      )

    // Use NoOpFulfillerSelector to avoid actual fulfillment
    val fulfillerSelector = NoOpFulfillerSelector()

    // Generate test requisitions directly
    val groupedRequisitions = generateTestRequisitions()

    // Create ResultsFulfiller
    val resultsFulfiller =
      ResultsFulfiller(
        privateEncryptionKey = privateKey,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = modelLineInfoMap,
        pipelineConfiguration = pipelineConfiguration,
        impressionMetadataService = impressionMetadataService,
        kmsClient = DummyKmsClient(),
        impressionsStorageConfig = impressionsStorageConfig,
        fulfillerSelector = fulfillerSelector,
      )

    try {
      logger.info("Running ResultsFulfiller performance test...")
      val startTime = System.currentTimeMillis()

      resultsFulfiller.fulfillRequisitions()

      val endTime = System.currentTimeMillis()
      val totalTime = endTime - startTime

      logger.info("ResultsFulfiller completed successfully!")
      logger.info("Total execution time: ${totalTime}ms")
      logger.info("Average time per requisition: ${totalTime.toDouble() / numRequisitions}ms")

      resultsFulfiller.logFulfillmentStats()
    } catch (e: Exception) {
      logger.severe("ResultsFulfiller failed: ${e.message}")
      e.printStackTrace()
      throw e
    }
  }

  private suspend fun buildSyntheticModelLineMap(): Map<String, ModelLineInfo> {
    val testDataRuntimePath = getRuntimePath(TEST_DATA_PATH)!!

    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        testDataRuntimePath.resolve("synthetic_population_spec_large.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )

    // Convert SyntheticPopulationSpec to PopulationSpec using inline converter
    val populationSpec = syntheticPopulationSpec.toPopulationSpecWithAttributes()

    // Use TestEvent descriptor
    val eventDescriptor = TestEvent.getDescriptor()

    return mapOf(
      modelLine to
        ModelLineInfo(
          populationSpec = populationSpec,
          vidIndexMap = InMemoryVidIndexMap.build(populationSpec),
          eventDescriptor = eventDescriptor,
        )
    )
  }

  private suspend fun generateTestRequisitions(): GroupedRequisitions {
    logger.info("Generating $numRequisitions test requisitions...")

    // Generate test requisitions similar to ResultsFulfillerTest
    val requisitions = (1..numRequisitions).map { index -> createTestRequisition(index) }

    // Create grouped requisitions directly without using external groupers
    val requisitionEntries =
      requisitions.map { requisition ->
        GroupedRequisitions.RequisitionEntry.newBuilder()
          .setRequisition(Any.pack(requisition))
          .build()
      }

    val groupedRequisitions =
      GroupedRequisitions.newBuilder()
        .setModelLine(modelLine)
        .addAllRequisitions(requisitionEntries)
        .addEventGroupMap(createEventGroupMap(GROUP_REFERENCE_ID_EDPA_EDP1, EVENT_GROUP_NAME))
        .build()

    logger.info("Generated grouped requisitions with $numRequisitions requisitions")
    return groupedRequisitions
  }

  private fun createTestRequisition(index: Int): Requisition {
    // Create requisition similar to ResultsFulfillerTest
    return requisition {
      name = "$DATA_PROVIDER_NAME/requisitions/perf-test-requisition-$index"
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/perf-test-measurement-$index"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(createMeasurementSpec(modelLine), MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
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
                  .setSeconds(1640995200) // 2022-01-01 - unused by the fulfiller
                  .build()
              )
              .setEndTime(
                com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(1672531200) // 2023-01-01 - unused
                  .build()
              )
              .build()
          )
          .build()
      )
      .build()
  }

  /** Dummy KMS client for testing that doesn't actually decrypt anything. */
  private class DummyKmsClient : KmsClient {
    override fun withCredentials(credentialPath: String): KmsClient = this

    override fun withDefaultCredentials(): KmsClient = this

    override fun getAead(keyUri: String) = throw UnsupportedOperationException("Dummy KMS client")

    override fun doesSupport(keyUri: String): Boolean = false
  }

  companion object {
    private val logger = Logger.getLogger(ResultsFulfillerPerformanceTest::class.java.name)

    private val TEST_DATA_PATH = Paths.get("src", "main", "k8s", "testing", "data")

    // Same group reference IDs as EdpAggregatorCorrectnessTest
    private const val GROUP_REFERENCE_ID_EDPA_EDP1 = "edpa-eg-reference-id-1"
    private const val GROUP_REFERENCE_ID_EDPA_EDP2 = "edpa-eg-reference-id-2"

    // Constants from ResultsFulfillerTest
    private val LAST_EVENT_DATE = LocalDate.now(ZoneId.of("America/New_York")).minusDays(1)
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
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
