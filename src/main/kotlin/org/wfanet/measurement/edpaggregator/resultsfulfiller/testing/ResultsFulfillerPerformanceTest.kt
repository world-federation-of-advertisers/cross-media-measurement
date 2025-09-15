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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.nio.file.Paths
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.resultsfulfiller.PipelineConfiguration
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfiller
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageImpressionMetadataService
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine

/**
 * Performance test for [ResultsFulfiller] that processes requisitions without actually fulfilling them.
 * 
 * This utility creates test requisitions and runs them through the ResultsFulfiller pipeline
 * using a [NoOpFulfillerSelector] to measure performance without making actual RPC calls.
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

  @CommandLine.Option(
    names = ["--temp-storage-root"],
    description = ["Temporary directory for test requisitions storage"],
    defaultValue = "/tmp/results_fulfiller_perf_test",
  )
  private var tempStorageRoot: String = "/tmp/results_fulfiller_perf_test"

  override fun run() {
    runBlocking {
      runPerformanceTest()
    }
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
    logger.info("  Pipeline config: batchSize=$batchSize, workers=$workers, threadPoolSize=$threadPoolSize")
    logger.info("  Temp storage: $tempStorageRoot")

    // Load private key
    val privateKey = loadPrivateKey(File(privateKeyPath))

    // Create storage configurations
    val requisitionsStorageConfig = StorageConfig(rootDirectory = File(tempStorageRoot))
    val impressionsStorageConfig = StorageConfig(rootDirectory = File(impressionsStorageRoot))
    val impressionsMetadataStorageConfig = StorageConfig(rootDirectory = File(impressionsMetadataStorageRoot))

    // Create impression metadata service
    val impressionMetadataService = StorageImpressionMetadataService(
      impressionsMetadataStorageConfig = impressionsMetadataStorageConfig,
      impressionsBlobDetailsUriPrefix = impressionsBlobDetailsUriPrefix,
      zoneIdForDates = ZoneOffset.UTC,
    )

    // Build model line info map using synthetic data
    val modelLineInfoMap = buildSyntheticModelLineMap()

    // Create pipeline configuration
    val pipelineConfiguration = PipelineConfiguration(
      batchSize = batchSize,
      channelCapacity = channelCapacity,
      threadPoolSize = threadPoolSize,
      workers = workers
    )

    // Use NoOpFulfillerSelector to avoid actual fulfillment
    val fulfillerSelector = NoOpFulfillerSelector()

    // Generate test requisitions and write to temporary storage
    val requisitionsBlobUri = generateTestRequisitions()

    logger.info("Generated requisitions blob: $requisitionsBlobUri")

    // Create ResultsFulfiller
    val resultsFulfiller = ResultsFulfiller(
      privateEncryptionKey = privateKey,
      requisitionsBlobUri = requisitionsBlobUri,
      requisitionsStorageConfig = requisitionsStorageConfig,
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
      modelLine to ModelLineInfo(
        populationSpec = populationSpec,
        vidIndexMap = InMemoryVidIndexMap.build(populationSpec),
        eventDescriptor = eventDescriptor,
      )
    )
  }

  private suspend fun generateTestRequisitions(): String {
    // Ensure temp directory exists
    File(tempStorageRoot).mkdirs()
    
    val blobUri = "file://$tempStorageRoot/test-requisitions-${System.currentTimeMillis()}.pb"
    
    logger.info("Generating $numRequisitions test requisitions...")
    
    // Generate test requisitions using simplified approach
    val testRequisitions = (1..numRequisitions).map { index ->
      GroupedRequisitions.Requisition.newBuilder()
        .setRequisition(
          Any.pack(
            Requisition.newBuilder()
              .setName("requisitions/perf-test-requisition-$index")
              .setDataProvider("dataProviders/perf-test-dp-$index")
              .setMeasurementSpec(
                Any.pack(
                  MeasurementSpec.newBuilder()
                    .setImpressionCount(MeasurementSpec.ImpressionCount.getDefaultInstance())
                    .build()
                )
              )
              .setProtocolConfig(
                ProtocolConfig.newBuilder()
                  .addProtocols(
                    ProtocolConfig.Protocol.newBuilder()
                      .setDirect(
                        ProtocolConfig.Direct.newBuilder()
                          .addNoiseMechanisms(
                            ProtocolConfig.NoiseMechanism.newBuilder()
                              .setContinuousGaussian(
                                ProtocolConfig.NoiseMechanism.ContinuousGaussianMechanism.getDefaultInstance()
                              )
                              .build()
                          )
                          .build()
                      )
                      .build()
                  )
                  .build()
              )
              .setDataProviderCertificate("dataProviderCertificates/perf-test-cert-$index")
              .setEncryptedRequisitionSpec(ByteString.copyFromUtf8("dummy-encrypted-spec-$index"))
              .build()
          )
        )
        .build()
    }

    // Generate event group maps using the same pattern as EdpAggregatorCorrectnessTest
    val eventGroupMaps = listOf(
      createEventGroupMap(GROUP_REFERENCE_ID_EDPA_EDP1, "eventGroups/edpa-event-group-1"),
      createEventGroupMap(GROUP_REFERENCE_ID_EDPA_EDP2, "eventGroups/edpa-event-group-2"),
    )

    val groupedRequisitions = GroupedRequisitions.newBuilder()
      .setModelLine(modelLine)
      .addAllRequisitions(testRequisitions)
      .addAllEventGroupMap(eventGroupMaps)
      .build()

    val storageClientUri = SelectedStorageClient.parseBlobUri(blobUri)
    val storageClient = SelectedStorageClient(
      storageClientUri,
      File(tempStorageRoot),
      projectId = null
    )

    val serializedData = Any.pack(groupedRequisitions).toByteString()
    storageClient.writeBlob(storageClientUri.key, serializedData)
    
    logger.info("Generated test requisitions blob with $numRequisitions requisitions and ${eventGroupMaps.size} event groups")
    return blobUri
  }

  private fun createEventGroupMap(
    eventGroupReferenceId: String,
    eventGroupName: String
  ): GroupedRequisitions.EventGroupMap {
    return GroupedRequisitions.EventGroupMap.newBuilder()
      .setEventGroup(eventGroupName)
      .setDetails(
        GroupedRequisitions.EventGroupDetails.newBuilder()
          .setEventGroupReferenceId(eventGroupReferenceId)
          .addCollectionIntervals(
            com.google.type.Interval.newBuilder()
              .setStartTime(
                com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(1640995200) // 2022-01-01
                  .build()
              )
              .setEndTime(
                com.google.protobuf.Timestamp.newBuilder()
                  .setSeconds(1672531200) // 2023-01-01
                  .build()
              )
              .build()
          )
          .build()
      )
      .build()
  }

  /**
   * Dummy KMS client for testing that doesn't actually decrypt anything.
   */
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

    @JvmStatic 
    fun main(args: Array<String>) = commandLineMain(ResultsFulfillerPerformanceTest(), args)
  }
}

/**
 * Extension function to convert SyntheticPopulationSpec to PopulationSpec with attributes.
 * Based on PopulationSpecConverter from loadtest.dataprovider package.
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