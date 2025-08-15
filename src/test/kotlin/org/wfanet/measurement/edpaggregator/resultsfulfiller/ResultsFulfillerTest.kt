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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.requisitionfetcher.SingleRequisitionGrouper
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

@RunWith(JUnit4::class)
class ResultsFulfillerTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  private class FakeRequisitionFulfillmentService : RequisitionFulfillmentCoroutineImplBase() {
    data class FulfillRequisitionInvocation(val requests: List<FulfillRequisitionRequest>)

    private val _fullfillRequisitionInvocations = mutableListOf<FulfillRequisitionInvocation>()
    val fullfillRequisitionInvocations: List<FulfillRequisitionInvocation>
      get() = _fullfillRequisitionInvocations

    override suspend fun fulfillRequisition(
      requests: Flow<FulfillRequisitionRequest>
    ): FulfillRequisitionResponse {
      // Consume flow before returning.
      _fullfillRequisitionInvocations.add(FulfillRequisitionInvocation(requests.toList()))
      return FulfillRequisitionResponse.getDefaultInstance()
    }
  }

  private val requisitionFulfillmentMock = FakeRequisitionFulfillmentService()

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase by lazy {
    mockService {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId = "some-event-group-reference-id"
          }
        }
    }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
    addService(requisitionFulfillmentMock)
  }
  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  @Test
  fun `runWork processes direct requisition successfully`() = runBlocking {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = Files.createTempDirectory(null).toFile()
    val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressions =
      List(130) {
        LABELED_IMPRESSION.copy {
          vid = it.toLong() + 1
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      }
    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    createData(
      kmsClient,
      kekUri,
      impressionsTmpPath,
      metadataTmpPath,
      requisitionsTmpPath,
      impressions,
      listOf(DIRECT_REQUISITION),
    )

    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = metadataTmpPath),
        IMPRESSIONS_METADATA_FILE_URI_PREFIX,
      )

    val resultsFulfiller =
      ResultsFulfiller(
        PRIVATE_ENCRYPTION_KEY,
        requisitionsStub,
        requisitionFulfillmentStub,
        DATA_PROVIDER_CERTIFICATE_KEY,
        EDP_RESULT_SIGNING_KEY,
        typeRegistry,
        REQUISITIONS_FILE_URI,
        StorageConfig(rootDirectory = requisitionsTmpPath),
        ZoneOffset.UTC,
        ContinuousGaussianNoiseSelector(),
        eventReader,
        mapOf("some-model-line" to POPULATION_SPEC),
      )

    resultsFulfiller.fulfillRequisitions()

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long = computeExpectedReach(impressions, MEASUREMENT_SPEC)
    val expectedFrequencyDistribution: Map<Long, Double> =
      computeExpectedFrequencyDistribution(impressions, MEASUREMENT_SPEC)

    assertThat(result.reach.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN)
    assertTrue(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism)
      .isEqualTo(ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN)
    assertTrue(result.frequency.hasDeterministicDistribution())

    val reachTolerance =
      getNoiseTolerance(
        dpParams =
          DifferentialPrivacyParams(
            delta = OUTPUT_DP_PARAMS.delta,
            epsilon = OUTPUT_DP_PARAMS.epsilon,
          ),
        l2Sensitivity = 1.0,
      )
    assertThat(result).reachValue().isWithin(reachTolerance.toDouble()).of(expectedReach)

    assertThat(result)
      .frequencyDistribution()
      .isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE)
      .of(expectedFrequencyDistribution)
  }

  @Test
  fun `runWork processes HM Shuffle requisitions successfully`() = runBlocking {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = Files.createTempDirectory(null).toFile()
    val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressions =
      List(130) {
        LABELED_IMPRESSION.copy {
          vid = it.toLong() + 1
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      }
    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    createData(
      kmsClient,
      kekUri,
      impressionsTmpPath,
      metadataTmpPath,
      requisitionsTmpPath,
      impressions,
      listOf(MULTI_PARTY_REQUISITION),
    )

    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()

    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = metadataTmpPath),
        IMPRESSIONS_METADATA_FILE_URI_PREFIX,
      )

    val resultsFulfiller =
      ResultsFulfiller(
        PRIVATE_ENCRYPTION_KEY,
        requisitionsStub,
        requisitionFulfillmentStub,
        DATA_PROVIDER_CERTIFICATE_KEY,
        EDP_RESULT_SIGNING_KEY,
        typeRegistry,
        REQUISITIONS_FILE_URI,
        StorageConfig(rootDirectory = requisitionsTmpPath),
        ZoneOffset.UTC,
        ContinuousGaussianNoiseSelector(),
        eventReader,
        mapOf("some-model-line" to POPULATION_SPEC),
      )

    resultsFulfiller.fulfillRequisitions()

    val fulfilledRequisitions =
      requisitionFulfillmentMock.fullfillRequisitionInvocations.single().requests
    assertThat(fulfilledRequisitions).hasSize(2)
    ProtoTruth.assertThat(fulfilledRequisitions[0])
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        fulfillRequisitionRequest {
          header =
            FulfillRequisitionRequestKt.header {
              name = MULTI_PARTY_REQUISITION.name
              nonce = REQUISITION_SPEC.nonce
            }
        }
      )
    assertThat(fulfilledRequisitions[0].header.honestMajorityShareShuffle.dataProviderCertificate)
      .isEqualTo(DATA_PROVIDER_CERTIFICATE_NAME)
    assertThat(fulfilledRequisitions[1].bodyChunk.data).isNotEmpty()
  }

  private suspend fun createData(
    kmsClient: KmsClient,
    kekUri: String,
    impressionsTmpPath: File,
    metadataTmpPath: File,
    requisitionsTmpPath: File,
    impressions: List<LabeledImpression>,
    requisitions: List<Requisition>,
  ) {
    // Create requisitions storage client
    Files.createDirectories(requisitionsTmpPath.resolve(REQUISITIONS_BUCKET).toPath())
    val requisitionsStorageClient =
      SelectedStorageClient(REQUISITIONS_FILE_URI, requisitionsTmpPath)
    val requisitionValidator =
      RequisitionsValidator(TestRequisitionData.EDP_DATA.privateEncryptionKey)
    val groupedRequisitions =
      SingleRequisitionGrouper(
          requisitionsClient = requisitionsStub,
          eventGroupsClient = eventGroupsStub,
          requisitionValidator = requisitionValidator,
          throttler = throttler,
        )
        .groupRequisitions(requisitions)
    // Add requisitions to storage
    requisitionsStorageClient.writeBlob(
      REQUISITIONS_BLOB_KEY,
      Any.pack(groupedRequisitions.single()).toByteString(),
    )

    // Create impressions storage client
    Files.createDirectories(impressionsTmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, impressionsTmpPath)

    // Set up streaming encryption
    val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    // Wrap aead client in mesos client
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions metadata store
    Files.createDirectories(metadataTmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())

    val dates =
      generateSequence(FIRST_EVENT_DATE) { date ->
        if (date < LAST_EVENT_DATE) date.plusDays(1) else null
      }

    for (date in dates) {
      val impressionMetadataBlobKey =
        "ds/$date/event-group-reference-id/some-event-group-reference-id/metadata"
      val impressionsMetadataFileUri =
        "file:///$IMPRESSIONS_METADATA_BUCKET/$impressionMetadataBlobKey"

      val impressionsMetadataStorageClient =
        SelectedStorageClient(impressionsMetadataFileUri, metadataTmpPath)

      val encryptedDek =
        EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
      val blobDetails =
        BlobDetails.newBuilder()
          .setBlobUri(IMPRESSIONS_FILE_URI)
          .setEncryptedDek(encryptedDek)
          .build()
      logger.info("Writing $impressionMetadataBlobKey")

      impressionsMetadataStorageClient.writeBlob(
        impressionMetadataBlobKey,
        blobDetails.toByteString(),
      )
    }
  }

  private fun computeExpectedReach(
    impressionsList: List<LabeledImpression>,
    measurementSpec: MeasurementSpec,
  ): Long {
    val sampledVids = sampleVids(impressionsList.map { it.vid }, measurementSpec)
    val sampledReach = runBlocking { MeasurementResults.computeReach(sampledVids.asFlow()) }
    return (sampledReach / measurementSpec.vidSamplingInterval.width).toLong()
  }

  private fun computeExpectedFrequencyDistribution(
    impressionsList: List<LabeledImpression>,
    measurementSpec: MeasurementSpec,
  ): Map<Long, Double> {
    val sampledVids = sampleVids(impressionsList.map { it.vid }, measurementSpec)
    val (_, frequencyMap) =
      runBlocking {
        MeasurementResults.computeReachAndFrequency(
          sampledVids.asFlow(),
          measurementSpec.reachAndFrequency.maximumFrequency,
        )
      }
    return frequencyMap.mapKeys { it.key.toLong() }
  }

  private fun sampleVids(
    impressionsList: List<Long>,
    measurementSpec: MeasurementSpec,
  ): Iterable<Long> {
    val vidSamplingIntervalStart = measurementSpec.vidSamplingInterval.start
    val vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width

    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    return impressionsList
      .filter { vid ->
        VidSampling.sampler.vidIsInSamplingBucket(
          vid,
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth,
        )
      }
      .asIterable()
  }

  /**
   * Calculates a test tolerance for a noised value.
   *
   * The standard deviation of the Gaussian noise is `sqrt(2 * ln(1.25 / delta)) / epsilon`. We
   * return a tolerance of 6 standard deviations, which means a correct implementation should pass
   * this check with near-certainty.
   */
  private fun getNoiseTolerance(dpParams: DifferentialPrivacyParams, l2Sensitivity: Double): Long {
    val stddev = sqrt(2 * ln(1.25 / dpParams.delta)) * l2Sensitivity / dpParams.epsilon
    return (6 * stddev).toLong()
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val LAST_EVENT_DATE = LocalDate.now(ZoneId.of("America/New_York")).minusDays(1)
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val FREQUENCY_DISTRIBUTION_TOLERANCE = 1.0

    private val PERSON = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT = testEvent { person = PERSON }

    private val LABELED_IMPRESSION =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(TEST_EVENT.pack())
        .build()

    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private const val EDP_DISPLAY_NAME = "edp1"
    private val PRIVATE_ENCRYPTION_KEY =
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/$EDP_ID"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
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
    private val MC_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val POPULATION_SPEC = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1
              endVidInclusive = 1000
            }
        }
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
    private val MEASUREMENT_SPEC = measurementSpec {
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
      modelLine = "some-model-line"
    }
    private val DATA_PROVIDER_CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAAg"

    private val DIRECT_REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
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

    const val DUCHY_ONE_ID = "worker1"
    const val DUCHY_TWO_ID = "worker2"

    val DUCHY_ONE_NAME = DuchyKey(DUCHY_ONE_ID).toName()
    val DUCHY_TWO_NAME = DuchyKey(DUCHY_TWO_ID).toName()

    val DUCHY_ONE_SIGNING_KEY =
      loadSigningKey("${DUCHY_ONE_ID}_cs_cert.der", "${DUCHY_ONE_ID}_cs_private.der")
    val DUCHY_TWO_SIGNING_KEY =
      loadSigningKey("${DUCHY_TWO_ID}_cs_cert.der", "${DUCHY_TWO_ID}_cs_private.der")

    val DUCHY_ONE_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_ONE_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_ONE_SIGNING_KEY.certificate.encoded.toByteString()
    }
    val DUCHY_TWO_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_TWO_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_TWO_SIGNING_KEY.certificate.encoded.toByteString()
    }

    val DUCHY1_ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    val DUCHY_ENTRY_ONE = duchyEntry {
      key = DUCHY_ONE_NAME
      value = value {
        duchyCertificate = DUCHY_ONE_CERTIFICATE.name
        honestMajorityShareShuffle = honestMajorityShareShuffle {
          publicKey = signEncryptionPublicKey(DUCHY1_ENCRYPTION_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
        }
      }
    }

    val DUCHY_ENTRY_TWO = duchyEntry {
      key = DUCHY_TWO_NAME
      value = value { duchyCertificate = DUCHY_TWO_CERTIFICATE.name }
    }

    private val MULTI_PARTY_REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            honestMajorityShareShuffle =
              ProtocolConfigKt.honestMajorityShareShuffle {
                noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
                ringModulus = 127
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE_NAME
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += DUCHY_ENTRY_ONE
      duchies += DUCHY_ENTRY_TWO
    }

    private val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    private const val REQUISITIONS_BUCKET = "requisitions-bucket"
    private const val REQUISITIONS_BLOB_KEY = "requisitions"
    private const val REQUISITIONS_FILE_URI = "file:///$REQUISITIONS_BUCKET/$REQUISITIONS_BLOB_KEY"

    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_BLOB_KEY = "impressions"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET/$IMPRESSIONS_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"
  }
}
