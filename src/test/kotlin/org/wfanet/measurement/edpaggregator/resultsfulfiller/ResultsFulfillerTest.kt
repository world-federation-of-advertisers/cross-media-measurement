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
import com.google.crypto.tink.*
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.*
import java.util.logging.Logger
import kotlin.math.ln
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
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
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.computation.DifferentialPrivacyParams
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.*
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.requisitionfetcher.SingleRequisitionGrouper
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

@RunWith(JUnit4::class)
class ResultsFulfillerTest {
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader
  private lateinit var spanExporter: InMemorySpanExporter
  private lateinit var metrics: ResultsFulfillerMetrics

  @Before
  fun initTelemetry() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    spanExporter = InMemorySpanExporter.create()
    // Ensure exporters are cleared for test isolation
    metricExporter.reset()
    spanExporter.reset()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
    metrics = ResultsFulfillerMetrics(openTelemetry.getMeter("test"))
  }

  @After
  fun cleanupTelemetry() {
    if (this::openTelemetry.isInitialized) {
      openTelemetry.close()
    }
    if (this::metricExporter.isInitialized) {
      metricExporter.reset()
    }
    if (this::spanExporter.isInitialized) {
      spanExporter.reset()
    }
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
  }

  private fun collectMetrics(): List<MetricData> {
    // Force flush the meter provider to ensure all metrics are exported
    openTelemetry.sdkMeterProvider.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS)
    return metricExporter.finishedMetricItems
  }

  private fun collectSpans(): List<SpanData> {
    return spanExporter.finishedSpanItems
  }

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })
  }

  private val requisitionMetadataServiceMock: RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { startProcessingRequisitionMetadata(any()) }
        .thenReturn(requisitionMetadata { cmmsRequisition = REQUISITION_NAME })
      onBlocking { fulfillRequisitionMetadata(any()) }.thenReturn(requisitionMetadata {})
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
            eventGroupReferenceId = request.name
          }
        }
    }
  }

  private val impressionMetadataServiceMock =
    mockService<ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase>()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
    addService(requisitionFulfillmentMock)
    addService(requisitionMetadataServiceMock)
    addService(impressionMetadataServiceMock)
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

  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub by lazy {
    RequisitionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val impressionMetadataStub:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  private fun createImpressionMetadataList(
    dates: List<LocalDate>,
    eventGroupRef: String,
  ): List<ImpressionMetadata> {
    return dates.map { date ->
      impressionMetadata {
        state = ImpressionMetadata.State.ACTIVE
        blobUri =
          "file:///$IMPRESSIONS_METADATA_BUCKET/ds/$date/model-line/some-model-line/event-group-reference-id/$eventGroupRef/metadata"
      }
    }
  }

  @Test
  fun `runWork processes direct R&F requisition successfully`() = runBlocking {
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

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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
      listOf(DIRECT_RNF_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )

    resultsFulfiller.fulfillRequisitions()

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long = computeExpectedReach(impressions, RNF_MEASUREMENT_SPEC)
    val expectedFrequencyDistribution: Map<Long, Double> =
      computeExpectedFrequencyDistribution(impressions, RNF_MEASUREMENT_SPEC)

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

    verifyBlocking(requisitionMetadataServiceMock, times(1)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
  }

  @Test
  fun `runWork processes direct impression requisition successfully`() = runBlocking {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = Files.createTempDirectory(null).toFile()
    val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressions =
      List(130) {
        LABELED_IMPRESSION.copy {
          vid = it.toLong() + 1
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      } +
        List(130) {
          LABELED_IMPRESSION.copy {
            vid = it.toLong() + 1
            eventTime = TIME_RANGE.start.toProtoTime()
          }
        } +
        List(130) {
          LABELED_IMPRESSION.copy {
            vid = it.toLong() + 1
            eventTime = TIME_RANGE.start.toProtoTime()
          }
        }

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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
      listOf(DIRECT_IMPRESSION_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = NoNoiserSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )

    resultsFulfiller.fulfillRequisitions()

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedImpressions: Long =
      computeExpectedImpressions(impressions, IMPRESSION_MEASUREMENT_SPEC, 10)

    assertThat(result.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
    assertTrue(result.impression.hasDeterministicCount())

    assertThat(result).impressionValue().isEqualTo(expectedImpressions)

    verifyBlocking(requisitionMetadataServiceMock, times(1)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
  }

  @Test
  fun `runWork processes direct impression requisition successfully with overrideMaxFrequency`() =
    runBlocking {
      val impressionsTmpPath = Files.createTempDirectory(null).toFile()
      val metadataTmpPath = Files.createTempDirectory(null).toFile()
      val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
      val impressions =
        List(130) {
          LABELED_IMPRESSION.copy {
            vid = it.toLong() + 1
            eventTime = TIME_RANGE.start.toProtoTime()
          }
        } +
          List(130) {
            LABELED_IMPRESSION.copy {
              vid = it.toLong() + 1
              eventTime = TIME_RANGE.start.toProtoTime()
            }
          } +
          List(130) {
            LABELED_IMPRESSION.copy {
              vid = it.toLong() + 1
              eventTime = TIME_RANGE.start.toProtoTime()
            }
          }

      val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

      val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = REQUISITION_NAME
              blobUri = "some-prefix"
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )
      whenever(requisitionsServiceMock.getRequisition(any()))
        .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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
        listOf(DIRECT_IMPRESSION_REQUISITION),
      )
      val impressionsMetadataService =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
        )

      val fulfillerSelector =
        DefaultFulfillerSelector(
          requisitionsStub = requisitionsStub,
          requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
          noiserSelector = NoNoiserSelector(),
          kAnonymityParams = null,
          overrideImpressionMaxFrequencyPerUser = 2,
        )

      // Load grouped requisitions from storage
      val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

      val resultsFulfiller =
        ResultsFulfiller(
          dataProvider = EDP_NAME,
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
          requisitionMetadataStub = requisitionMetadataStub,
          requisitionsStub = requisitionsStub,
          groupedRequisitions = groupedRequisitions,
          modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
          pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
          impressionDataSourceProvider = impressionsMetadataService,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          kmsClient = kmsClient,
          fulfillerSelector = fulfillerSelector,
          metrics = metrics,
        )

      resultsFulfiller.fulfillRequisitions()

      val request: FulfillDirectRequisitionRequest =
        verifyAndCapture(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
      val result: Measurement.Result =
        decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
      val expectedImpressions: Long =
        computeExpectedImpressions(impressions, IMPRESSION_MEASUREMENT_SPEC, 10)
      val overrideExpectedImpressions: Long =
        computeExpectedImpressions(impressions, IMPRESSION_MEASUREMENT_SPEC, 2)

      assertThat(result.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.impression.hasDeterministicCount())

      assertThat(result).impressionValue().isEqualTo(overrideExpectedImpressions)
      assertThat(overrideExpectedImpressions).isNotEqualTo(expectedImpressions)
      assertThat(result.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(2)

      verifyBlocking(requisitionMetadataServiceMock, times(1)) {
        startProcessingRequisitionMetadata(any())
      }
      verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
    }

  @Test
  fun `runWork processes direct impression requisition successfully with higher overrideMaxFrequency`() =
    runBlocking {
      val impressionsTmpPath = Files.createTempDirectory(null).toFile()
      val metadataTmpPath = Files.createTempDirectory(null).toFile()
      val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
      val impressions =
        List(130) {
          LABELED_IMPRESSION.copy {
            vid = it.toLong() + 1
            eventTime = TIME_RANGE.start.toProtoTime()
          }
        } +
          List(130) {
            LABELED_IMPRESSION.copy {
              vid = it.toLong() + 1
              eventTime = TIME_RANGE.start.toProtoTime()
            }
          } +
          List(130) {
            LABELED_IMPRESSION.copy {
              vid = it.toLong() + 1
              eventTime = TIME_RANGE.start.toProtoTime()
            }
          } +
          List(130) {
            LABELED_IMPRESSION.copy {
              vid = it.toLong() + 1
              eventTime = TIME_RANGE.start.toProtoTime()
            }
          }

      val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

      val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = REQUISITION_NAME
              blobUri = "some-prefix"
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )
      whenever(requisitionsServiceMock.getRequisition(any()))
        .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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
        listOf(CAPPED_DIRECT_IMPRESSION_REQUISITION),
      )
      val impressionsMetadataService =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
        )

      val fulfillerSelector =
        DefaultFulfillerSelector(
          requisitionsStub = requisitionsStub,
          requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
          noiserSelector = NoNoiserSelector(),
          kAnonymityParams = null,
          overrideImpressionMaxFrequencyPerUser = 3,
        )

      // Load grouped requisitions from storage
      val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

      val resultsFulfiller =
        ResultsFulfiller(
          dataProvider = EDP_NAME,
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
          requisitionMetadataStub = requisitionMetadataStub,
          requisitionsStub = requisitionsStub,
          groupedRequisitions = groupedRequisitions,
          modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
          pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
          impressionDataSourceProvider = impressionsMetadataService,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          kmsClient = kmsClient,
          fulfillerSelector = fulfillerSelector,
          metrics = metrics,
        )

      resultsFulfiller.fulfillRequisitions()

      val request: FulfillDirectRequisitionRequest =
        verifyAndCapture(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
      val result: Measurement.Result =
        decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
      val expectedImpressions: Long =
        computeExpectedImpressions(impressions, CAPPED_IMPRESSION_MEASUREMENT_SPEC, 2)
      val overrideExpectedImpressions: Long =
        computeExpectedImpressions(impressions, CAPPED_IMPRESSION_MEASUREMENT_SPEC, 3)

      assertThat(result.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.impression.hasDeterministicCount())

      assertThat(result).impressionValue().isEqualTo(overrideExpectedImpressions)
      assertThat(overrideExpectedImpressions).isNotEqualTo(expectedImpressions)
      assertThat(result.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(3)

      verifyBlocking(requisitionMetadataServiceMock, times(1)) {
        startProcessingRequisitionMetadata(any())
      }
      verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
    }

  @Test
  fun `runWork processes direct impression requisition with uncapped frequency when overrideMaxFrequency is -1`() =
    runBlocking {
      val impressionsTmpPath = Files.createTempDirectory(null).toFile()
      val metadataTmpPath = Files.createTempDirectory(null).toFile()
      val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
      // Create 130 VIDs, each appearing 150 times (19500 total impressions)
      // This tests with data larger than MAX_BYTE_SIZE
      // When uncapped (-1), we should get the raw total: 19500
      val impressions =
        List(150) {
            List(130) {
              LABELED_IMPRESSION.copy {
                vid = it.toLong() + 1
                eventTime = TIME_RANGE.start.toProtoTime()
              }
            }
          }
          .flatten()

      val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

      val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = REQUISITION_NAME
              blobUri = "some-prefix"
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )
      whenever(requisitionsServiceMock.getRequisition(any()))
        .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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
        listOf(DIRECT_IMPRESSION_REQUISITION),
      )
      val impressionsMetadataService =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
        )

      val fulfillerSelector =
        DefaultFulfillerSelector(
          requisitionsStub = requisitionsStub,
          requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
          noiserSelector = NoNoiserSelector(),
          kAnonymityParams = null,
          overrideImpressionMaxFrequencyPerUser = -1, // Uncapped
        )

      // Load grouped requisitions from storage
      val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

      val resultsFulfiller =
        ResultsFulfiller(
          dataProvider = EDP_NAME,
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
          requisitionMetadataStub = requisitionMetadataStub,
          requisitionsStub = requisitionsStub,
          groupedRequisitions = groupedRequisitions,
          modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
          pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
          impressionDataSourceProvider = impressionsMetadataService,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          kmsClient = kmsClient,
          fulfillerSelector = fulfillerSelector,
          metrics = metrics,
        )

      resultsFulfiller.fulfillRequisitions()

      val request: FulfillDirectRequisitionRequest =
        verifyAndCapture(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
      val result: Measurement.Result =
        decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

      // With uncapped impressions (-1), we expect the total impression count (19500)
      // not the capped value (260 if max_freq=2, or 1300 if max_freq=10)
      val expectedUncappedImpressions = 19500L
      val cappedAtTwo = computeExpectedImpressions(impressions, IMPRESSION_MEASUREMENT_SPEC, 2)

      assertThat(result.impression.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.impression.hasDeterministicCount())

      // The uncapped value should be used
      assertThat(result).impressionValue().isEqualTo(expectedUncappedImpressions)
      // Verify it's different from what we'd get with capping
      assertThat(expectedUncappedImpressions).isNotEqualTo(cappedAtTwo)
      // The effective max frequency should be from the measurement spec
      assertThat(result.impression.deterministicCount.customMaximumFrequencyPerUser).isEqualTo(10)

      verifyBlocking(requisitionMetadataServiceMock, times(1)) {
        startProcessingRequisitionMetadata(any())
      }
      verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
    }

  @Test
  fun `runWork skips already fulfilled requisitions`() = runBlocking {
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

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.FULFILLED })

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
      listOf(DIRECT_RNF_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )

    resultsFulfiller.fulfillRequisitions()

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }

    verifyBlocking(requisitionMetadataServiceMock, times(0)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
  }

  @Test
  fun `runWork skips withdrawn requisitions`() = runBlocking {
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

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.WITHDRAWN })

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
      listOf(DIRECT_RNF_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )

    resultsFulfiller.fulfillRequisitions()

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }

    verifyBlocking(requisitionMetadataServiceMock, times(0)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) {
      markWithdrawnRequisitionMetadata(any())
    }
  }

  @Test
  fun `runWork refuses requisitions mark refused`() = runBlocking {
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

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.REFUSED })

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
      listOf(DIRECT_RNF_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )

    resultsFulfiller.fulfillRequisitions()

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }

    verifyBlocking(requisitionMetadataServiceMock, times(0)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) { refuseRequisitionMetadata(any()) }
  }

  @Test
  fun `runWork refuses requisitions in an invalid state`() = runBlocking {
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

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.STATE_UNSPECIFIED })

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
      listOf(DIRECT_RNF_REQUISITION),
    )
    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )
    assertFailsWith<IllegalStateException> { resultsFulfiller.fulfillRequisitions() }

    verifyBlocking(requisitionsServiceMock, times(0)) { fulfillDirectRequisition(any()) }

    verifyBlocking(requisitionMetadataServiceMock, times(0)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(0)) { refuseRequisitionMetadata(any()) }
  }

  fun `runWork processes direct requisition successfully with no noise and k-anonymity`() =
    runBlocking {
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

      val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

      val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = REQUISITION_NAME
              blobUri = "some-prefix"
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )

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
        listOf(DIRECT_RNF_REQUISITION),
      )
      val impressionsMetadataService =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
        )

      val fulfillerSelector =
        DefaultFulfillerSelector(
          requisitionsStub = requisitionsStub,
          requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
          noiserSelector = NoNoiserSelector(),
          kAnonymityParams = KAnonymityParams(100, 100),
          overrideImpressionMaxFrequencyPerUser = null,
        )

      // Load grouped requisitions from storage
      val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

      val resultsFulfiller =
        ResultsFulfiller(
          dataProvider = EDP_NAME,
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
          requisitionMetadataStub = requisitionMetadataStub,
          requisitionsStub = requisitionsStub,
          groupedRequisitions = groupedRequisitions,
          modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
          pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
          impressionDataSourceProvider = impressionsMetadataService,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          kmsClient = kmsClient,
          fulfillerSelector = fulfillerSelector,
          metrics = metrics,
        )

      resultsFulfiller.fulfillRequisitions()

      val request: FulfillDirectRequisitionRequest =
        verifyAndCapture(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
      val result: Measurement.Result =
        decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
      val expectedReach: Long = computeExpectedReach(impressions, RNF_MEASUREMENT_SPEC)
      val expectedFrequencyDistribution: Map<Long, Double> =
        computeExpectedFrequencyDistribution(impressions, RNF_MEASUREMENT_SPEC)

      assertThat(result.reach.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.reach.hasDeterministicCountDistinct())
      assertThat(result.frequency.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.frequency.hasDeterministicDistribution())

      assertThat(result).reachValue().isEqualTo(expectedReach)
      assertThat(expectedReach).isGreaterThan(0)

      assertThat(result).frequencyDistribution().isEqualTo(expectedFrequencyDistribution)

      verifyBlocking(requisitionMetadataServiceMock, times(1)) {
        startProcessingRequisitionMetadata(any())
      }
      verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
    }

  @Test
  fun `runWork processes direct requisition with 0 result for insufficient k-anonymity`() =
    runBlocking {
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

      val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()

      val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

      whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
        .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = REQUISITION_NAME
              blobUri = "some-prefix"
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )

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
        listOf(DIRECT_RNF_REQUISITION),
      )
      val impressionsMetadataService =
        ImpressionDataSourceProvider(
          impressionMetadataStub = impressionMetadataStub,
          dataProvider = "dataProviders/123",
          impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
        )

      val fulfillerSelector =
        DefaultFulfillerSelector(
          requisitionsStub = requisitionsStub,
          requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
          noiserSelector = NoNoiserSelector(),
          kAnonymityParams = KAnonymityParams(100, 1000),
          overrideImpressionMaxFrequencyPerUser = null,
        )

      // Load grouped requisitions from storage
      val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

      val resultsFulfiller =
        ResultsFulfiller(
          dataProvider = EDP_NAME,
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
          requisitionMetadataStub = requisitionMetadataStub,
          requisitionsStub = requisitionsStub,
          groupedRequisitions = groupedRequisitions,
          modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
          pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
          impressionDataSourceProvider = impressionsMetadataService,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          kmsClient = kmsClient,
          fulfillerSelector = fulfillerSelector,
          metrics = metrics,
        )

      resultsFulfiller.fulfillRequisitions()

      val request: FulfillDirectRequisitionRequest =
        verifyAndCapture(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
      val result: Measurement.Result =
        decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

      assertThat(result.reach.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.reach.hasDeterministicCountDistinct())
      assertThat(result.frequency.noiseMechanism).isEqualTo(ProtocolConfig.NoiseMechanism.NONE)
      assertTrue(result.frequency.hasDeterministicDistribution())

      assertThat(result).reachValue().isEqualTo(0)

      result.frequency.relativeFrequencyDistribution.values.forEach {
        assertThat(it).isEqualTo(0.0)
      }

      verifyBlocking(requisitionMetadataServiceMock, times(1)) {
        startProcessingRequisitionMetadata(any())
      }
      verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
    }

  @Test
  fun `fulfillRequisitions emits telemetry`() = runBlocking {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = Files.createTempDirectory(null).toFile()
    val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressions =
      listOf(
        LABELED_IMPRESSION.copy {
          vid = 42
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      )

    val dates = FIRST_EVENT_DATE.datesUntil(LAST_EVENT_DATE.plusDays(1)).toList()
    val impressionMetadataList = createImpressionMetadataList(dates, EVENT_GROUP_NAME)

    whenever(impressionMetadataServiceMock.listImpressionMetadata(any()))
      .thenReturn(listImpressionMetadataResponse { impressionMetadata += impressionMetadataList })
    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "telemetry-prefix"
            blobTypeUrl = "telemetry-blob-type-url"
            groupId = "telemetry-group-id"
            report = "reports/telemetry-report"
          }
        }
      )

    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "telemetry"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    createData(
      kmsClient,
      kekUri,
      impressionsTmpPath,
      metadataTmpPath,
      requisitionsTmpPath,
      impressions,
      listOf(DIRECT_RNF_REQUISITION),
    )

    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = EDP_NAME,
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = emptyMap<String, RequisitionFulfillmentCoroutineStub>(),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)

    val testMetrics = ResultsFulfillerMetrics(openTelemetry.getMeter("test"))

    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = testMetrics,
      )

    resultsFulfiller.fulfillRequisitions()

    verifyBlocking(requisitionsServiceMock, times(1)) { fulfillDirectRequisition(any()) }

    verifyBlocking(requisitionMetadataServiceMock, times(1)) {
      startProcessingRequisitionMetadata(any())
    }

    val metricsByName = collectMetrics().associateBy { it.name }

    val statusKey = AttributeKey.stringKey("edpa.results_fulfiller.status")
    val processedMetrics =
      metricsByName.getValue("edpa.results_fulfiller.requisitions_processed").longSumData.points
    assertThat(processedMetrics).hasSize(1)
    val processedPoint = processedMetrics.single()
    assertThat(processedPoint.value).isEqualTo(1)
    assertThat(processedPoint.attributes.get(statusKey)).isEqualTo("success")

    val requisitionLatency =
      metricsByName
        .getValue("edpa.results_fulfiller.requisition_fulfillment_latency")
        .histogramData
        .points
    assertThat(requisitionLatency).isNotEmpty()
    assertThat(requisitionLatency.single().count).isAtLeast(1)

    val reportLatency =
      metricsByName
        .getValue("edpa.results_fulfiller.report_fulfillment_latency")
        .histogramData
        .points
    assertThat(reportLatency).isNotEmpty()
    assertThat(reportLatency.single().count).isAtLeast(1)

    val frequencyVector =
      metricsByName
        .getValue("edpa.results_fulfiller.frequency_vector_duration")
        .histogramData
        .points
    assertThat(frequencyVector).isNotEmpty()
    assertThat(frequencyVector.single().sum).isGreaterThan(0.0)

    val sendDuration =
      metricsByName.getValue("edpa.results_fulfiller.send_duration").histogramData.points
    assertThat(sendDuration).isNotEmpty()
    assertThat(sendDuration.single().sum).isGreaterThan(0.0)

    val networkDurations =
      metricsByName.getValue("edpa.results_fulfiller.network_tasks_duration").histogramData.points
    assertThat(networkDurations).isNotEmpty()
    assertThat(networkDurations.sumOf { it.count }).isAtLeast(2)

    val spans = collectSpans()
    val reportSpan = spans.first { it.name == "report_fulfillment" }
    val reportFinishedEvent = reportSpan.events.first { it.name == "report_processing_finished" }
    val reportIdAttr = AttributeKey.stringKey("edpa.results_fulfiller.report_id")
    val groupIdAttr = AttributeKey.stringKey("edpa.results_fulfiller.group_id")
    val statusAttr = AttributeKey.stringKey("edpa.results_fulfiller.status")
    assertThat(reportFinishedEvent.attributes.get(reportIdAttr))
      .isEqualTo("reports/telemetry-report")
    val expectedGroupId = groupedRequisitions.groupId
    assertThat(reportFinishedEvent.attributes.get(groupIdAttr)).isEqualTo(expectedGroupId)
    assertThat(reportFinishedEvent.attributes.get(statusAttr)).isEqualTo("success")
    assertThat(reportSpan.status.statusCode).isEqualTo(StatusCode.OK)

    val requisitionSpan = spans.first { it.name == "requisition_fulfillment" }
    val requisitionFinishedEvent =
      requisitionSpan.events.first { it.name == "requisition_processing_finished" }
    val requisitionAttr = AttributeKey.stringKey("edpa.results_fulfiller.cmms_requisition")
    assertThat(requisitionFinishedEvent.attributes.get(requisitionAttr)).isEqualTo(REQUISITION_NAME)
    assertThat(requisitionFinishedEvent.attributes.get(reportIdAttr))
      .isEqualTo("reports/telemetry-report")
    assertThat(requisitionFinishedEvent.attributes.get(statusAttr)).isEqualTo("success")
    assertThat(requisitionSpan.status.statusCode).isEqualTo(StatusCode.OK)
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

    whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
      .thenReturn(
        listRequisitionMetadataResponse {
          requisitionMetadata += requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = timestamp { seconds = 12345 }
            cmmsRequisition = REQUISITION_NAME
            blobUri = "some-prefix"
            blobTypeUrl = "some-blob-type-url"
            groupId = "an-existing-group-id"
            report = "report-name"
          }
        }
      )
    whenever(requisitionsServiceMock.getRequisition(any()))
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })

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

    val impressionsMetadataService =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = "dataProviders/123",
        impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      )

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap =
          mapOf(
            DUCHY_ONE_NAME to requisitionFulfillmentStub,
            DUCHY_TWO_NAME to requisitionFulfillmentStub,
          ),
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        dataProviderSigningKeyHandle = EDP_RESULT_SIGNING_KEY,
        noiserSelector = ContinuousGaussianNoiseSelector(),
        kAnonymityParams = null,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Load grouped requisitions from storage
    val groupedRequisitions = loadGroupedRequisitions(requisitionsTmpPath)
    val resultsFulfiller =
      ResultsFulfiller(
        dataProvider = EDP_NAME,
        privateEncryptionKey = PRIVATE_ENCRYPTION_KEY,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = mapOf("some-model-line" to MODEL_LINE_INFO),
        pipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
        impressionDataSourceProvider = impressionsMetadataService,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
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
    verifyBlocking(requisitionMetadataServiceMock, times(1)) {
      startProcessingRequisitionMetadata(any())
    }
    verifyBlocking(requisitionMetadataServiceMock, times(1)) { fulfillRequisitionMetadata(any()) }
  }

  private suspend fun createData(
    kmsClient: KmsClient,
    kekUri: String,
    impressionsTmpPath: File,
    metadataTmpPath: File,
    requisitionsTmpPath: File,
    allImpressions: List<LabeledImpression>,
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

    // Only write impressions if we have them (not empty blobs for extra days)
    if (allImpressions.isNotEmpty()) {
      val impressionsFlow = flow {
        allImpressions.forEach { impression -> emit(impression.toByteString()) }
      }

      // Write impressions to storage
      mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)
    }

    // Create the impressions metadata store
    Files.createDirectories(metadataTmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    val bucketWithDsPath =
      metadataTmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).resolve("ds").toPath()
    Files.createDirectories(bucketWithDsPath.parent)

    // Create metadata only for the days in the actual time range
    val dates =
      generateSequence(FIRST_EVENT_DATE) { date ->
        if (date <= LAST_EVENT_DATE) date.plusDays(1) else null
      }

    for (date in dates) {
      val impressionMetadataBlobKey =
        "ds/$date/model-line/some-model-line/event-group-reference-id/$EVENT_GROUP_NAME/metadata"
      val impressionsMetadataFileUri =
        "file:///$IMPRESSIONS_METADATA_BUCKET/$impressionMetadataBlobKey"

      val impressionsMetadataStorageClient =
        SelectedStorageClient(impressionsMetadataFileUri, metadataTmpPath)

      val encryptedDek = encryptedDek {
        this.kekUri = kekUri
        typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
        protobufFormat = EncryptedDek.ProtobufFormat.BINARY
        ciphertext = serializedEncryptionKey
      }

      val blobDetails =
        BlobDetails.newBuilder()
          .setBlobUri(IMPRESSIONS_FILE_URI)
          .setEncryptedDek(encryptedDek)
          .setEventGroupReferenceId(EVENT_GROUP_NAME)
          .build()
      logger.info("Writing $impressionMetadataBlobKey")

      impressionsMetadataStorageClient.writeBlob(
        impressionMetadataBlobKey,
        blobDetails.toByteString(),
      )
    }
  }

  private fun computeExpectedImpressions(
    impressionsList: List<LabeledImpression>,
    measurementSpec: MeasurementSpec,
    maximumFrequency: Int,
  ): Long {
    val sampledVids = sampleVids(impressionsList.map { it.vid }, measurementSpec)
    return runBlocking {
      MeasurementResults.computeImpression(sampledVids.asFlow(), maximumFrequency)
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

  /** Loads grouped requisitions from the test storage. */
  private suspend fun loadGroupedRequisitions(
    requisitionsTmpPath: File
  ): org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions {
    val requisitionsStorageClient =
      SelectedStorageClient(REQUISITIONS_FILE_URI, requisitionsTmpPath)
    val requisitionBytes =
      requisitionsStorageClient.getBlob(REQUISITIONS_BLOB_KEY)?.read()?.flatten()
        ?: throw Exception("Requisitions blob not found")
    return Any.parseFrom(requisitionBytes)
      .unpack(org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions::class.java)
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

    // Common pipeline configuration for tests
    private const val DEFAULT_BATCH_SIZE = 256
    private val DEFAULT_PIPELINE_CONFIGURATION =
      PipelineConfiguration(
        batchSize = 1000,
        channelCapacity = 100,
        threadPoolSize = 4,
        workers = 2,
      )

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
    private val RNF_MEASUREMENT_SPEC = measurementSpec {
      reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
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
    private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
      reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      impression = impression {
        privacyParams = OUTPUT_DP_PARAMS
        maximumFrequencyPerUser = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
      modelLine = "some-model-line"
    }
    private val CAPPED_IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
      reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      impression = impression {
        privacyParams = OUTPUT_DP_PARAMS
        maximumFrequencyPerUser = 2
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
      modelLine = "some-model-line"
    }
    private val DATA_PROVIDER_CERTIFICATE_NAME = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAAg"

    private val DIRECT_RNF_REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(RNF_MEASUREMENT_SPEC, MC_SIGNING_KEY)
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
    private val DIRECT_IMPRESSION_REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MC_SIGNING_KEY)
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
                deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE_NAME
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }

    private val CAPPED_DIRECT_IMPRESSION_REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(CAPPED_IMPRESSION_MEASUREMENT_SPEC, MC_SIGNING_KEY)
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
                deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
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
      measurementSpec = signMeasurementSpec(RNF_MEASUREMENT_SPEC, MC_SIGNING_KEY)
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

    private val MODEL_LINE_INFO =
      ModelLineInfo(
        eventDescriptor = TestEvent.getDescriptor(),
        populationSpec = POPULATION_SPEC,
        vidIndexMap = InMemoryVidIndexMap.build(POPULATION_SPEC),
      )
  }
}
