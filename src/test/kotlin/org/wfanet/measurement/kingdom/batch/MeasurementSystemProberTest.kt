/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.batch

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.DoublePointData
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.metrics.export.MetricReader
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter
import java.io.File
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.ListMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.encryptMessage
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec

@RunWith(JUnit4::class)
class MeasurementSystemProberTest {
  private val now = Instant.now()

  private val measurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }

  private val measurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {}

  private val dataProvidersMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }

  private val eventGroupsMock: EventGroupsGrpcKt.EventGroupsCoroutineImplBase = mockService {
    onBlocking { listEventGroups(any()) }.thenReturn(LIST_EVENT_GROUPS_RESPONSE)
  }

  private val requisitionsMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }.thenReturn(LIST_REQUISITIONS_RESPONSE)
  }

  private lateinit var measurementConsumersClient: MeasurementConsumersCoroutineStub
  private lateinit var measurementsClient: MeasurementsCoroutineStub
  private lateinit var dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub
  private lateinit var eventGroupsClient: EventGroupsGrpcKt.EventGroupsCoroutineStub
  private lateinit var requisitionsClient: RequisitionsGrpcKt.RequisitionsCoroutineStub
  private lateinit var prober: MeasurementSystemProber
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var metricExporter: InMemoryMetricExporter
  private lateinit var metricReader: MetricReader

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersMock)
    addService(measurementsMock)
    addService(dataProvidersMock)
    addService(eventGroupsMock)
    addService(requisitionsMock)
  }

  @Before
  fun initServices() {
    val dataProviderNames = listOf(DATA_PROVIDER_NAME)
    val apiAuthenticationKey = "some-api-key"
    val privateKeyDerFile = SECRETS_DIR.resolve("mc_cs_private.der")
    measurementConsumersClient = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    measurementsClient = MeasurementsCoroutineStub(grpcTestServerRule.channel)
    dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel)
    eventGroupsClient = EventGroupsGrpcKt.EventGroupsCoroutineStub(grpcTestServerRule.channel)
    requisitionsClient = RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
    val clock = TestClockWithNamedInstants(now)

    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    metricExporter = InMemoryMetricExporter.create()
    metricReader = PeriodicMetricReader.create(metricExporter)
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
        .buildAndRegisterGlobal()

    prober =
      MeasurementSystemProber(
        MEASUREMENT_CONSUMER_NAME,
        dataProviderNames,
        apiAuthenticationKey,
        privateKeyDerFile,
        MEASUREMENT_LOOKBACK_DURATION,
        DURATION_BETWEEN_MEASUREMENT,
        MEASUREMENT_UPDATE_LOOKBACK_DURATION,
        measurementConsumersClient,
        measurementsClient,
        dataProvidersClient,
        eventGroupsClient,
        requisitionsClient,
        clock,
        fixedRandom,
      )
  }

  @After
  fun resetOpenTelemetry() {
    if (this::openTelemetry.isInitialized) {
      SdkMeterProviderUtil.resetForTest(openTelemetry.sdkMeterProvider)
    }
  }

  @Test
  fun `run creates a new prober measurement when there is no previous prober measurement`(): Unit =
    runBlocking {
      whenever(measurementsMock.listMeasurements(any()))
        .thenReturn(ListMeasurementsResponse.getDefaultInstance())

      prober.run()

      verifyProtoArgument(
          measurementsMock,
          MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
        )
        .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
        .isEqualTo(
          createMeasurementRequest {
            parent = MEASUREMENT_CONSUMER_NAME
            measurement = MEASUREMENT
          }
        )

      val metricData: List<MetricData> = metricExporter.finishedMetricItems
      assertThat(metricData).hasSize(0)
    }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement succeeded a long time ago`():
    Unit = runBlocking {
    val oldFinishedMeasurement = measurement {
      state = Measurement.State.SUCCEEDED
      updateTime = now.minus(DURATION_BETWEEN_MEASUREMENT).toProtoTime()
    }
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += oldFinishedMeasurement })

    prober.run()

    verifyProtoArgument(
        measurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .isEqualTo(
        createMeasurementRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          measurement = MEASUREMENT
        }
      )

    metricReader.forceFlush()
    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(
        LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME,
        LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME,
      )
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(
        oldFinishedMeasurement.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND
      )
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(MEASUREMENT_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(true)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(REQUISITION.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(REQUISITION.name)!!.dataProviderId)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(REQUISITION_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(true)
  }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement was cancelled a long time ago`():
    Unit = runBlocking {
    val oldCancelledMeasurement = measurement {
      state = Measurement.State.CANCELLED
      updateTime = now.minus(DURATION_BETWEEN_MEASUREMENT).toProtoTime()
    }
    whenever(requisitionsMock.listRequisitions(any()))
      .thenReturn(
        listRequisitionsResponse {
          requisitions += REQUISITION.copy { state = Requisition.State.WITHDRAWN }
        }
      )
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += oldCancelledMeasurement })

    prober.run()

    verifyProtoArgument(
        measurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .isEqualTo(
        createMeasurementRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          measurement = MEASUREMENT
        }
      )

    metricReader.forceFlush()
    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(
        LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME,
        LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME,
      )
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(
        oldCancelledMeasurement.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND
      )
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(MEASUREMENT_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(false)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(REQUISITION.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(REQUISITION.name)!!.dataProviderId)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(REQUISITION_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(false)
  }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement failed a long time ago`():
    Unit = runBlocking {
    val oldFailedMeasurement = measurement {
      state = Measurement.State.FAILED
      updateTime = now.minus(DURATION_BETWEEN_MEASUREMENT).toProtoTime()
    }
    whenever(requisitionsMock.listRequisitions(any()))
      .thenReturn(
        listRequisitionsResponse {
          requisitions += REQUISITION.copy { state = Requisition.State.REFUSED }
        }
      )
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += oldFailedMeasurement })

    prober.run()

    verifyProtoArgument(
        measurementsMock,
        MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement,
      )
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .isEqualTo(
        createMeasurementRequest {
          parent = MEASUREMENT_CONSUMER_NAME
          measurement = MEASUREMENT
        }
      )

    metricReader.forceFlush()
    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(
        LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME,
        LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME,
      )
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(
        oldFailedMeasurement.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND
      )
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(MEASUREMENT_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(false)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(REQUISITION.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(REQUISITION.name)!!.dataProviderId)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(REQUISITION_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(false)
  }

  @Test
  fun `run does not create a new prober measurement when most recent issued prober measurement is finished too recently`():
    Unit = runBlocking {
    val newFinishedMeasurement = measurement {
      state = Measurement.State.SUCCEEDED
      updateTime = now.minus(DURATION_BETWEEN_MEASUREMENT - Duration.ofSeconds(1)).toProtoTime()
    }
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += newFinishedMeasurement })
    prober.run()
    verify(measurementsMock, never()).createMeasurement(any())

    metricReader.forceFlush()
    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(2)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(
        LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME,
        LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME,
      )
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(
        newFinishedMeasurement.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND
      )
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(MEASUREMENT_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(true)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(REQUISITION.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(REQUISITION.name)!!.dataProviderId)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(REQUISITION_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(true)
  }

  @Test
  fun `run does not create a new prober measurement when the most recent issued prober measurement is not finished`():
    Unit = runBlocking {
    val unfinishedMeasurement = measurement { state = Measurement.State.COMPUTING }
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += unfinishedMeasurement })
    prober.run()
    verify(measurementsMock, never()).createMeasurement(any())

    metricReader.forceFlush()
    val metricData: List<MetricData> = metricExporter.finishedMetricItems
    assertThat(metricData).hasSize(1)
    val metricNameToPoints: Map<String, List<DoublePointData>> =
      metricData.associateBy({ it.name }, { it.doubleGaugeData.points.map { point -> point } })
    assertThat(metricNameToPoints.keys)
      .containsExactly(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)
    assertThat(
        metricNameToPoints.getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0].value
      )
      .isEqualTo(REQUISITION.updateTime.toInstant().toEpochMilli() / MILLISECONDS_PER_SECOND)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(DATA_PROVIDER_ATTRIBUTE_KEY)
      )
      .isEqualTo(CanonicalRequisitionKey.fromName(REQUISITION.name)!!.dataProviderId)
    assertThat(
        metricNameToPoints
          .getValue(LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME)[0]
          .attributes
          .get(REQUISITION_SUCCESS_ATTRIBUTE_KEY)
      )
      .isEqualTo(true)
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private val DURATION_BETWEEN_MEASUREMENT = Duration.ofDays(1)
    private val MEASUREMENT_LOOKBACK_DURATION = Duration.ofDays(1)
    private val MEASUREMENT_UPDATE_LOOKBACK_DURATION = Duration.ofHours(2)
    private const val NONCE = -3060866405677570814L // Hex: D5859E38A0A96502
    private val fixedRandom =
      object : SecureRandom() {
        override fun nextLong(): Long = NONCE
      }

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/1"
    private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
      "measurementConsumers/1/certificates/1"
    private val MC_CERTIFICATE_DER: ByteString =
      SECRETS_DIR.resolve("mc_cs_cert.der").readByteString()
    private val MC_ENCRYPTION_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRETS_DIR.resolve("mc_enc_public.tink")).toEncryptionPublicKey()
    private val MEASUREMENT_CONSUMER: MeasurementConsumer = measurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      certificateDer = MC_CERTIFICATE_DER
      publicKey = signedMessage { setMessage(MC_ENCRYPTION_PUBLIC_KEY.pack()) }
    }
    private val MC_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("mc_cs_cert.der"),
        SECRETS_DIR.resolve("mc_cs_private.der"),
      )

    private val REQUISITION = requisition {
      name = "${DATA_PROVIDER_NAME}/requisitions/foo"
      state = Requisition.State.FULFILLED
    }
    private val LIST_REQUISITIONS_RESPONSE: ListRequisitionsResponse = listRequisitionsResponse {
      requisitions += REQUISITION
    }
    private val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups += eventGroupEntry {
            eventGroupEntry {
              key = EVENT_GROUP_NAME
              value =
                RequisitionSpecKt.EventGroupEntryKt.value {
                  collectionInterval = interval {
                    startTime = Instant.now().minus(MEASUREMENT_LOOKBACK_DURATION).toProtoTime()
                    endTime = Instant.now().plus(Duration.ofDays(1)).toProtoTime()
                  }
                }
            }
          }
        }
      measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
      nonce = NONCE
    }

    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val DATA_PROVIDER_CERTIFICATE_NAME =
      "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAHs"
    private val DATA_PROVIDER_CERTIFICATE_DER: ByteString =
      readCertificate(SECRETS_DIR.resolve("edp1_cs_cert.der").readByteString())
        .encoded
        .toByteString()
    private val DATA_PROVIDER_SIGNING_KEY_HANDLE: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("edp1_cs_cert.der"),
        SECRETS_DIR.resolve("edp1_cs_private.der"),
      )
    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = DATA_PROVIDER_CERTIFICATE_NAME
      certificateDer = DATA_PROVIDER_CERTIFICATE_DER
      publicKey =
        signEncryptionPublicKey(
          loadPublicKey(SECRETS_DIR.resolve("edp1_enc_public.tink")).toEncryptionPublicKey(),
          DATA_PROVIDER_SIGNING_KEY_HANDLE,
        )
    }
    private val DATA_PROVIDER_ENTRY = dataProviderEntry {
      key = DATA_PROVIDER_NAME
      value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = DATA_PROVIDER.certificate
          dataProviderPublicKey = DATA_PROVIDER.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
              DATA_PROVIDER.publicKey.unpack(),
            )
          nonceHash = Hashing.hashSha256(REQUISITION_SPEC.nonce)
        }
    }

    private const val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
    private val LIST_EVENT_GROUPS_RESPONSE: ListEventGroupsResponse = listEventGroupsResponse {
      eventGroups += eventGroup {
        name = EVENT_GROUP_NAME
        measurementConsumer = MEASUREMENT_CONSUMER_NAME
        eventGroupReferenceId = "aaa"
        measurementConsumerPublicKey = MEASUREMENT_CONSUMER.publicKey.message
        encryptedMetadata =
          MC_ENCRYPTION_PUBLIC_KEY.toPublicKeyHandle()
            .encryptMessage(EventGroup.Metadata.getDefaultInstance().pack())
      }
    }

    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MEASUREMENT_CONSUMER.publicKey.message
      nonceHashes += DATA_PROVIDER_ENTRY.value.nonceHash
      vidSamplingInterval = vidSamplingInterval {
        start = 0f
        width = 1f
      }
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 0.005
            delta = 1e-15
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 0.005
            delta = 1e-15
          }
          maximumFrequency = 1
        }
    }

    private val MEASUREMENT = measurement {
      measurementConsumerCertificate = MEASUREMENT_CONSUMER.certificate
      dataProviders += DATA_PROVIDER_ENTRY
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
    }

    private val MEASUREMENT_SPEC_FIELD: Descriptors.FieldDescriptor =
      Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER)
    private val ENCRYPTED_REQUISITION_SPEC_FIELD: Descriptors.FieldDescriptor =
      Measurement.DataProviderEntry.Value.getDescriptor()
        .findFieldByNumber(
          Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
        )

    private const val PROBER_NAMESPACE = "${Instrumentation.ROOT_NAMESPACE}.prober"
    private const val LAST_TERMINAL_MEASUREMENT_TIME_GAUGE_METRIC_NAME =
      "${PROBER_NAMESPACE}.last_terminal_measurement.timestamp"
    private const val LAST_TERMINAL_REQUISITION_TIME_GAUGE_METRIC_NAME =
      "${PROBER_NAMESPACE}.last_terminal_requisition.timestamp"
    private val DATA_PROVIDER_ATTRIBUTE_KEY =
      AttributeKey.stringKey("${Instrumentation.ROOT_NAMESPACE}.data_provider")
    private val REQUISITION_SUCCESS_ATTRIBUTE_KEY =
      AttributeKey.booleanKey("${Instrumentation.ROOT_NAMESPACE}.requisition.success")
    private val MEASUREMENT_SUCCESS_ATTRIBUTE_KEY =
      AttributeKey.booleanKey("${Instrumentation.ROOT_NAMESPACE}.measurement.success")
    private const val MILLISECONDS_PER_SECOND = 1000.0
  }
}
