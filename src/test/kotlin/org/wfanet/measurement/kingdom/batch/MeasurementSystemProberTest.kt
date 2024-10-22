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
import com.google.protobuf.kotlin.toByteString
import java.io.File
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.listMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.encryptMessage
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey

@RunWith(JUnit4::class)
class MeasurementSystemProberTest {
  private val measurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
    }

  private val measurementsMock: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {
    onBlocking { listMeasurements(any()) }.thenReturn(LIST_MEASUREMENTS_RESPONSE)
  }

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
    measurementConsumersClient = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    measurementsClient = MeasurementsCoroutineStub(grpcTestServerRule.channel)
    dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel)
    eventGroupsClient = EventGroupsGrpcKt.EventGroupsCoroutineStub(grpcTestServerRule.channel)
    requisitionsClient = RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
    prober =
      MeasurementSystemProber(
        MEASUREMENT_CONSUMER_NAME,
        DATA_PROVIDER_NAMES,
        API_AUTHENTICATION_KEY,
        PRIVATE_KEY_DER_FILE,
        MEASUREMENT_LOOKBACK_DURATION,
        DURATION_BETWEEN_MEASUREMENT,
        measurementConsumersClient,
        measurementsClient,
        dataProvidersClient,
        eventGroupsClient,
        requisitionsClient,
        CLOCK,
      )
  }

  @Test
  fun `run creates a new prober measurement when there is no previous prober measurement`(): Unit =
    runBlocking {
      whenever(measurementsMock.listMeasurements(any())).thenReturn(listMeasurementsResponse {})
      val createMeasurementRequest =
        argumentCaptor {
            prober.run()
            verify(measurementsMock).createMeasurement(capture())
          }
          .firstValue
      assertThat(createMeasurementRequest).isNotNull()
    }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement is finished a long time ago`():
    Unit = runBlocking {
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += OLD_FINISHED_MEASUREMENT })
    whenever(CLOCK.instant()).thenReturn(NOW)
    val createMeasurementRequest =
      argumentCaptor {
          prober.run()
          verify(measurementsMock).createMeasurement(capture())
        }
        .firstValue
    assertThat(createMeasurementRequest).isNotNull()
  }

  @Test
  fun `run creates a new prober measurement when most recent issued prober measurement is finished too recently`():
    Unit = runBlocking {
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += NEW_FINISHED_MEASUREMENT })
    whenever(CLOCK.instant()).thenReturn(NOW)
    prober.run()
    verify(measurementsMock, Mockito.never()).createMeasurement(any())
  }

  @Test
  fun `run does not create a new prober measurement when the most recent issued prober measurement is not finished`():
    Unit = runBlocking {
    whenever(measurementsMock.listMeasurements(any()))
      .thenReturn(listMeasurementsResponse { measurements += UNFINISHED_MEASUREMENT })
    prober.run()
    verify(measurementsMock, Mockito.never()).createMeasurement(any())
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private const val API_AUTHENTICATION_KEY = "some-api-key"
    private val PRIVATE_KEY_DER_FILE = SECRETS_DIR.resolve("mc_cs_private.der")
    private val DURATION_BETWEEN_MEASUREMENT = Duration.ofDays(1)
    private val MEASUREMENT_LOOKBACK_DURATION = Duration.ofDays(1)
    private val CLOCK = Mockito.mock<Clock>()

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
    private val LIST_MEASUREMENTS_RESPONSE: ListMeasurementsResponse = listMeasurementsResponse {
      measurements += measurement { measurementConsumer { MEASUREMENT_CONSUMER } }
    }

    val NOW = Instant.now()
    val OLD_FINISHED_MEASUREMENT = measurement {
      state = Measurement.State.SUCCEEDED
      updateTime = NOW.minus(DURATION_BETWEEN_MEASUREMENT).toProtoTime()
    }
    val NEW_FINISHED_MEASUREMENT = measurement {
      state = Measurement.State.SUCCEEDED
      updateTime = NOW.minus(DURATION_BETWEEN_MEASUREMENT - Duration.ofSeconds(1)).toProtoTime()
    }
    private val UNFINISHED_MEASUREMENT = measurement { state = Measurement.State.COMPUTING }

    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private val DATA_PROVIDER_NAMES = listOf(DATA_PROVIDER_NAME)
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

    private val LIST_REQUISITIONS_RESPONSE: ListRequisitionsResponse = listRequisitionsResponse {}
  }
}
