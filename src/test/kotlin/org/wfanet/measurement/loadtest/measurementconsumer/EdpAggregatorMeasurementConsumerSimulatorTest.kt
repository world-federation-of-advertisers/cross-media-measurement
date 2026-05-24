// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.measurementconsumer

import com.google.common.truth.Truth.assertThat
import com.google.type.interval
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.integration.common.EventGroupConfig

@RunWith(JUnit4::class)
class EdpAggregatorMeasurementConsumerSimulatorTest {

  private val measurementConsumersServiceMock: MeasurementConsumersCoroutineImplBase = mockService {
    onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
  }

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService()

  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { getDataProvider(any()) }.thenReturn(DATA_PROVIDER)
  }

  private val measurementsServiceMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { createMeasurement(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<CreateMeasurementRequest>(0)
        request.measurement.toBuilder().setName("$MC_NAME/measurements/m1").build()
      }
  }

  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersServiceMock)
    addService(eventGroupsServiceMock)
    addService(dataProvidersServiceMock)
    addService(measurementsServiceMock)
    addService(certificatesServiceMock)
  }

  private class ShortCircuitException : RuntimeException("short-circuit")

  private fun createSimulator(
    syntheticEventGroupMap: Map<String, EventGroupConfig>
  ): EdpAggregatorMeasurementConsumerSimulator {
    return EdpAggregatorMeasurementConsumerSimulator(
      measurementConsumerData = MC_DATA,
      outputDpParams = OUTPUT_DP_PARAMS,
      dataProvidersClient = DataProvidersCoroutineStub(grpcTestServerRule.channel),
      eventGroupsClient = EventGroupsCoroutineStub(grpcTestServerRule.channel),
      measurementsClient = MeasurementsCoroutineStub(grpcTestServerRule.channel),
      measurementConsumersClient = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
      certificatesClient = CertificatesCoroutineStub(grpcTestServerRule.channel),
      trustedCertificates = emptyMap(),
      messageInstance = TestEvent.getDefaultInstance(),
      expectedDirectNoiseMechanism = NoiseMechanism.GEOMETRIC,
      populationSpec = populationSpec {},
      syntheticEventGroupMap = syntheticEventGroupMap,
      reportName = "$MC_NAME/reports/report1",
      modelLineName = "modelLines/line1",
      listEventGroupsEntityTypes = emptyList(),
      onMeasurementsCreated = { throw ShortCircuitException() },
    )
  }

  @Test
  fun `multiple event groups from same data provider all appear in requisition spec`() {
    val eventGroupRefIds = listOf("ref-id-1", "ref-id-2", "ref-id-3")
    val syntheticEventGroupMap =
      eventGroupRefIds.associateWith { EventGroupConfig.LegacySpec(SYNTHETIC_SPEC) }

    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups +=
              eventGroupRefIds.map { refId ->
                eventGroup {
                  name = "$DATA_PROVIDER_NAME/eventGroups/$refId"
                  measurementConsumer = MC_NAME
                  eventGroupReferenceId = refId
                  dataAvailabilityInterval = DATA_INTERVAL
                }
              }
          }
        )
    }

    val simulator = createSimulator(syntheticEventGroupMap)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val request = captor.firstValue

    assertThat(request.measurement.dataProvidersList).hasSize(1)

    val entry = request.measurement.dataProvidersList[0]
    val requisitionSpec = decryptAndParseRequisitionSpec(entry.value.encryptedRequisitionSpec)
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(3)
    val keys = requisitionSpec.events.eventGroupsList.map { it.key }
    assertThat(keys)
      .containsExactly(
        "$DATA_PROVIDER_NAME/eventGroups/ref-id-1",
        "$DATA_PROVIDER_NAME/eventGroups/ref-id-2",
        "$DATA_PROVIDER_NAME/eventGroups/ref-id-3",
      )
  }

  @Test
  fun `single event group produces single entry in requisition spec`() {
    val syntheticEventGroupMap = mapOf("ref-id-1" to EventGroupConfig.LegacySpec(SYNTHETIC_SPEC))

    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/ref-id-1"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "ref-id-1"
              dataAvailabilityInterval = DATA_INTERVAL
            }
          }
        )
    }

    val simulator = createSimulator(syntheticEventGroupMap)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(1)
  }

  @Test
  fun `filterEventGroups excludes event groups not in syntheticEventGroupMap`() {
    val syntheticEventGroupMap =
      mapOf("included-ref" to EventGroupConfig.LegacySpec(SYNTHETIC_SPEC))

    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/included-ref"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "included-ref"
              dataAvailabilityInterval = DATA_INTERVAL
            }
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/excluded-ref"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "excluded-ref"
              dataAvailabilityInterval = DATA_INTERVAL
            }
          }
        )
    }

    val simulator = createSimulator(syntheticEventGroupMap)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(1)
    assertThat(requisitionSpec.events.eventGroupsList[0].key)
      .isEqualTo("$DATA_PROVIDER_NAME/eventGroups/included-ref")
  }

  @Test
  fun `eventGroupFilter parameter further filters event groups`() {
    val syntheticEventGroupMap =
      mapOf(
        "wanted" to EventGroupConfig.LegacySpec(SYNTHETIC_SPEC),
        "unwanted" to EventGroupConfig.LegacySpec(SYNTHETIC_SPEC),
      )

    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/wanted"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "wanted"
              dataAvailabilityInterval = DATA_INTERVAL
            }
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/unwanted"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "unwanted"
              dataAvailabilityInterval = DATA_INTERVAL
            }
          }
        )
    }

    val simulator = createSimulator(syntheticEventGroupMap)
    assertFailsWith<ShortCircuitException> {
      runBlocking {
        simulator.testDirectReachAndFrequency(
          "run1",
          1,
          eventGroupFilter = { it.eventGroupReferenceId == "wanted" },
        )
      }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    assertThat(requisitionSpec.events.eventGroupsList).hasSize(1)
    assertThat(requisitionSpec.events.eventGroupsList[0].key)
      .isEqualTo("$DATA_PROVIDER_NAME/eventGroups/wanted")
  }

  @Test
  fun `collection interval matches event group data availability`() {
    val syntheticEventGroupMap = mapOf("ref-id-1" to EventGroupConfig.LegacySpec(SYNTHETIC_SPEC))

    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = "$DATA_PROVIDER_NAME/eventGroups/ref-id-1"
              measurementConsumer = MC_NAME
              eventGroupReferenceId = "ref-id-1"
              dataAvailabilityInterval = DATA_INTERVAL
            }
          }
        )
    }

    val simulator = createSimulator(syntheticEventGroupMap)
    assertFailsWith<ShortCircuitException> {
      runBlocking { simulator.testDirectReachAndFrequency("run1", 1) }
    }

    val captor = argumentCaptor<CreateMeasurementRequest>()
    verifyBlocking(measurementsServiceMock) { createMeasurement(captor.capture()) }
    val requisitionSpec =
      decryptAndParseRequisitionSpec(
        captor.firstValue.measurement.dataProvidersList[0].value.encryptedRequisitionSpec
      )
    val collectionInterval = requisitionSpec.events.eventGroupsList[0].value.collectionInterval
    assertThat(collectionInterval.startTime).isEqualTo(DATA_INTERVAL.startTime)
    assertThat(collectionInterval.endTime).isEqualTo(DATA_INTERVAL.endTime)
  }

  private fun decryptAndParseRequisitionSpec(
    encryptedRequisitionSpec: org.wfanet.measurement.api.v2alpha.EncryptedMessage
  ): RequisitionSpec {
    val signedRequisitionSpec =
      org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec(
        encryptedRequisitionSpec,
        EDP_PRIVATE_KEY,
      )
    return RequisitionSpec.parseFrom(signedRequisitionSpec.data)
  }

  companion object {
    private val SECRETS_DIR =
      checkNotNull(
          getRuntimePath(
            Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
          )
        )
        .toFile()

    private const val MC_NAME = "measurementConsumers/mc1"
    private const val MC_CERT_NAME = "$MC_NAME/certificates/cert1"
    private const val DATA_PROVIDER_NAME = "dataProviders/dp1"

    private val MC_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey(
        SECRETS_DIR.resolve("mc_cs_cert.der"),
        SECRETS_DIR.resolve("mc_cs_private.der"),
      )

    private val MC_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRETS_DIR.resolve("mc_enc_public.tink").readByteString()
    }

    private val EDP_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
      format = EncryptionPublicKey.Format.TINK_KEYSET
      data = SECRETS_DIR.resolve("edp1_enc_public.tink").readByteString()
    }

    private val EDP_PRIVATE_KEY = loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink"))

    private val MC_DATA =
      MeasurementConsumerData(
        name = MC_NAME,
        signingKey = MC_SIGNING_KEY,
        encryptionKey = loadPrivateKey(SECRETS_DIR.resolve("mc_enc_private.tink")),
        apiAuthenticationKey = "fake-api-key",
      )

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private val MEASUREMENT_CONSUMER = measurementConsumer {
      name = MC_NAME
      certificate = MC_CERT_NAME
      publicKey = signEncryptionPublicKey(MC_ENCRYPTION_PUBLIC_KEY, MC_SIGNING_KEY)
    }

    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = "$DATA_PROVIDER_NAME/certificates/cert1"
      publicKey = signEncryptionPublicKey(EDP_ENCRYPTION_PUBLIC_KEY, MC_SIGNING_KEY)
    }

    private val DATA_START = LocalDate.of(2021, 3, 15)
    private val DATA_END = LocalDate.of(2021, 3, 22)
    private val DATA_INTERVAL = interval {
      startTime = DATA_START.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
      endTime = DATA_END.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
    }

    private val SYNTHETIC_SPEC = SyntheticEventGroupSpec.getDefaultInstance()
  }
}
