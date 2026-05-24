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
import com.google.protobuf.ByteString
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import org.junit.Test
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.integration.common.EventGroupConfig

class EdpAggregatorMeasurementConsumerSimulatorTest {

  private class TestableSimulator(
    measurementConsumerData: MeasurementConsumerData,
    syntheticEventGroupMap: Map<String, EventGroupConfig>,
  ) :
    EdpAggregatorMeasurementConsumerSimulator(
      measurementConsumerData = measurementConsumerData,
      outputDpParams = OUTPUT_DP_PARAMS,
      dataProvidersClient = mock<DataProvidersCoroutineStub>(),
      eventGroupsClient = mock<EventGroupsCoroutineStub>(),
      measurementsClient = mock<MeasurementsCoroutineStub>(),
      measurementConsumersClient = mock<MeasurementConsumersCoroutineStub>(),
      certificatesClient = mock<CertificatesCoroutineStub>(),
      trustedCertificates = emptyMap<ByteString, X509Certificate>(),
      messageInstance = TestEvent.getDefaultInstance(),
      expectedDirectNoiseMechanism = NoiseMechanism.GEOMETRIC,
      populationSpec = populationSpec {},
      syntheticEventGroupMap = syntheticEventGroupMap,
      reportName = "measurementConsumers/mc1/reports/report1",
      modelLineName = "modelLines/line1",
      listEventGroupsEntityTypes = emptyList(),
    ) {
    public override fun buildRequisitionInfo(
      dataProvider: DataProvider,
      eventGroups: List<EventGroup>,
      measurementConsumer: MeasurementConsumer,
      nonce: Long,
      percentage: Double,
    ) =
      super.buildRequisitionInfo(dataProvider, eventGroups, measurementConsumer, nonce, percentage)
  }

  @Test
  fun `buildRequisitionInfo includes all event groups when given multiple`() {
    val simulator = TestableSimulator(MC_DATA, emptyMap())

    val eventGroups =
      listOf(
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg1"
          eventGroupReferenceId = "ref-id-1"
          dataAvailabilityInterval = DATA_INTERVAL
        },
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg2"
          eventGroupReferenceId = "ref-id-2"
          dataAvailabilityInterval = DATA_INTERVAL
        },
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg3"
          eventGroupReferenceId = "ref-id-3"
          dataAvailabilityInterval = DATA_INTERVAL
        },
      )

    val result =
      simulator.buildRequisitionInfo(DATA_PROVIDER, eventGroups, MEASUREMENT_CONSUMER, NONCE, 1.0)

    assertThat(result.requisitionSpec.events.eventGroupsList).hasSize(3)
    val keys = result.requisitionSpec.events.eventGroupsList.map { it.key }
    assertThat(keys)
      .containsExactly(
        "$DATA_PROVIDER_NAME/eventGroups/eg1",
        "$DATA_PROVIDER_NAME/eventGroups/eg2",
        "$DATA_PROVIDER_NAME/eventGroups/eg3",
      )
      .inOrder()
    assertThat(result.eventGroups).hasSize(3)
    assertThat(result.dataProviderEntry.key).isEqualTo(DATA_PROVIDER_NAME)
    for (entry in result.requisitionSpec.events.eventGroupsList) {
      assertThat(entry.value.filter.expression).isNotEmpty()
    }
  }

  @Test
  fun `buildRequisitionInfo includes single event group`() {
    val simulator = TestableSimulator(MC_DATA, emptyMap())

    val eventGroups =
      listOf(
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg1"
          eventGroupReferenceId = "ref-id-1"
          dataAvailabilityInterval = DATA_INTERVAL
        }
      )

    val result =
      simulator.buildRequisitionInfo(DATA_PROVIDER, eventGroups, MEASUREMENT_CONSUMER, NONCE, 1.0)

    assertThat(result.requisitionSpec.events.eventGroupsList).hasSize(1)
    assertThat(result.requisitionSpec.events.eventGroupsList[0].key)
      .isEqualTo("$DATA_PROVIDER_NAME/eventGroups/eg1")
  }

  @Test
  fun `buildRequisitionInfo applies percentage to collection interval`() {
    val simulator = TestableSimulator(MC_DATA, emptyMap())

    val eventGroups =
      listOf(
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg1"
          eventGroupReferenceId = "ref-id-1"
          dataAvailabilityInterval = DATA_INTERVAL
        }
      )

    val result =
      simulator.buildRequisitionInfo(DATA_PROVIDER, eventGroups, MEASUREMENT_CONSUMER, NONCE, 0.5)

    val entry = result.requisitionSpec.events.eventGroupsList[0].value
    val startInstant = DATA_START.atStartOfDay(ZoneOffset.UTC).toInstant()
    val endInstant = DATA_END.atStartOfDay(ZoneOffset.UTC).toInstant()
    val fullDurationMs = Duration.between(startInstant, endInstant).toMillis()
    val expectedEndInstant = startInstant.plusMillis((fullDurationMs * 0.5).toLong())
    assertThat(entry.collectionInterval.endTime).isEqualTo(expectedEndInstant.toProtoTime())
  }

  @Test
  fun `buildRequisitionInfo sets nonce on requisition spec`() {
    val simulator = TestableSimulator(MC_DATA, emptyMap())

    val eventGroups =
      listOf(
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg1"
          eventGroupReferenceId = "ref-id-1"
          dataAvailabilityInterval = DATA_INTERVAL
        }
      )

    val result =
      simulator.buildRequisitionInfo(DATA_PROVIDER, eventGroups, MEASUREMENT_CONSUMER, NONCE, 1.0)

    assertThat(result.requisitionSpec.nonce).isEqualTo(NONCE)
  }

  @Test
  fun `buildRequisitionInfo sets measurement public key from measurement consumer`() {
    val simulator = TestableSimulator(MC_DATA, emptyMap())

    val eventGroups =
      listOf(
        eventGroup {
          name = "$DATA_PROVIDER_NAME/eventGroups/eg1"
          eventGroupReferenceId = "ref-id-1"
          dataAvailabilityInterval = DATA_INTERVAL
        }
      )

    val result =
      simulator.buildRequisitionInfo(DATA_PROVIDER, eventGroups, MEASUREMENT_CONSUMER, NONCE, 1.0)

    assertThat(result.requisitionSpec.measurementPublicKey)
      .isEqualTo(MEASUREMENT_CONSUMER.publicKey.message)
  }

  companion object {
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    private const val NONCE = 12345L
    private const val DATA_PROVIDER_NAME = "dataProviders/dp1"

    private val MC_SIGNING_KEY =
      loadSigningKey(
        SECRET_FILES_PATH.resolve("mc_cs_cert.der").toFile(),
        SECRET_FILES_PATH.resolve("mc_cs_private.der").toFile(),
      )

    private val MC_ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val EDP_ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("edp1_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val MC_DATA =
      MeasurementConsumerData(
        name = "measurementConsumers/mc1",
        signingKey = MC_SIGNING_KEY,
        encryptionKey = mock(),
        apiAuthenticationKey = "fake-api-key",
      )

    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private val DATA_START = LocalDate.of(2021, 3, 15)
    private val DATA_END = LocalDate.of(2021, 3, 22)
    private val DATA_INTERVAL = interval {
      startTime = DATA_START.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
      endTime = DATA_END.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
    }

    private val DATA_PROVIDER = dataProvider {
      name = DATA_PROVIDER_NAME
      certificate = "$DATA_PROVIDER_NAME/certificates/cert1"
      publicKey = signedMessage { setMessage(EDP_ENCRYPTION_PUBLIC_KEY.pack()) }
    }

    private val MEASUREMENT_CONSUMER = measurementConsumer {
      name = "measurementConsumers/mc1"
      publicKey = signedMessage { setMessage(MC_ENCRYPTION_PUBLIC_KEY.pack()) }
    }
  }
}
