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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.integration.common.EventGroupConfig

@RunWith(JUnit4::class)
class EdpAggregatorMeasurementConsumerSimulatorTest : AbstractMeasurementConsumerSimulatorTest() {

  override fun createSimulator(eventGroupRefIds: List<String>): MeasurementConsumerSimulator {
    val syntheticEventGroupMap =
      eventGroupRefIds.associateWith { EventGroupConfig.LegacySpec(SYNTHETIC_SPEC) }
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
  fun `filterEventGroups excludes event groups not in syntheticEventGroupMap`() {
    val includedRefIds = listOf("sim-eg-included")
    stubEventGroups(includedRefIds + "sim-eg-excluded")

    val simulator = createSimulator(includedRefIds)
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
      .isEqualTo("$DATA_PROVIDER_NAME/eventGroups/sim-eg-included")
  }

  @Test
  fun `collection interval matches event group data availability`() {
    val refIds = listOf("sim-eg-ref-1")
    stubEventGroups(refIds)

    val simulator = createSimulator(refIds)
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

  companion object {
    private val SYNTHETIC_SPEC = SyntheticEventGroupSpec.getDefaultInstance()
  }
}
