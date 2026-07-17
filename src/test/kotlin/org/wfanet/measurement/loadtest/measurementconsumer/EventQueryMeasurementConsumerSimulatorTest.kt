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
import com.google.protobuf.Message
import java.time.LocalDate
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
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

@RunWith(JUnit4::class)
class EventQueryMeasurementConsumerSimulatorTest : AbstractMeasurementConsumerSimulatorTest() {

  override fun createSimulator(eventGroupRefIds: List<String>): MeasurementConsumerSimulator {
    return EventQueryMeasurementConsumerSimulator(
      measurementConsumerData = MC_DATA,
      outputDpParams = OUTPUT_DP_PARAMS,
      dataProvidersClient = DataProvidersCoroutineStub(grpcTestServerRule.channel),
      eventGroupsClient = EventGroupsCoroutineStub(grpcTestServerRule.channel),
      measurementsClient = MeasurementsCoroutineStub(grpcTestServerRule.channel),
      measurementConsumersClient = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
      certificatesClient = CertificatesCoroutineStub(grpcTestServerRule.channel),
      trustedCertificates = emptyMap(),
      eventQuery = STUB_EVENT_QUERY,
      expectedDirectNoiseMechanism = NoiseMechanism.GEOMETRIC,
      eventRange = EVENT_RANGE,
      onMeasurementsCreated = { throw ShortCircuitException() },
    )
  }

  @Test
  fun `filterEventGroups excludes event groups without sim-eg prefix`() {
    val allRefIds = listOf("sim-eg-included", "no-prefix-excluded")
    stubEventGroups(allRefIds)

    val simulator = createSimulator(listOf("sim-eg-included"))
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
  fun `collection interval uses configured eventRange`() {
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
    val expectedInterval = EVENT_RANGE.toInterval()
    assertThat(collectionInterval.startTime).isEqualTo(expectedInterval.startTime)
    assertThat(collectionInterval.endTime).isEqualTo(expectedInterval.endTime)
  }

  companion object {
    private val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    private val STUB_EVENT_QUERY =
      object : EventQuery<Message> {
        override fun getLabeledEvents(
          eventGroupSpec: EventQuery.EventGroupSpec
        ): Sequence<LabeledEvent<Message>> = emptySequence()

        override fun getUserVirtualIdUniverse(): Sequence<Long> = emptySequence()
      }
  }
}
