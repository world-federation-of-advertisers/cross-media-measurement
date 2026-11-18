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
import java.time.Duration
import java.time.LocalDate
import java.util.concurrent.TimeoutException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doSuspendableAnswer
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

@RunWith(JUnit4::class)
class EventQueryMeasurementConsumerSimulatorTest : AbstractMeasurementConsumerSimulatorTest() {

  override fun createSimulator(eventGroupRefIds: List<String>): MeasurementConsumerSimulator {
    return createSimulator(
      eventGroupRefIds,
      initialResultPollingDelay = Duration.ofSeconds(1),
      maximumResultPollingDelay = Duration.ofMinutes(1),
      onMeasurementsCreated = { throw ShortCircuitException() },
    )
  }

  private fun createSimulator(
    eventGroupRefIds: List<String>,
    initialResultPollingDelay: Duration,
    maximumResultPollingDelay: Duration,
    onMeasurementsCreated: (() -> Unit)?,
  ): MeasurementConsumerSimulator {
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
      kingdomApiThrottler = kingdomApiThrottler,
      eventRange = EVENT_RANGE,
      initialResultPollingDelay = initialResultPollingDelay,
      maximumResultPollingDelay = maximumResultPollingDelay,
      onMeasurementsCreated = onMeasurementsCreated,
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

  @Test
  fun `testImpression performs final poll when result polling times out`() = runBlocking {
    stubEventGroups(listOf("sim-eg-ref-1"))
    val measurementName = "$MC_NAME/measurements/m1"
    measurementsServiceMock.stub {
      onBlocking { getMeasurement(any()) }
        .thenReturn(
          measurement {
            name = measurementName
            state = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
          },
          measurement {
            name = measurementName
            state = Measurement.State.FAILED
          },
        )
    }
    val simulator =
      createSimulator(
        listOf("sim-eg-ref-1"),
        initialResultPollingDelay = Duration.ofSeconds(10),
        maximumResultPollingDelay = Duration.ofSeconds(10),
        onMeasurementsCreated = null,
      )

    val exception =
      assertFailsWith<IllegalStateException> {
        simulator.testImpression("run1", resultPollingTimeout = Duration.ofMillis(100))
      }

    assertThat(exception).hasMessageThat().contains("Measurement failed")
    verifyBlocking(measurementsServiceMock, times(2)) { getMeasurement(any()) }
  }

  @Test
  fun `testImpression times out when final poll does not complete`() = runTest {
    stubEventGroups(listOf("sim-eg-ref-1"))
    val measurementName = "$MC_NAME/measurements/m1"
    var getMeasurementCount = 0
    measurementsServiceMock.stub {
      onBlocking { getMeasurement(any()) } doSuspendableAnswer
        {
          getMeasurementCount++
          if (getMeasurementCount == 1) {
            measurement {
              name = measurementName
              state = Measurement.State.AWAITING_REQUISITION_FULFILLMENT
            }
          } else {
            awaitCancellation()
          }
        }
    }
    val simulator =
      createSimulator(
        listOf("sim-eg-ref-1"),
        initialResultPollingDelay = Duration.ofSeconds(10),
        maximumResultPollingDelay = Duration.ofSeconds(10),
        onMeasurementsCreated = null,
      )

    val exception =
      assertFailsWith<TimeoutException> {
        simulator.testImpression("run1", resultPollingTimeout = Duration.ofSeconds(1))
      }

    assertThat(exception).hasMessageThat().contains("Final result read did not complete")
    verifyBlocking(measurementsServiceMock, times(2)) { getMeasurement(any()) }
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
