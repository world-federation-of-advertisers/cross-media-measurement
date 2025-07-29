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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.type.interval
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry

@RunWith(JUnit4::class)
class SingleRequisitionGrouperTest : AbstractRequisitionGrouperTest() {

  override val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase by lazy {
    mockService {}
  }

  override val eventGroupsServiceMock: EventGroupsCoroutineImplBase by lazy {
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

  override val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  private val requisitionValidator by lazy {
    RequisitionsValidator(privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey)
  }

  override val requisitionGrouper: RequisitionGrouper by lazy {
    SingleRequisitionGrouper(
      requisitionValidator = requisitionValidator,
      eventGroupsClient = eventGroupsStub,
      requisitionsClient = requisitionsStub,
      throttler = throttler,
    )
  }

  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `able to map Requisition to GroupedRequisitions`() {
    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(
        listOf(TestRequisitionData.REQUISITION, TestRequisitionData.REQUISITION)
      )
    }
    assertThat(groupedRequisitions).hasSize(2)
    groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals +=
                listOf(
                  interval {
                    startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                    endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
                  }
                )
            }
          }
        )
      assertThat(
          groupedRequisition.requisitionsList
            .map { it.requisition.unpack(Requisition::class.java) }
            .single()
        )
        .isEqualTo(TestRequisitionData.REQUISITION)
    }
  }
}
