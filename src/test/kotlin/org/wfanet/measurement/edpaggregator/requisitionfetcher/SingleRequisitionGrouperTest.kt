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
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
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

  @Test
  fun `entity key is populated when EventGroup has one`() {
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId = "some-event-group-reference-id"
            entityKey = EventGroupKt.entityKey {
              entityType = "placement"
              entityId = "P-123"
            }
          }
        }
    }
    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION))
    }
    assertThat(groupedRequisitions).hasSize(1)
    val details = groupedRequisitions[0].eventGroupMapList.single().details
    assertThat(details.hasEntityKey()).isTrue()
    assertThat(details.entityKey.entityType).isEqualTo("placement")
    assertThat(details.entityKey.entityId).isEqualTo("P-123")
  }

  @Test
  fun `entity key is not populated when EventGroup does not have one`() {
    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION))
    }
    assertThat(groupedRequisitions).hasSize(1)
    val details = groupedRequisitions[0].eventGroupMapList.single().details
    assertThat(details.hasEntityKey()).isFalse()
  }

  @Test
  fun `skips requisition when event groups have mixed entity key presence`() {
    val secondEventGroupName = "${TestRequisitionData.EDP_NAME}/eventGroups/name2"

    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          if (request.name == TestRequisitionData.EVENT_GROUP_NAME) {
            eventGroup {
              name = request.name
              eventGroupReferenceId = "ref-1"
              entityKey = EventGroupKt.entityKey {
                entityType = "placement"
                entityId = "P-123"
              }
            }
          } else {
            eventGroup {
              name = request.name
              eventGroupReferenceId = "ref-2"
            }
          }
        }
    }

    val requisitionSpec =
      TestRequisitionData.REQUISITION_SPEC.copy {
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = TestRequisitionData.EVENT_GROUP_NAME
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                      endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
                    }
                  }
              }
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = secondEventGroupName
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                      endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
                    }
                  }
              }
          }
      }
    val requisition =
      TestRequisitionData.REQUISITION.copy {
        encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    // SingleRequisitionGrouper catches exceptions and skips the requisition
    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(requisition))
    }
    assertThat(groupedRequisitions).isEmpty()
  }
}
