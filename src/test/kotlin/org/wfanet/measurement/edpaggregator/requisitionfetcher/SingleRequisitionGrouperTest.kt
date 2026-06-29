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
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.interval
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.ProtoReflection
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
class SingleRequisitionGrouperTest {

  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase by lazy {
    mockService {}
  }

  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<GetEventGroupRequest>(0)
        eventGroup {
          name = request.name
          eventGroupReferenceId = "some-event-group-reference-id"
        }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
    addService(eventGroupsServiceMock)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1L))
  private val validator by lazy {
    RequisitionsValidator(privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey)
  }
  private val requisitionsStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub by lazy { EventGroupsCoroutineStub(grpcTestServerRule.channel) }
  private lateinit var grouper: SingleRequisitionGrouper

  @Before
  fun setUp() {
    grouper =
      SingleRequisitionGrouper(
        requisitionValidator = validator,
        requisitionsClient = requisitionsStub,
        eventGroupsClient = eventGroupsStub,
        kingdomMutationThrottler = throttler,
        kingdomEventGroupThrottler = throttler,
      )
  }

  @Test
  fun `groupRequisitions maps two requisitions to two GroupedRequisitions`() = runBlocking {
    val result =
      grouper.groupRequisitions(
        listOf(TestRequisitionData.REQUISITION, TestRequisitionData.REQUISITION)
      )
    assertThat(result).hasSize(2)
    result.forEach { groupedRequisition ->
      assertThat(groupedRequisition.groupId).isNotEmpty()
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = TestRequisitionData.EVENT_GROUP_NAME
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals += interval {
                startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
                endTime = TestRequisitionData.TIME_RANGE.endExclusive.toProtoTime()
              }
            }
          }
        )
    }
  }

  @Test
  fun `groupSingle populates entityKey when present`() = runBlocking {
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId = "some-event-group-reference-id"
            entityKey =
              EventGroupKt.entityKey {
                entityType = "placement"
                entityId = "P-123"
              }
          }
        }
    }
    val result = grouper.groupSingle(TestRequisitionData.REQUISITION, "g1")
    assertThat(result).isNotNull()
    val details = result!!.eventGroupMapList.single().details
    assertThat(details.hasEntityKey()).isTrue()
    assertThat(details.entityKey.entityType).isEqualTo("placement")
    assertThat(details.entityKey.entityId).isEqualTo("P-123")
  }

  @Test
  fun `groupSingle does not populate entityKey when EventGroup has none`() = runBlocking {
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenReturn(eventGroup { eventGroupReferenceId = "no-entity-key" })
    }
    val result = grouper.groupSingle(TestRequisitionData.REQUISITION, "some-group-id")
    assertThat(result).isNotNull()
    val entry = result!!.eventGroupMapList.single()
    assertThat(entry.details.hasEntityKey()).isFalse()
  }

  @Test
  fun `groupSingle returns null when RequisitionSpec cannot be parsed`() = runBlocking {
    val bad =
      TestRequisitionData.REQUISITION.copy {
        encryptedRequisitionSpec = encryptedMessage {
          ciphertext = "some-invalid-spec".toByteStringUtf8()
          typeUrl = ProtoReflection.getTypeUrl(RequisitionSpec.getDescriptor())
        }
      }
    val result = grouper.groupSingle(bad, "g1")
    assertThat(result).isNull()
  }

  @Test
  fun `groupSingle returns null on mixed entity key event groups`() = runBlocking {
    val secondEventGroupName = "${TestRequisitionData.EDP_NAME}/eventGroups/name2"
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          if (request.name == TestRequisitionData.EVENT_GROUP_NAME) {
            eventGroup {
              name = request.name
              eventGroupReferenceId = "ref-1"
              entityKey =
                EventGroupKt.entityKey {
                  entityType = "placement"
                  entityId = "P-1"
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

    val result: GroupedRequisitions? = grouper.groupSingle(requisition, "g1")
    assertThat(result).isNull()
  }
}
