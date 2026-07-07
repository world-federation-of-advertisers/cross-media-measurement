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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.requisition
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
class RequisitionGrouperByReportIdTest {

  private val refuseRequisitionRequests = mutableListOf<RefuseRequisitionRequest>()

  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { refuseRequisition(any()) }
        .thenAnswer { invocation ->
          refuseRequisitionRequests += invocation.getArgument<RefuseRequisitionRequest>(0)
          requisition {}
        }
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

  private val requisitionsStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val eventGroupsStub by lazy { EventGroupsCoroutineStub(grpcTestServerRule.channel) }
  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1L))
  private val validator by lazy {
    RequisitionsValidator(privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey)
  }
  private lateinit var grouper: RequisitionGrouperByReportId

  @Before
  fun setUp() {
    grouper =
      RequisitionGrouperByReportId(
        requisitionValidator = validator,
        requisitionsClient = requisitionsStub,
        eventGroupsClient = eventGroupsStub,
        kingdomMutationThrottler = throttler,
        kingdomEventGroupThrottler = throttler,
      )
  }

  private fun makeRequisitionWithInterval(
    name: String,
    startOffsetHours: Long,
    endOffsetHours: Long,
  ): Requisition {
    val spec =
      TestRequisitionData.REQUISITION_SPEC.copy {
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = TestRequisitionData.EVENT_GROUP_NAME
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime =
                        TestRequisitionData.TIME_RANGE.start
                          .plus(startOffsetHours, ChronoUnit.HOURS)
                          .toProtoTime()
                      endTime =
                        TestRequisitionData.TIME_RANGE.endExclusive
                          .plus(endOffsetHours, ChronoUnit.HOURS)
                          .toProtoTime()
                    }
                    filter =
                      RequisitionSpecKt.eventFilter {
                        expression =
                          "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                            "person.gender == ${Person.Gender.FEMALE_VALUE}"
                      }
                  }
              }
          }
      }
    return TestRequisitionData.REQUISITION.copy {
      this.name = name
      encryptedRequisitionSpec =
        encryptRequisitionSpec(
          signedMessage { message = spec.pack() },
          TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
        )
    }
  }

  @Test
  fun `groupForReport returns null for empty input`() = runBlocking {
    val result = grouper.groupForReport(REPORT_ID, emptyList(), GROUP_ID)
    assertThat(result).isNull()
  }

  @Test
  fun `combines two requisitions with overlapping intervals into one entry`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION
    val r2 =
      makeRequisitionWithInterval(
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2",
        startOffsetHours = 1,
        endOffsetHours = 1,
      )

    val result = grouper.groupForReport(REPORT_ID, listOf(r1, r2), GROUP_ID)

    assertThat(result).isNotNull()
    assertThat(result!!.groupId).isEqualTo(GROUP_ID)
    assertThat(result.modelLine).isEqualTo("some-model-line")
    assertThat(result.eventGroupMapList.single())
      .isEqualTo(
        eventGroupMapEntry {
          eventGroup = TestRequisitionData.EVENT_GROUP_NAME
          details = eventGroupDetails {
            eventGroupReferenceId = "some-event-group-reference-id"
            collectionIntervals += interval {
              startTime = TestRequisitionData.TIME_RANGE.start.toProtoTime()
              endTime = TestRequisitionData.TIME_RANGE.endExclusive.plusSeconds(3600).toProtoTime()
            }
          }
        }
      )
    assertThat(result.requisitionsList).hasSize(2)
  }

  @Test
  fun `does not combine disparate time intervals`() = runBlocking {
    val r1 = TestRequisitionData.REQUISITION
    val r2 =
      makeRequisitionWithInterval(
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2",
        startOffsetHours = 100,
        endOffsetHours = 100,
      )

    val result = grouper.groupForReport(REPORT_ID, listOf(r1, r2), GROUP_ID)

    assertThat(result!!.eventGroupMapList.single().details.collectionIntervalsList).hasSize(2)
  }

  @Test
  fun `throws InconsistentEventGroupSelectorsException on mixed entity-key event groups`() {
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

    assertFailsWith<InconsistentEventGroupSelectorsException> {
      runBlocking { grouper.groupForReport(REPORT_ID, listOf(requisition), GROUP_ID) }
    }
  }

  @Test
  fun `groups requisition when all event groups have entity key`() = runBlocking {
    val secondEventGroupName = "${TestRequisitionData.EDP_NAME}/eventGroups/name2"
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<GetEventGroupRequest>(0)
          eventGroup {
            name = request.name
            eventGroupReferenceId =
              if (request.name == TestRequisitionData.EVENT_GROUP_NAME) "ref-1" else "ref-2"
            entityKey =
              EventGroupKt.entityKey {
                entityType = "placement"
                entityId =
                  if (request.name == TestRequisitionData.EVENT_GROUP_NAME) "P-1" else "P-2"
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

    val result: GroupedRequisitions? =
      grouper.groupForReport(REPORT_ID, listOf(requisition), GROUP_ID)

    assertThat(result).isNotNull()
    assertThat(result!!.eventGroupMapList).hasSize(2)
    result.eventGroupMapList.forEach { entry -> assertThat(entry.details.hasEntityKey()).isTrue() }
  }

  @Test
  fun `propagates EventGroup lookup failure`() {
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }
    assertFailsWith<StatusException> {
      runBlocking {
        grouper.groupForReport(REPORT_ID, listOf(TestRequisitionData.REQUISITION), GROUP_ID)
      }
    }
  }

  companion object {
    private const val REPORT_ID = "some-report"
    private const val GROUP_ID = "some-group-id"
  }
}
