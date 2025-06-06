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
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.interval
import java.time.Clock
import java.time.Duration
import java.time.temporal.ChronoUnit
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.times
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry

@RunWith(JUnit4::class)
class RequisitionGrouperByReportIdTest : AbstractRequisitionGrouperTest() {

  override val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase by lazy {
    mockService {
      onBlocking { refuseRequisition(any()) }.thenReturn(TestRequisitionData.REQUISITION)
    }
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
    RequisitionValidator(
      requisitionsClient = requisitionsStub,
      throttler = throttler,
      privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey,
    )
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
  fun `able to combine two GroupedRequisitions to a single GroupedRequisitions`() {
    val requisition2 =
      REQUISITION.copy {
        val requisitionSpec =
          REQUISITION_SPEC.copy {
            events =
              RequisitionSpecKt.events {
                eventGroups +=
                  RequisitionSpecKt.eventGroupEntry {
                    key = EVENT_GROUP_NAME
                    value =
                      RequisitionSpecKt.EventGroupEntryKt.value {
                        collectionInterval = interval {
                          startTime = TIME_RANGE.start.plus(1, ChronoUnit.HOURS).toProtoTime()
                          endTime = TIME_RANGE.endExclusive.plus(1, ChronoUnit.HOURS).toProtoTime()
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    val groupedRequisitions: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(listOf(REQUISITION, requisition2))
    assertThat(groupedRequisitions).hasSize(1)
    groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            eventGroupReferenceId = "some-event-group-reference-id"
          }
        )
      assertThat(
          groupedRequisition.collectionIntervals["some-event-group-reference-id"]!!.startTime
        )
        .isEqualTo(TIME_RANGE.start.toProtoTime())
      assertThat(groupedRequisition.collectionIntervals["some-event-group-reference-id"]!!.endTime)
        .isEqualTo(TIME_RANGE.endExclusive.plusSeconds(3600).toProtoTime())
      assertThat(
          groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
        )
        .isEqualTo(listOf(REQUISITION, requisition2))
    }
  }

  @Test
  fun `throws an error for disparate time intervals`() {
    val requisition2 =
      REQUISITION.copy {
        val requisitionSpec =
          REQUISITION_SPEC.copy {
            events =
              RequisitionSpecKt.events {
                eventGroups +=
                  RequisitionSpecKt.eventGroupEntry {
                    key = EVENT_GROUP_NAME
                    value =
                      RequisitionSpecKt.EventGroupEntryKt.value {
                        collectionInterval = interval {
                          startTime = TIME_RANGE.start.plus(100, ChronoUnit.HOURS).toProtoTime()
                          endTime =
                            TIME_RANGE.endExclusive.plus(100, ChronoUnit.HOURS).toProtoTime()
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    val groupedRequisitions: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(listOf(REQUISITION, requisition2))
    assertThat(groupedRequisitions).hasSize(0)
    val refuseRequests: List<RefuseRequisitionRequest> =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::refuseRequisition,
        times(2),
      )
    refuseRequests.forEach { refuseRequest ->
      assertThat(refuseRequest)
        .ignoringFieldScope(
          FieldScopes.allowingFieldDescriptors(
            Requisition.Refusal.getDescriptor()
              .findFieldByNumber(Requisition.Refusal.MESSAGE_FIELD_NUMBER)
          )
        )
        .isEqualTo(
          refuseRequisitionRequest {
            name = REQUISITION.name
            refusal = refusal { justification = Requisition.Refusal.Justification.UNFULFILLABLE }
          }
        )
    }
  }

  @Test
  fun `throws an error if multiple model ids are used for the same report id`() {
    val requisition2 =
      REQUISITION.copy {
        val measurementSpec = MEASUREMENT_SPEC.copy { modelLine = "some-other-model-line" }
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
      }

    val groupedRequisitions: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(listOf(REQUISITION, requisition2))
    assertThat(groupedRequisitions).hasSize(0)
    val refuseRequests: List<RefuseRequisitionRequest> =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::refuseRequisition,
        times(2),
      )
    refuseRequests.forEach { refuseRequest ->
      assertThat(refuseRequest)
        .ignoringFieldScope(
          FieldScopes.allowingFieldDescriptors(
            Requisition.Refusal.getDescriptor()
              .findFieldByNumber(Requisition.Refusal.MESSAGE_FIELD_NUMBER)
          )
        )
        .isEqualTo(
          refuseRequisitionRequest {
            name = REQUISITION.name
            refusal = refusal { justification = Requisition.Refusal.Justification.UNFULFILLABLE }
          }
        )
    }
  }
}
