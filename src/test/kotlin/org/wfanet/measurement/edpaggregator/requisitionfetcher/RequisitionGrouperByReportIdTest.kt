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
import com.google.protobuf.Any
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.time.temporal.ChronoUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.edpaggregator.requisitionfetcher.testing.TestRequisitionData
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class RequisitionGrouperByReportIdTest : AbstractRequisitionGrouperTest() {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  private val createRequisitionMetadataRequests = mutableListOf<CreateRequisitionMetadataRequest>()
  private val refuseRequisitionMetadataRequests = mutableListOf<RefuseRequisitionMetadataRequest>()
  private val refuseRequisitionRequests = mutableListOf<RefuseRequisitionRequest>()

  override val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { refuseRequisition(any()) }
        .thenAnswer { invocation ->
          val req = invocation.getArgument<RefuseRequisitionRequest>(0)
          refuseRequisitionRequests += req
          requisition {}
        }
    }

  private val requisitionMetadataServiceMock:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listRequisitionMetadata(any()) }.thenReturn(listRequisitionMetadataResponse {})
      onBlocking { createRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val req = invocation.getArgument<CreateRequisitionMetadataRequest>(0)
          createRequisitionMetadataRequests += req
          requisitionMetadata {}
        }
      onBlocking { refuseRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val req = invocation.getArgument<RefuseRequisitionMetadataRequest>(0)
          refuseRequisitionMetadataRequests += req
          requisitionMetadata {}
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
    addService(requisitionMetadataServiceMock)
  }

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  private val requisitionValidator by lazy {
    RequisitionsValidator(privateEncryptionKey = TestRequisitionData.EDP_DATA.privateEncryptionKey)
  }

  override val requisitionGrouper: RequisitionGrouper by lazy {
    val storageClient = FileSystemStorageClient(tempFolder.root)
    RequisitionGrouperByReportId(
      requisitionValidator = requisitionValidator,
      dataProviderName = DATA_PROVIDER_NAME,
      blobUriPrefix = BLOB_URI_PREFIX,
      requisitionMetadataStub = requisitionMetadataStub,
      storageClient,
      100,
      STORAGE_PATH_PREFIX,
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

  private val requisitionMetadataStub:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub by lazy {
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  @Test
  fun `able to combine two GroupedRequisitions to a single GroupedRequisitions`() {
    val requisition2 =
      TestRequisitionData.REQUISITION.copy {
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
                          startTime =
                            TestRequisitionData.TIME_RANGE.start
                              .plus(1, ChronoUnit.HOURS)
                              .toProtoTime()
                          endTime =
                            TestRequisitionData.TIME_RANGE.endExclusive
                              .plus(1, ChronoUnit.HOURS)
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
    }
    assertThat(groupedRequisitions).hasSize(1)
    val groupedRequisition = groupedRequisitions.single()
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
                  endTime =
                    TestRequisitionData.TIME_RANGE.endExclusive.plusSeconds(3600).toProtoTime()
                }
              )
          }
        }
      )
    assertThat(
        groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
      )
      .isEqualTo(listOf(TestRequisitionData.REQUISITION, requisition2))

    assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")
    assertThat(createRequisitionMetadataRequests).hasSize(2)
  }

  @Test
  fun `existing requisition metadata without blob storage resolves into two grouped requisitions`() =
    runBlocking {
      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = TestRequisitionData.REQUISITION.name
              blobUri = BLOB_URI_PREFIX
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )

      val requisition2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                            startTime =
                              TestRequisitionData.TIME_RANGE.start
                                .plus(1, ChronoUnit.HOURS)
                                .toProtoTime()
                            endTime =
                              TestRequisitionData.TIME_RANGE.endExclusive
                                .plus(1, ChronoUnit.HOURS)
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
          this.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = requisitionSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
        }

      val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
        requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
      }
      assertThat(groupedRequisitions).hasSize(2)
      val groupedRequisition = groupedRequisitions[0]
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
          groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
        )
        .isEqualTo(listOf(TestRequisitionData.REQUISITION))

      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      val newGroupedRequisition = groupedRequisitions[1]
      assertThat(newGroupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals +=
                listOf(
                  interval {
                    startTime =
                      TestRequisitionData.TIME_RANGE.start.plus(1, ChronoUnit.HOURS).toProtoTime()
                    endTime =
                      TestRequisitionData.TIME_RANGE.endExclusive
                        .plus(1, ChronoUnit.HOURS)
                        .toProtoTime()
                  }
                )
            }
          }
        )
      assertThat(
          newGroupedRequisition.requisitionsList.map {
            it.requisition.unpack(Requisition::class.java)
          }
        )
        .isEqualTo(listOf(requisition2))

      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      assertThat(createRequisitionMetadataRequests).hasSize(1)
    }

  @Test
  fun `Requisitions are refused to Kingdom when createRequisitionMetadata throws`() = runBlocking {
    whenever(requisitionMetadataServiceMock.createRequisitionMetadata(any()))
      .thenThrow(RuntimeException())

    val requisition2 =
      TestRequisitionData.REQUISITION.copy {
        val measurementSpec =
          TestRequisitionData.MEASUREMENT_SPEC.copy { modelLine = "some-other-model-line" }
        this.measurementSpec =
          signMeasurementSpec(measurementSpec, TestRequisitionData.MC_SIGNING_KEY)
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                          startTime =
                            TestRequisitionData.TIME_RANGE.start
                              .plus(1, ChronoUnit.HOURS)
                              .toProtoTime()
                          endTime =
                            TestRequisitionData.TIME_RANGE.endExclusive
                              .plus(1, ChronoUnit.HOURS)
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }
    assertFailsWith<StatusException> {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
    }

    assertThat(refuseRequisitionRequests).hasSize(2)
  }

  @Test
  fun `Requisitions are refused to Kingdom when refuseRequisitionMetadata throws`() = runBlocking {
    whenever(requisitionMetadataServiceMock.refuseRequisitionMetadata(any()))
      .thenThrow(RuntimeException())

    val requisition2 =
      TestRequisitionData.REQUISITION.copy {
        val measurementSpec =
          TestRequisitionData.MEASUREMENT_SPEC.copy { modelLine = "some-other-model-line" }
        this.measurementSpec =
          signMeasurementSpec(measurementSpec, TestRequisitionData.MC_SIGNING_KEY)
        name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                          startTime =
                            TestRequisitionData.TIME_RANGE.start
                              .plus(1, ChronoUnit.HOURS)
                              .toProtoTime()
                          endTime =
                            TestRequisitionData.TIME_RANGE.endExclusive
                              .plus(1, ChronoUnit.HOURS)
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    assertFailsWith<StatusException> {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
    }
    assertThat(refuseRequisitionRequests).hasSize(2)
  }

  @Test
  fun `skips Requisition when EventGroup not found`() {

    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }
    assertFailsWith<StatusException> {
      runBlocking { requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION)) }
    }
  }

  @Test
  fun `existing multiple requisition metadata resolves correclty into two grouped requisitions`() =
    runBlocking {
      val storageClient = FileSystemStorageClient(tempFolder.root)
      val firstBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/an-existing-group-id"
      val secondBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/another-existing-group-id"
      val blobKey = "$STORAGE_PATH_PREFIX/an-existing-group-id"
      val defaultGroupedRequisitions = GroupedRequisitions.getDefaultInstance()

      storageClient.writeBlob(blobKey, Any.pack(defaultGroupedRequisitions).toByteString())

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
              blobUri = firstBlobUri
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 67890 }
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo3"
              blobUri = secondBlobUri
              blobTypeUrl = "another-blob-type-url"
              groupId = "another-existing-group-id"
              report = "another-report-name"
            }
          }
        )

      val requisition2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                            startTime =
                              TestRequisitionData.TIME_RANGE.start
                                .plus(1, ChronoUnit.HOURS)
                                .toProtoTime()
                            endTime =
                              TestRequisitionData.TIME_RANGE.endExclusive
                                .plus(1, ChronoUnit.HOURS)
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
          this.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = requisitionSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
        }

      val requisition3 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo3"
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
                            startTime =
                              TestRequisitionData.TIME_RANGE.start
                                .plus(2, ChronoUnit.HOURS)
                                .toProtoTime()
                            endTime =
                              TestRequisitionData.TIME_RANGE.endExclusive
                                .plus(2, ChronoUnit.HOURS)
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
          this.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = requisitionSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
        }

      val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
        requisitionGrouper.groupRequisitions(
          listOf(TestRequisitionData.REQUISITION, requisition2, requisition3)
        )
      }
      assertThat(groupedRequisitions).hasSize(2)
      val groupedRequisition = groupedRequisitions[0]
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals +=
                listOf(
                  interval {
                    startTime =
                      TestRequisitionData.TIME_RANGE.start.plus(2, ChronoUnit.HOURS).toProtoTime()
                    endTime =
                      TestRequisitionData.TIME_RANGE.endExclusive
                        .plus(2, ChronoUnit.HOURS)
                        .toProtoTime()
                  }
                )
            }
          }
        )
      assertThat(
          groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
        )
        .isEqualTo(listOf(requisition3))
      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      val newGroupedRequisition = groupedRequisitions[1]
      assertThat(newGroupedRequisition.eventGroupMapList.single())
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
          newGroupedRequisition.requisitionsList.map {
            it.requisition.unpack(Requisition::class.java)
          }
        )
        .isEqualTo(listOf(TestRequisitionData.REQUISITION))

      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      assertThat(createRequisitionMetadataRequests).hasSize(1)
    }

  @Test
  fun `existing metadata in queued or processing state prevents creation of new grouped requisitions`() =
    runBlocking {
      val storageClient = FileSystemStorageClient(tempFolder.root)

      val queuedBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/group-id"
      val processingBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/group-id"
      val blobKey = "$STORAGE_PATH_PREFIX/group-id"
      val defaultGroupedRequisitions = GroupedRequisitions.getDefaultInstance()

      storageClient.writeBlob(blobKey, Any.pack(defaultGroupedRequisitions).toByteString())

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.QUEUED
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo-queued"
              blobUri = queuedBlobUri
              blobTypeUrl = "blob-type"
              groupId = "group-id"
              report = "report-name"
            }
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.PROCESSING
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo-processing"
              blobUri = processingBlobUri
              blobTypeUrl = "blob-type"
              groupId = "group-id"
              report = "report-name"
            }
          }
        )

      val requisition1 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo-queued"
        }

      val requisition2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo-processing"
        }

      val result = requisitionGrouper.groupRequisitions(listOf(requisition1, requisition2))

      assertThat(result).isEmpty()

      assertThat(createRequisitionMetadataRequests).isEmpty()
    }

  @Test
  fun `requisition metadata with no blob associated, correctly returns GroupedRequisitions`() =
    runBlocking {
      val firstBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/an-existing-group-id"
      val secondBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/another-existing-group-id"

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo"
              blobUri = firstBlobUri
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 67890 }
              cmmsRequisition = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
              blobUri = secondBlobUri
              blobTypeUrl = "another-blob-type-url"
              groupId = "another-existing-group-id"
              report = "another-report-name"
            }
          }
        )

      val requisition2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                            startTime =
                              TestRequisitionData.TIME_RANGE.start
                                .plus(1, ChronoUnit.HOURS)
                                .toProtoTime()
                            endTime =
                              TestRequisitionData.TIME_RANGE.endExclusive
                                .plus(1, ChronoUnit.HOURS)
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
          this.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = requisitionSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
        }

      val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
        requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
      }
      assertThat(groupedRequisitions).hasSize(2)
      val groupedRequisition = groupedRequisitions[0]
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
          groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
        )
        .isEqualTo(listOf(TestRequisitionData.REQUISITION))
      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      val newGroupedRequisition = groupedRequisitions[1]
      assertThat(newGroupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals +=
                listOf(
                  interval {
                    startTime =
                      TestRequisitionData.TIME_RANGE.start.plus(1, ChronoUnit.HOURS).toProtoTime()
                    endTime =
                      TestRequisitionData.TIME_RANGE.endExclusive
                        .plus(1, ChronoUnit.HOURS)
                        .toProtoTime()
                  }
                )
            }
          }
        )
      assertThat(
          newGroupedRequisition.requisitionsList.map {
            it.requisition.unpack(Requisition::class.java)
          }
        )
        .isEqualTo(listOf(requisition2))

      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")
      assertThat(createRequisitionMetadataRequests).hasSize(0)
      assertThat(refuseRequisitionRequests).hasSize(0)
    }

  @Test
  fun `existing requisition metadata with existing blob storage resolves into a single grouped requisitions`() =
    runBlocking {
      val storageClient = FileSystemStorageClient(tempFolder.root)
      val createdBlobUri = "$BLOB_URI_PREFIX/$STORAGE_PATH_PREFIX/an-existing-group-id"
      val blobKey = "$STORAGE_PATH_PREFIX/an-existing-group-id"
      val defaultGroupedRequisitions = GroupedRequisitions.getDefaultInstance()

      storageClient.writeBlob(blobKey, Any.pack(defaultGroupedRequisitions).toByteString())

      whenever(requisitionMetadataServiceMock.listRequisitionMetadata(any()))
        .thenReturn(
          listRequisitionMetadataResponse {
            requisitionMetadata += requisitionMetadata {
              state = RequisitionMetadata.State.STORED
              cmmsCreateTime = timestamp { seconds = 12345 }
              cmmsRequisition = TestRequisitionData.REQUISITION.name
              blobUri = createdBlobUri
              blobTypeUrl = "some-blob-type-url"
              groupId = "an-existing-group-id"
              report = "report-name"
            }
          }
        )

      val requisition2 =
        TestRequisitionData.REQUISITION.copy {
          name = "${TestRequisitionData.EDP_NAME}/requisitions/foo2"
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
                            startTime =
                              TestRequisitionData.TIME_RANGE.start
                                .plus(1, ChronoUnit.HOURS)
                                .toProtoTime()
                            endTime =
                              TestRequisitionData.TIME_RANGE.endExclusive
                                .plus(1, ChronoUnit.HOURS)
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
          this.encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signedMessage { message = requisitionSpec.pack() },
              TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
            )
        }

      val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
        requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
      }
      assertThat(groupedRequisitions).hasSize(1)
      val groupedRequisition = groupedRequisitions.single()
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            details = eventGroupDetails {
              eventGroupReferenceId = "some-event-group-reference-id"
              collectionIntervals +=
                listOf(
                  interval {
                    startTime =
                      TestRequisitionData.TIME_RANGE.start.plus(1, ChronoUnit.HOURS).toProtoTime()
                    endTime =
                      TestRequisitionData.TIME_RANGE.endExclusive
                        .plus(1, ChronoUnit.HOURS)
                        .toProtoTime()
                  }
                )
            }
          }
        )
      assertThat(
          groupedRequisition.requisitionsList.map { it.requisition.unpack(Requisition::class.java) }
        )
        .isEqualTo(listOf(requisition2))

      assertThat(groupedRequisition.modelLine).isEqualTo("some-model-line")

      assertThat(createRequisitionMetadataRequests).hasSize(1)
    }

  @Test
  fun `does not combine disparate time intervals`() {
    val requisition2 =
      TestRequisitionData.REQUISITION.copy {
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
                          startTime =
                            TestRequisitionData.TIME_RANGE.start
                              .plus(100, ChronoUnit.HOURS)
                              .toProtoTime()
                          endTime =
                            TestRequisitionData.TIME_RANGE.endExclusive
                              .plus(100, ChronoUnit.HOURS)
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
        this.encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signedMessage { message = requisitionSpec.pack() },
            TestRequisitionData.DATA_PROVIDER_PUBLIC_KEY,
          )
      }

    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
    }
    assertThat(groupedRequisitions).hasSize(1)
    assertThat(
        groupedRequisitions.single().eventGroupMapList.single().details.collectionIntervalsList
      )
      .hasSize(2)
  }

  @Test
  fun `skips if multiple model ids are used for the same report id`() {
    val requisition2 =
      TestRequisitionData.REQUISITION.copy {
        val measurementSpec =
          TestRequisitionData.MEASUREMENT_SPEC.copy { modelLine = "some-other-model-line" }
        this.measurementSpec =
          signMeasurementSpec(measurementSpec, TestRequisitionData.MC_SIGNING_KEY)
      }

    val groupedRequisitions: List<GroupedRequisitions> = runBlocking {
      requisitionGrouper.groupRequisitions(listOf(TestRequisitionData.REQUISITION, requisition2))
    }
    assertThat(groupedRequisitions).hasSize(0)
    assertThat(createRequisitionMetadataRequests).hasSize(2)
    assertThat(refuseRequisitionMetadataRequests).hasSize(2)
  }

  companion object {
    private const val STORAGE_PATH_PREFIX = "test-requisitions"
    private const val BLOB_URI_PREFIX = "file:///my-bucket"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
  }
}
