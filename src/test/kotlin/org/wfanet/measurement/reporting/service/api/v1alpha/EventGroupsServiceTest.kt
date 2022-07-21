// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.age
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.duration
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.name
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testParentMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse as cmmsListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.common.signMessage
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private val ENCRYPTION_PRIVATE_KEY = loadEncryptionPrivateKey("mc_enc_private.tink")
private val ENCRYPTION_PUBLIC_KEY = loadEncryptionPublicKey("mc_enc_public.tink")
private val EDP_SIGNING_KEY = loadSigningKey("edp1_cs_cert.der", "edp1_cs_private.der")
private const val MEASUREMENT_CONSUMER_REFERENCE_ID = "measurementConsumerRefId"
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID).toName()
private val TEST_MESSAGE = testMetadataMessage {
  name = name { value = "Bob" }
  age = age { value = 15 }
  duration = duration { value = 20 }
}
private val CMMS_EVENT_GROUP_ID = "AAAAAAAAAHs"
private val CMMS_EVENT_GROUP = cmmsEventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID"
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = "id1"
  encryptedMetadata =
    ENCRYPTION_PUBLIC_KEY.hybridEncrypt(
      signMessage(
          CmmsEventGroup.metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(TEST_MESSAGE)
          },
          EDP_SIGNING_KEY
        )
        .toByteString()
    )
}
private val TEST_MESSAGE_2 = testMetadataMessage {
  name = name { value = "Alice" }
  age = age { value = 5 }
  duration = duration { value = 20 }
}
private val CMMS_EVENT_GROUP_ID_2 = "AAAAAAAAAGs"
private val CMMS_EVENT_GROUP_2 = cmmsEventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID_2"
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = "id2"
  encryptedMetadata =
    ENCRYPTION_PUBLIC_KEY.hybridEncrypt(
      signMessage(
          CmmsEventGroup.metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(TEST_MESSAGE_2)
          },
          EDP_SIGNING_KEY
        )
        .toByteString()
    )
}
private val EVENT_GROUP = eventGroup {
  name =
    EventGroupKey(
        MEASUREMENT_CONSUMER_REFERENCE_ID,
        DATA_PROVIDER_REFERENCE_ID,
        CMMS_EVENT_GROUP_ID
      )
      .toName()
  dataProvider = DATA_PROVIDER_NAME
  eventGroupReferenceId = "id1"
  metadata = metadata {
    eventGroupMetadataDescriptor = METADATA_NAME
    metadata = Any.pack(TEST_MESSAGE)
  }
}
private const val PAGE_TOKEN = "base64encodedtoken"
private const val NEXT_PAGE_TOKEN = "base64encodedtoken2"
private const val DATA_PROVIDER_REFERENCE_ID = "123"
private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_REFERENCE_ID"
private const val METADATA_NAME = "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
  name = METADATA_NAME
  descriptorSet = TEST_MESSAGE.descriptorForType.getFileDescriptorSet()
}

@RunWith(JUnit4::class)
class EventGroupsServiceTest {
  private val cmmsEventGroupsServiceMock: EventGroupsCoroutineImplBase =
    mockService() {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2)
            nextPageToken = NEXT_PAGE_TOKEN
          }
        )
    }
  private val cmmsEventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService() {
      onBlocking { batchGetEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          batchGetEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupsServiceMock)
    addService(cmmsEventGroupMetadataDescriptorsServiceMock)
  }

  @Test
  fun `listEventGroups returns list with no filter`() {
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_PRIVATE_KEY
      )
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = DATA_PROVIDER_NAME
              pageSize = 10
              pageToken = PAGE_TOKEN
            }
          )
        }
      }

    assertThat(result)
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups +=
            listOf(
              EVENT_GROUP,
              eventGroup {
                name =
                  EventGroupKey(
                      MeasurementConsumerKey.fromName(CMMS_EVENT_GROUP_2.measurementConsumer)!!
                        .measurementConsumerId,
                      DATA_PROVIDER_REFERENCE_ID,
                      CMMS_EVENT_GROUP_ID_2
                    )
                    .toName()
                dataProvider = DATA_PROVIDER_NAME
                eventGroupReferenceId = "id2"
                metadata = metadata {
                  eventGroupMetadataDescriptor = METADATA_NAME
                  metadata = Any.pack(TEST_MESSAGE_2)
                }
              }
            )
          nextPageToken = NEXT_PAGE_TOKEN
        }
      )

    val expectedCmmsEventGroupsRequest = cmmsListEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
      pageToken = PAGE_TOKEN
      filter =
        ListEventGroupsRequestKt.filter {
          measurementConsumers += MEASUREMENT_CONSUMER_REFERENCE_ID
        }
    }

    verifyProtoArgument(cmmsEventGroupsServiceMock, EventGroupsCoroutineImplBase::listEventGroups)
      .isEqualTo(expectedCmmsEventGroupsRequest)
  }

  @Test
  fun `listEventGroups returns list with filter`() {
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_PRIVATE_KEY
      )
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = DATA_PROVIDER_NAME
              filter = "age.value > 10"
              pageToken = PAGE_TOKEN
            }
          )
        }
      }

    assertThat(result)
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups += EVENT_GROUP
          nextPageToken = NEXT_PAGE_TOKEN
        }
      )

    val expectedCmmsMetadataDescriptorRequest = batchGetEventGroupMetadataDescriptorsRequest {
      parent = DATA_PROVIDER_NAME
      names += setOf(METADATA_NAME)
    }

    verifyProtoArgument(
        cmmsEventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::batchGetEventGroupMetadataDescriptors
      )
      .isEqualTo(expectedCmmsMetadataDescriptorRequest)

    val expectedCmmsEventGroupsRequest = cmmsListEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 0
      pageToken = PAGE_TOKEN
      filter =
        ListEventGroupsRequestKt.filter {
          measurementConsumers += MEASUREMENT_CONSUMER_REFERENCE_ID
        }
    }

    verifyProtoArgument(cmmsEventGroupsServiceMock, EventGroupsCoroutineImplBase::listEventGroups)
      .isEqualTo(expectedCmmsEventGroupsRequest)
  }

  @Test
  fun `listEventGroups throws FAILED_PRECONDITION if message descriptor not found`() {
    val eventGroupInvalidMetadata = cmmsEventGroup {
      name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID"
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = "id1"
      encryptedMetadata =
        ENCRYPTION_PUBLIC_KEY.hybridEncrypt(
          signMessage(
              CmmsEventGroup.metadata {
                eventGroupMetadataDescriptor = METADATA_NAME
                metadata = Any.pack(testParentMetadataMessage { name = "name" })
              },
              EDP_SIGNING_KEY
            )
            .toByteString()
        )
    }
    cmmsEventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2, eventGroupInvalidMetadata)
          }
        )
    }
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_PRIVATE_KEY
      )
    val result =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            eventGroupsService.listEventGroups(
              listEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                filter = "age.value > 10"
              }
            )
          }
        }
      }

    assertThat(result.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }
}

fun Descriptor.getFileDescriptorSet(): FileDescriptorSet {
  val fileDescriptors = mutableSetOf<FileDescriptor>()
  val toVisit = mutableListOf<FileDescriptor>(file)
  while (toVisit.isNotEmpty()) {
    val fileDescriptor = toVisit.removeLast()
    if (!fileDescriptors.contains(fileDescriptor)) {
      fileDescriptors.add(fileDescriptor)
      fileDescriptor.dependencies.forEach {
        if (!fileDescriptors.contains(it)) {
          toVisit.add(it)
        }
      }
    }
  }
  return FileDescriptorSet.newBuilder().addAllFile(fileDescriptors.map { it.toProto() }).build()
}

fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
  return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

fun loadEncryptionPublicKey(fileName: String): TinkPublicKeyHandle {
  return loadPublicKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
  return loadSigningKey(
    SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
    SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile()
  )
}
