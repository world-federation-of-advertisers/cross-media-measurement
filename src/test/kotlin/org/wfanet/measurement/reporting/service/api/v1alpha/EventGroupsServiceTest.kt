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
import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
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
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.age
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.duration
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.name
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testParentMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as cmmsListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse as cmmsListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.reporting.v1alpha.eventGroup
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsResponse

private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }
private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private val ENCRYPTION_PRIVATE_KEY = loadEncryptionPrivateKey("mc_enc_private.tink")
private val ENCRYPTION_PUBLIC_KEY = loadEncryptionPublicKey("mc_enc_public.tink")
private const val MEASUREMENT_CONSUMER_REFERENCE_ID = "measurementConsumerRefId"
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID).toName()
private val ENCRYPTION_KEY_PAIR_STORE =
  InMemoryEncryptionKeyPairStore(
    mapOf(
      MEASUREMENT_CONSUMER_NAME to
        listOf(ENCRYPTION_PUBLIC_KEY.toByteString() to ENCRYPTION_PRIVATE_KEY)
    )
  )
private val TEST_MESSAGE = testMetadataMessage {
  name = name { value = "Bob" }
  age = age { value = 15 }
  duration = duration { value = 20 }
}
private const val CMMS_EVENT_GROUP_ID = "AAAAAAAAAHs"
private val CMMS_EVENT_GROUP = cmmsEventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID"
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
  measurementConsumerPublicKey = signedData {
    data = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().toByteString()
  }
  encryptedMetadata =
    encryptMetadata(
      CmmsEventGroup.metadata {
        eventGroupMetadataDescriptor = METADATA_NAME
        metadata = Any.pack(TEST_MESSAGE)
      },
      ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
    )
}
private val TEST_MESSAGE_2 = testMetadataMessage {
  name = name { value = "Alice" }
  age = age { value = 5 }
  duration = duration { value = 20 }
}
private const val CMMS_EVENT_GROUP_ID_2 = "AAAAAAAAAGs"
private val CMMS_EVENT_GROUP_2 = cmmsEventGroup {
  name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID_2"
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = "id2"
  measurementConsumerPublicKey = signedData {
    data = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().toByteString()
  }
  encryptedMetadata =
    encryptMetadata(
      CmmsEventGroup.metadata {
        eventGroupMetadataDescriptor = METADATA_NAME
        metadata = Any.pack(TEST_MESSAGE_2)
      },
      ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
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
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
  metadata = metadata {
    eventGroupMetadataDescriptor = METADATA_NAME
    metadata = Any.pack(TEST_MESSAGE)
  }
}
private const val PAGE_TOKEN = "base64encodedtoken"
private const val NEXT_PAGE_TOKEN = "base64encodedtoken2"
private const val DATA_PROVIDER_REFERENCE_ID = "123"
private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_REFERENCE_ID"
private const val EVENT_GROUP_REFERENCE_ID = "edpRefId1"
private const val EVENT_GROUP_PARENT =
  "measurementConsumers/$MEASUREMENT_CONSUMER_REFERENCE_ID/dataProviders/$DATA_PROVIDER_REFERENCE_ID"
private const val METADATA_NAME = "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
  name = METADATA_NAME
  descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
}

@RunWith(JUnit4::class)
class EventGroupsServiceTest {
  private val cmmsEventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
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
    mockService {
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
    cmmsEventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups +=
              listOf(
                CMMS_EVENT_GROUP,
                // When there's no filter applied to metadata, it doesn't need to be set on all EGs.
                CMMS_EVENT_GROUP_2.copy { clearEncryptedMetadata() }
              )
            nextPageToken = NEXT_PAGE_TOKEN
          }
        )
    }
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE
      )
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = EVENT_GROUP_PARENT
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
              }
            )
          nextPageToken = NEXT_PAGE_TOKEN
        }
      )

    val expectedCmmsEventGroupsRequest = cmmsListEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 10
      pageToken = PAGE_TOKEN
      filter = ListEventGroupsRequestKt.filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
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
        ENCRYPTION_KEY_PAIR_STORE,
        TypeRegistry.newBuilder().add(TestMetadataMessage.getDescriptor()).build()
      )
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = EVENT_GROUP_PARENT
              filter = "metadata.metadata.age.value > 10"
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
      filter = ListEventGroupsRequestKt.filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
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
      measurementConsumerPublicKey = signedData {
        data = ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey().toByteString()
      }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroup.metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(testParentMetadataMessage { name = "name" })
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
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
        ENCRYPTION_KEY_PAIR_STORE
      )
    val result =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            eventGroupsService.listEventGroups(
              listEventGroupsRequest {
                parent = EVENT_GROUP_PARENT
                filter = "metadata.metadata.age.value > 10"
              }
            )
          }
        }
      }

    assertThat(result.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `listEventGroups throws FAILED_PRECONDITION if private key not found`() {
    val eventGroupInvalidPublicKey = cmmsEventGroup {
      name = "$DATA_PROVIDER_NAME/eventGroups/$CMMS_EVENT_GROUP_ID"
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId = "id1"
      measurementConsumerPublicKey = signedData {
        data = encryptionPublicKey { data = ByteString.copyFromUtf8("consumerkey") }.toByteString()
      }
      encryptedMetadata =
        encryptMetadata(
          CmmsEventGroup.metadata {
            eventGroupMetadataDescriptor = METADATA_NAME
            metadata = Any.pack(testParentMetadataMessage { name = "name" })
          },
          ENCRYPTION_PUBLIC_KEY.toEncryptionPublicKey()
        )
    }
    cmmsEventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          cmmsListEventGroupsResponse {
            eventGroups += listOf(CMMS_EVENT_GROUP, CMMS_EVENT_GROUP_2, eventGroupInvalidPublicKey)
          }
        )
    }
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE
      )

    val result =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            eventGroupsService.listEventGroups(
              listEventGroupsRequest {
                parent = EVENT_GROUP_PARENT
                filter = "metadata.metadata.age.value > 10"
              }
            )
          }
        }
      }

    assertThat(result.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT if parent not specified`() {
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME, CONFIG) {
          runBlocking {
            eventGroupsService.listEventGroups(
              listEventGroupsRequest {
                filter = "metadata.metadata.age.value > 10"
                pageToken = PAGE_TOKEN
                ENCRYPTION_KEY_PAIR_STORE
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("parent")
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED if principal not found`() {
    val eventGroupsService =
      EventGroupsService(
        EventGroupsCoroutineStub(grpcTestServerRule.channel),
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE
      )
    val result =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          eventGroupsService.listEventGroups(
            listEventGroupsRequest {
              parent = EVENT_GROUP_PARENT
              filter = "metadata.metadata.age.value > 10"
            }
          )
        }
      }

    assertThat(result.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }
}

private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
  return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}

private fun loadEncryptionPublicKey(fileName: String): TinkPublicKeyHandle {
  return loadPublicKey(SECRET_FILES_PATH.resolve(fileName).toFile())
}
