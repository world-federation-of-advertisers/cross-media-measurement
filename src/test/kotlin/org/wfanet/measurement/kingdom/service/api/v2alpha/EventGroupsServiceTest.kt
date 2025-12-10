/*
 * Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.BatchCreateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.ListEventGroupsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.batchCreateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.batchCreateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.encryptedMessage
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupDetailsKt
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MediaType as InternalMediaType
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsRequest as internalBatchCreateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchCreateEventGroupsResponse as internalBatchCreateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsRequest as internalBatchUpdateEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.batchUpdateEventGroupsResponse as internalBatchUpdateEventGroupsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createEventGroupRequest as internalCreateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest as internalDeleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.eventGroupDetails
import org.wfanet.measurement.internal.kingdom.eventGroupKey
import org.wfanet.measurement.internal.kingdom.eventTemplate
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest as internalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest as internalUpdateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundByMeasurementConsumerException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)
private val DATA_PROVIDER_EXTERNAL_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

private val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
private val EVENT_GROUP_NAME_2 = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAJs"
private val EVENT_GROUP_NAME_3 = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAKs"
private val EVENT_GROUP_NAME_4 = "$DATA_PROVIDER_NAME_2/eventGroups/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_CONSUMER_EVENT_GROUP_NAME =
  "$MEASUREMENT_CONSUMER_NAME/eventGroups/AAAAAAAAAHs"
private val ENCRYPTED_METADATA = encryptedMessage {
  ciphertext = ByteString.copyFromUtf8("encryptedMetadata")
  typeUrl = ProtoReflection.getTypeUrl(EventGroup.Metadata.getDescriptor())
}
private val API_VERSION = Version.V2_ALPHA

private val EVENT_GROUP_EXTERNAL_ID =
  apiIdToExternalId(EventGroupKey.fromName(EVENT_GROUP_NAME)!!.eventGroupId)
private val EVENT_GROUP_EXTERNAL_ID_2 =
  apiIdToExternalId(EventGroupKey.fromName(EVENT_GROUP_NAME_2)!!.eventGroupId)
private val EVENT_GROUP_EXTERNAL_ID_3 =
  apiIdToExternalId(EventGroupKey.fromName(EVENT_GROUP_NAME_3)!!.eventGroupId)
private val MEASUREMENT_CONSUMER_EXTERNAL_ID =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
  )
private val MEASUREMENT_CONSUMER_EXTERNAL_ID_2 =
  apiIdToExternalId(
    MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME_2)!!.measurementConsumerId
  )

private val MEASUREMENT_CONSUMER_PUBLIC_KEY = encryptionPublicKey {
  data = ByteString.copyFromUtf8("foodata")
}
private val VID_MODEL_LINES = listOf("model1", "model2")
private val EVENT_TEMPLATE_TYPES = listOf("type1", "type2")
private val EVENT_TEMPLATES =
  EVENT_TEMPLATE_TYPES.map { type -> EventGroupKt.eventTemplate { this.type = type } }
private val INTERNAL_EVENT_TEMPLATES =
  EVENT_TEMPLATE_TYPES.map { type -> eventTemplate { fullyQualifiedType = type } }

private val EVENT_GROUP: EventGroup = eventGroup {
  name = EVENT_GROUP_NAME
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = "aaa"
  measurementConsumerPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()
  // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this field.
  signedMeasurementConsumerPublicKey = signedMessage {
    message = this@eventGroup.measurementConsumerPublicKey
    data = message.value
  }
  vidModelLines.addAll(VID_MODEL_LINES)
  eventTemplates.addAll(EVENT_TEMPLATES)
  mediaTypes += MediaType.VIDEO
  eventGroupMetadata = eventGroupMetadata {
    adMetadata =
      EventGroupMetadataKt.adMetadata {
        campaignMetadata =
          EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
            brandName = "Blammo!"
            campaignName = "Log: Better Than Bad"
          }
      }
  }
  encryptedMetadata = ENCRYPTED_METADATA
  // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this field.
  serializedEncryptedMetadata = ENCRYPTED_METADATA.ciphertext
  state = EventGroup.State.ACTIVE
  dataAvailabilityInterval = interval {
    startTime = LocalDate.of(2025, 2, 6).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
    endTime = LocalDate.of(2025, 5, 4).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  }
}

private val DELETED_EVENT_GROUP: EventGroup = eventGroup {
  name = EVENT_GROUP_NAME
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupReferenceId = "aaa"
  state = EventGroup.State.DELETED
}

private val INTERNAL_EVENT_GROUP: InternalEventGroup = internalEventGroup {
  externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
  externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
  externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID
  providedEventGroupId = EVENT_GROUP.eventGroupReferenceId
  createTime = CREATE_TIME
  mediaTypes += InternalMediaType.VIDEO
  details = eventGroupDetails {
    apiVersion = API_VERSION.string
    measurementConsumerPublicKey = EVENT_GROUP.measurementConsumerPublicKey.value
    vidModelLines.addAll(VID_MODEL_LINES)
    eventTemplates.addAll(INTERNAL_EVENT_TEMPLATES)
    metadata =
      EventGroupDetailsKt.eventGroupMetadata {
        adMetadata =
          EventGroupDetailsKt.EventGroupMetadataKt.adMetadata {
            campaignMetadata =
              EventGroupDetailsKt.EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                brandName = EVENT_GROUP.eventGroupMetadata.adMetadata.campaignMetadata.brandName
                campaignName =
                  EVENT_GROUP.eventGroupMetadata.adMetadata.campaignMetadata.campaignName
              }
          }
      }
    encryptedMetadata = ENCRYPTED_METADATA.ciphertext
  }
  state = InternalEventGroup.State.ACTIVE
  dataAvailabilityInterval = EVENT_GROUP.dataAvailabilityInterval
}

private val INTERNAL_DELETED_EVENT_GROUP: InternalEventGroup = internalEventGroup {
  externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
  externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
  externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID
  providedEventGroupId = EVENT_GROUP.eventGroupReferenceId
  createTime = CREATE_TIME
  state = InternalEventGroup.State.DELETED
}

@RunWith(JUnit4::class)
class EventGroupsServiceTest {

  private val internalEventGroupsMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
    onBlocking { createEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
    onBlocking { batchCreateEventGroups(any()) }
      .thenReturn(
        internalBatchCreateEventGroupsResponse {
          eventGroups += INTERNAL_EVENT_GROUP
          eventGroups +=
            INTERNAL_EVENT_GROUP.copy {
              externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID_2
              externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
            }
        }
      )
    onBlocking { updateEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
    onBlocking { batchUpdateEventGroup(any()) }
      .thenReturn(
        internalBatchUpdateEventGroupsResponse {
          eventGroups += INTERNAL_EVENT_GROUP
          eventGroups +=
            INTERNAL_EVENT_GROUP.copy { externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2 }
        }
      )
    onBlocking { deleteEventGroup(any()) }.thenReturn(INTERNAL_DELETED_EVENT_GROUP)
    onBlocking { streamEventGroups(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_EVENT_GROUP,
          INTERNAL_EVENT_GROUP.copy { externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2 },
          INTERNAL_EVENT_GROUP.copy { externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_3 },
        )
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalEventGroupsMock) }

  private lateinit var service: EventGroupsService

  @Before
  fun initService() {
    service = EventGroupsService(EventGroupsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `getEventGroup returns event group when mc caller is found`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.getEventGroup(request) }
      }

    val expected = EVENT_GROUP

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(
        internalGetEventGroupRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getEventGroup returns event group when edp caller is found`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getEventGroup(request) }
      }

    val expected = EVENT_GROUP

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::getEventGroup)
      .isEqualTo(
        internalGetEventGroupRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getEventGroup throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.getEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getEventGroup throws PERMISSION_DENIED when mc caller doesn't match`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.getEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.getEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = getEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.getEventGroup(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getEventGroup throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getEventGroup(getEventGroupRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroup returns created event group`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP
      requestId = "foo"
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::createEventGroup)
      .isEqualTo(
        internalCreateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearExternalEventGroupId()
              clearState()
            }
          requestId = request.requestId
        }
      )
    assertThat(response).isEqualTo(EVENT_GROUP)
  }

  @Test
  fun `createEventGroup with legacy message returns created event group`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup =
        EVENT_GROUP.copy {
          clearMeasurementConsumerPublicKey()
          // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Remove this test.
          signedMeasurementConsumerPublicKey =
            signedMeasurementConsumerPublicKey.copy { clearMessage() }
        }
      requestId = "foo"
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::createEventGroup)
      .isEqualTo(
        internalCreateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearExternalEventGroupId()
              clearState()
            }
          requestId = request.requestId
        }
      )
    assertThat(response).isEqualTo(EVENT_GROUP)
  }

  @Test
  fun `createEventGroup returns event group when data_availability_interval start_time set`() {
    val dataAvailabilityInterval = interval { startTime = timestamp { seconds = 300 } }

    runBlocking {
      whenever(internalEventGroupsMock.createEventGroup(any()))
        .thenReturn(
          INTERNAL_EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
        )
    }

    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::createEventGroup)
      .isEqualTo(
        internalCreateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearExternalEventGroupId()
              clearState()
              this.dataAvailabilityInterval = dataAvailabilityInterval
            }
        }
      )
    assertThat(response)
      .isEqualTo(EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval })
  }

  @Test
  fun `createEventGroup returns event group when data_availability_interval end_time set`() {
    val dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 300 }
      endTime = timestamp { seconds = 400 }
    }

    runBlocking {
      whenever(internalEventGroupsMock.createEventGroup(any()))
        .thenReturn(
          INTERNAL_EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
        )
    }

    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::createEventGroup)
      .isEqualTo(
        internalCreateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearExternalEventGroupId()
              clearState()
              this.dataAvailabilityInterval = dataAvailabilityInterval
            }
        }
      )
    assertThat(response)
      .isEqualTo(EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval })
  }

  @Test
  fun `createEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createEventGroup(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createEventGroup throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.createEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(createEventGroupRequest { eventGroup = EVENT_GROUP })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT when measurement consumer is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup = EVENT_GROUP.copy { clearMeasurementConsumer() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT if encrypted_metadata is set without public key`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup =
                  EVENT_GROUP.copy {
                    clearMeasurementConsumerPublicKey()
                    clearSignedMeasurementConsumerPublicKey()
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT if data_availability_interval has no start_time`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup =
                  EVENT_GROUP.copy { dataAvailabilityInterval = Interval.getDefaultInstance() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("start_time required")
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT if data_availability_interval end_time bad`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup =
                  EVENT_GROUP.copy {
                    dataAvailabilityInterval = interval {
                      startTime = timestamp { seconds = 500 }
                      endTime = timestamp { seconds = 300 }
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("before")
  }

  @Test
  fun `batchCreateEventGroup returns created event groups`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP.copy { measurementConsumer = MEASUREMENT_CONSUMER_NAME_2 }
        requestId = "boo"
      }
    }

    val response: BatchCreateEventGroupsResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchCreateEventGroups(request) }
      }

    verifyProtoArgument(
        internalEventGroupsMock,
        EventGroupsCoroutineImplBase::batchCreateEventGroups,
      )
      .isEqualTo(
        internalBatchCreateEventGroupsRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          requests += internalCreateEventGroupRequest {
            eventGroup =
              INTERNAL_EVENT_GROUP.copy {
                clearCreateTime()
                clearExternalEventGroupId()
                clearState()
              }
            requestId = request.requestsList[0].requestId
          }
          requests += internalCreateEventGroupRequest {
            eventGroup =
              INTERNAL_EVENT_GROUP.copy {
                externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID_2
                externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
                clearCreateTime()
                clearExternalEventGroupId()
                clearState()
              }
            requestId = request.requestsList[1].requestId
          }
        }
      )
    assertThat(response)
      .isEqualTo(
        batchCreateEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups +=
            EVENT_GROUP.copy {
              name = EVENT_GROUP_NAME_2
              measurementConsumer = MEASUREMENT_CONSUMER_NAME_2
            }
        }
      )
  }

  @Test
  fun `batchCreateEventGroup throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchCreateEventGroups(
              batchCreateEventGroupsRequest {
                requests += createEventGroupRequest {
                  parent = DATA_PROVIDER_NAME
                  eventGroup = EVENT_GROUP
                }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateEventGroup throws INVALID_ARGUMENT when parent is malformed`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchCreateEventGroups(
              batchCreateEventGroupsRequest {
                parent = "malformed-parent-name"
                requests += createEventGroupRequest {
                  parent = DATA_PROVIDER_NAME
                  eventGroup = EVENT_GROUP
                }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchCreateEventGroups(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchCreateEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.batchCreateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateEventGroup throws PERMISSION_DENIED when edp caller doesn't match`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.batchCreateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchCreateEventGroup throws INVALID_ARGUMENT when parent and child DataProvider doesn't match`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME_2
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent and child")
  }

  @Test
  fun `batchCreateEventGroup throws INVALID_ARGUMENT when child event group isn't specified`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("event group is unspecified")
  }

  @Test
  fun `batchCreateEventGroup throws INVALID_ARGUMENT when child request id is duplicate in the batch`() {
    val request = batchCreateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP
        requestId = "foo"
      }
      requests += createEventGroupRequest {
        parent = DATA_PROVIDER_NAME
        eventGroup = EVENT_GROUP.copy { measurementConsumer = MEASUREMENT_CONSUMER_NAME_2 }
        requestId = "foo"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchCreateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("duplicate")
  }

  @Test
  fun `updateEventGroup returns event group`() {
    val request = updateEventGroupRequest { this.eventGroup = EVENT_GROUP }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.updateEventGroup(request) }
      }

    val expected = EVENT_GROUP

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::updateEventGroup)
      .isEqualTo(
        internalUpdateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearState()
            }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `updateEventGroup returns event group when data_availability_interval start_time set`() {
    val dataAvailabilityInterval = interval { startTime = timestamp { seconds = 300 } }

    runBlocking {
      whenever(internalEventGroupsMock.updateEventGroup(any()))
        .thenReturn(
          INTERNAL_EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
        )
    }

    val request = updateEventGroupRequest {
      eventGroup = EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.updateEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::updateEventGroup)
      .isEqualTo(
        internalUpdateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearState()
              this.dataAvailabilityInterval = dataAvailabilityInterval
            }
        }
      )
    assertThat(response)
      .isEqualTo(EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval })
  }

  @Test
  fun `updateEventGroup returns event group when data_availability_interval end_time set`() {
    val dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 300 }
      endTime = timestamp { seconds = 400 }
    }

    runBlocking {
      whenever(internalEventGroupsMock.updateEventGroup(any()))
        .thenReturn(
          INTERNAL_EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
        )
    }

    val request = updateEventGroupRequest {
      eventGroup = EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval }
    }

    val response: EventGroup =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.updateEventGroup(request) }
      }

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::updateEventGroup)
      .isEqualTo(
        internalUpdateEventGroupRequest {
          eventGroup =
            INTERNAL_EVENT_GROUP.copy {
              clearCreateTime()
              clearState()
              this.dataAvailabilityInterval = dataAvailabilityInterval
            }
        }
      )
    assertThat(response)
      .isEqualTo(EVENT_GROUP.copy { this.dataAvailabilityInterval = dataAvailabilityInterval })
  }

  @Test
  fun `updateEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = updateEventGroupRequest { eventGroup = EVENT_GROUP }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.updateEventGroup(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `updateEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = updateEventGroupRequest { eventGroup = EVENT_GROUP }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.updateEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updateEventGroup throws PERMISSION_DENIED when edp caller doesn't match `() {
    val request = updateEventGroupRequest { eventGroup = EVENT_GROUP }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.updateEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT when name is missing or invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup = EVENT_GROUP.toBuilder().apply { clearName() }.build()
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT if encrypted_metadata is set without public key`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup =
                  EVENT_GROUP.copy {
                    name = EVENT_GROUP_NAME
                    clearMeasurementConsumerPublicKey()
                    clearSignedMeasurementConsumerPublicKey()
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT if data_availability_interval has no start_time`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup =
                  EVENT_GROUP.copy { dataAvailabilityInterval = Interval.getDefaultInstance() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("start_time required")
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT if data_availability_interval end_time bad`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup =
                  EVENT_GROUP.copy {
                    dataAvailabilityInterval = interval {
                      startTime = timestamp { seconds = 500 }
                      endTime = timestamp { seconds = 300 }
                    }
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("before")
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT when measurement consumer is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup = EVENT_GROUP.copy { clearMeasurementConsumer() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteEventGroup returns event group when edp caller is found`() {
    val request = deleteEventGroupRequest { name = EVENT_GROUP_NAME }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.deleteEventGroup(request) }
      }

    val expected = DELETED_EVENT_GROUP

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::deleteEventGroup)
      .isEqualTo(
        internalDeleteEventGroupRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
        }
      )
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `deleteEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = deleteEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.deleteEventGroup(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `deleteEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = deleteEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.deleteEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteEventGroup throws PERMISSION_DENIED when edp caller does not match`() {
    val request = deleteEventGroupRequest { name = EVENT_GROUP_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.deleteEventGroup(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `deleteEventGroup throws INVALID_ARGUMENT when name is missing or invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.deleteEventGroup(deleteEventGroupRequest {}) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listEventGroups requests EventGroups by DataProvider`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 100
    }

    val response: ListEventGroupsResponse =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    assertThat(response)
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_3 }
        }
      )
    val internalRequest: StreamEventGroupsRequest = captureFirst {
      verify(internalEventGroupsMock).streamEventGroups(capture())
    }
    assertThat(internalRequest)
      .isEqualTo(
        streamEventGroupsRequest {
          filter =
            StreamEventGroupsRequestKt.filter { externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID }
          limit = request.pageSize + 1
          allowStaleReads = true
        }
      )
  }

  @Test
  fun `listEventGroups requests EventGroups by MeasurementConsumer`() {
    val request = listEventGroupsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = 100
      filter = filter {
        dataAvailabilityStartTimeOnOrBefore = EVENT_GROUP.dataAvailabilityInterval.startTime
        dataAvailabilityEndTimeOnOrAfter = EVENT_GROUP.dataAvailabilityInterval.endTime
      }
      orderBy =
        ListEventGroupsRequestKt.orderBy {
          field = ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
          descending = true
        }
    }

    val response: ListEventGroupsResponse =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    assertThat(response)
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_3 }
        }
      )
    val internalRequest: StreamEventGroupsRequest = captureFirst {
      verify(internalEventGroupsMock).streamEventGroups(capture())
    }
    assertThat(internalRequest)
      .isEqualTo(
        streamEventGroupsRequest {
          filter =
            StreamEventGroupsRequestKt.filter {
              externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID
              dataAvailabilityStartTimeOnOrBefore =
                request.filter.dataAvailabilityStartTimeOnOrBefore
              dataAvailabilityEndTimeOnOrAfter = request.filter.dataAvailabilityEndTimeOnOrAfter
            }
          orderBy =
            StreamEventGroupsRequestKt.orderBy {
              field = StreamEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
              descending = true
            }
          limit = request.pageSize + 1
          allowStaleReads = true
        }
      )
  }

  @Test
  fun `listEventGroups response includes next page token when there are more items`() {
    val request = listEventGroupsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      filter = filter {
        dataProviderIn += DATA_PROVIDER_NAME
        mediaTypesIntersect += MediaType.VIDEO
        dataAvailabilityStartTimeOnOrAfter = EVENT_GROUP.dataAvailabilityInterval.startTime
        dataAvailabilityEndTimeOnOrBefore = EVENT_GROUP.dataAvailabilityInterval.endTime
        metadataSearchQuery = "log"
      }
      pageSize = 2
    }

    val response: ListEventGroupsResponse =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    assertThat(response)
      .ignoringFields(ListEventGroupsResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
      .isEqualTo(
        listEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
        }
      )
    val internalRequest: StreamEventGroupsRequest = captureFirst {
      verify(internalEventGroupsMock).streamEventGroups(capture())
    }
    assertThat(internalRequest)
      .isEqualTo(
        streamEventGroupsRequest {
          filter =
            StreamEventGroupsRequestKt.filter {
              externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID
              externalDataProviderIdIn += DATA_PROVIDER_EXTERNAL_ID
              mediaTypesIntersect += InternalMediaType.VIDEO
              dataAvailabilityStartTimeOnOrAfter = request.filter.dataAvailabilityStartTimeOnOrAfter
              dataAvailabilityEndTimeOnOrBefore = request.filter.dataAvailabilityEndTimeOnOrBefore
              metadataSearchQuery = request.filter.metadataSearchQuery
            }
          limit = request.pageSize + 1
          allowStaleReads = true
        }
      )
    val nextPageToken = ListEventGroupsPageToken.parseFrom(response.nextPageToken.base64UrlDecode())
    assertThat(nextPageToken)
      .isEqualTo(
        listEventGroupsPageToken {
          externalMeasurementConsumerId = MEASUREMENT_CONSUMER_EXTERNAL_ID
          externalDataProviderIdIn += DATA_PROVIDER_EXTERNAL_ID
          mediaTypesIntersect += MediaType.VIDEO
          dataAvailabilityStartTimeOnOrAfter = request.filter.dataAvailabilityStartTimeOnOrAfter
          dataAvailabilityEndTimeOnOrBefore = request.filter.dataAvailabilityEndTimeOnOrBefore
          metadataSearchQuery = request.filter.metadataSearchQuery
          lastEventGroup = previousPageEnd {
            externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
            externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
            dataAvailabilityStartTime = EVENT_GROUP.dataAvailabilityInterval.startTime
          }
        }
      )
  }

  @Test
  fun `listEventGroups with page token gets the next page`() {
    val pageToken = listEventGroupsPageToken {
      externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
      externalMeasurementConsumerIdIn += MEASUREMENT_CONSUMER_EXTERNAL_ID
      mediaTypesIntersect += MediaType.VIDEO
      lastEventGroup = previousPageEnd {
        externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        dataAvailabilityStartTime = EVENT_GROUP.dataAvailabilityInterval.startTime
      }
    }
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      filter = filter {
        measurementConsumerIn += MEASUREMENT_CONSUMER_NAME
        mediaTypesIntersect += MediaType.VIDEO
      }
      this.pageToken = pageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    val nextPageToken =
      pageToken.copy {
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          dataAvailabilityStartTime = EVENT_GROUP.dataAvailabilityInterval.startTime
        }
      }
    val expected = listEventGroupsResponse {
      eventGroups += EVENT_GROUP
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
      this.nextPageToken = nextPageToken.toByteArray().base64UrlEncode()
    }

    val streamEventGroupsRequest: StreamEventGroupsRequest = captureFirst {
      verify(internalEventGroupsMock).streamEventGroups(capture())
    }

    assertThat(streamEventGroupsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamEventGroupsRequest {
          limit = request.pageSize + 1
          filter =
            StreamEventGroupsRequestKt.filter {
              externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
              externalMeasurementConsumerIdIn += MEASUREMENT_CONSUMER_EXTERNAL_ID
              mediaTypesIntersect += InternalMediaType.VIDEO
              after =
                StreamEventGroupsRequestKt.FilterKt.after {
                  eventGroupKey = eventGroupKey {
                    externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
                    externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
                  }
                  dataAvailabilityStartTime = EVENT_GROUP.dataAvailabilityInterval.startTime
                }
              // TODO(@SanjayVas): Stop writing the deprecated field once the replacement has been
              // available for at least one release.
              eventGroupKeyAfter = after.eventGroupKey
            }
          allowStaleReads = true
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listEventGroups uses default page size when unspecified`() {
    val request = listEventGroupsRequest { parent = DATA_PROVIDER_NAME }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listEventGroups(request) }
    }

    val streamEventGroupsRequest: StreamEventGroupsRequest = captureFirst {
      verify(internalEventGroupsMock).streamEventGroups(capture())
    }
    assertThat(streamEventGroupsRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(streamEventGroupsRequest { limit = 11 })
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when subsequent request params mismatch page token`() {
    val initialRequest = listEventGroupsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      filter = filter { dataProviderIn += DATA_PROVIDER_NAME }
      pageSize = 2
    }
    val initialResponse: ListEventGroupsResponse =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(initialRequest) }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listEventGroups(
              initialRequest.copy {
                showDeleted = true
                pageToken = initialResponse.nextPageToken
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("page")
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED when no principal is found`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurementConsumerIn += MEASUREMENT_CONSUMER_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listEventGroups(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when DataProvider principal mismatches`() {
    val request = listEventGroupsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when MeasurementConsumer principal mismatches`() {
    val request = listEventGroupsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED parent type mismatches`() {
    val request = listEventGroupsRequest { parent = DATA_PROVIDER_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listRequisitions throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(ListEventGroupsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when measurement consumer in filter is invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                filter = filter { measurementConsumerIn += "asdf" }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listEventGroups throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getEventGroup throws NOT_FOUND with event group name when event group not found`() {
    val internalApiException =
      EventGroupNotFoundException(
          ExternalId(DATA_PROVIDER_EXTERNAL_ID),
          ExternalId(EVENT_GROUP_EXTERNAL_ID),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND)
    internalEventGroupsMock.stub {
      onBlocking { getEventGroup(any()) } doThrow internalApiException
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.getEventGroup(getEventGroupRequest { name = EVENT_GROUP_NAME }) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.cause).hasMessageThat().isEqualTo(internalApiException.message)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = "halo.wfanet.org"
          reason = "EVENT_GROUP_NOT_FOUND"
          metadata["eventGroup"] = EVENT_GROUP_NAME
        }
      )
  }

  @Test
  fun `getEventGroup throws NOT_FOUND with event group name when event group not found by measurement consumer`() {
    internalEventGroupsMock.stub {
      onBlocking { getEventGroup(any()) }
        .thenThrow(
          EventGroupNotFoundByMeasurementConsumerException(
              ExternalId(MEASUREMENT_CONSUMER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.getEventGroup(
              getEventGroupRequest { name = MEASUREMENT_CONSUMER_EVENT_GROUP_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("eventGroup", MEASUREMENT_CONSUMER_EVENT_GROUP_NAME)
  }

  @Test
  fun `createEventGroup throws FAILED_PRECONDITION with measurement consumer name when measurement consumer not found`() {
    internalEventGroupsMock.stub {
      onBlocking { createEventGroup(any()) }
        .thenThrow(
          MeasurementConsumerNotFoundException(ExternalId(MEASUREMENT_CONSUMER_EXTERNAL_ID))
            .asStatusRuntimeException(
              Status.Code.FAILED_PRECONDITION,
              "MeasurementConsumer not found.",
            )
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup = EVENT_GROUP
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("measurementConsumer", MEASUREMENT_CONSUMER_NAME)
  }

  @Test
  fun `createEventGroup throws FAILED_PRECONDITION with data provider name when data provider not found`() {
    internalEventGroupsMock.stub {
      onBlocking { createEventGroup(any()) }
        .thenThrow(
          DataProviderNotFoundException(ExternalId(DATA_PROVIDER_EXTERNAL_ID))
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup = EVENT_GROUP
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("dataProvider", DATA_PROVIDER_NAME)
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT with original and provided measurement consumer ids when event group argument invalid`() {
    internalEventGroupsMock.stub {
      onBlocking { updateEventGroup(any()) }
        .thenThrow(
          EventGroupInvalidArgsException(
              ExternalId(MEASUREMENT_CONSUMER_EXTERNAL_ID),
              ExternalId(MEASUREMENT_CONSUMER_EXTERNAL_ID_2),
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT, "EventGroup invalid args.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(updateEventGroupRequest { eventGroup = EVENT_GROUP })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("originalMeasurementConsumer", MEASUREMENT_CONSUMER_NAME)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("providedMeasurementConsumer", MEASUREMENT_CONSUMER_NAME_2)
  }

  @Test
  fun `updateEventGroup throws NOT_FOUND with event group name when event group not found`() {
    internalEventGroupsMock.stub {
      onBlocking { updateEventGroup(any()) }
        .thenThrow(
          EventGroupNotFoundException(
              ExternalId(DATA_PROVIDER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(updateEventGroupRequest { eventGroup = EVENT_GROUP })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("eventGroup", EVENT_GROUP_NAME)
  }

  @Test
  fun `updateEventGroup throws NOT_FOUND with event group name when event group stated is DELETED`() {
    internalEventGroupsMock.stub {
      onBlocking { updateEventGroup(any()) }
        .thenThrow(
          EventGroupStateIllegalException(
              ExternalId(DATA_PROVIDER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
              InternalEventGroup.State.DELETED,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(updateEventGroupRequest { eventGroup = EVENT_GROUP })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("eventGroup", EVENT_GROUP_NAME)
  }

  @Test
  fun `deleteEventGroup throws NOT_FOUND with event group name when event group not found`() {
    internalEventGroupsMock.stub {
      onBlocking { deleteEventGroup(any()) }
        .thenThrow(
          EventGroupNotFoundException(
              ExternalId(DATA_PROVIDER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.deleteEventGroup(deleteEventGroupRequest { name = EVENT_GROUP_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("eventGroup", EVENT_GROUP_NAME)
  }

  @Test
  fun `deleteEventGroup throws NOT_FOUND with event group name when event group not found by measurement consumer`() {
    internalEventGroupsMock.stub {
      onBlocking { deleteEventGroup(any()) }
        .thenThrow(
          EventGroupNotFoundByMeasurementConsumerException(
              ExternalId(MEASUREMENT_CONSUMER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.deleteEventGroup(deleteEventGroupRequest { name = EVENT_GROUP_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("eventGroup", MEASUREMENT_CONSUMER_EVENT_GROUP_NAME)
  }

  @Test
  fun `deleteEventGroup throws NOT_FOUND with event group name when event group stated is DELETED`() {
    internalEventGroupsMock.stub {
      onBlocking { deleteEventGroup(any()) }
        .thenThrow(
          EventGroupStateIllegalException(
              ExternalId(DATA_PROVIDER_EXTERNAL_ID),
              ExternalId(EVENT_GROUP_EXTERNAL_ID),
              InternalEventGroup.State.DELETED,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "EventGroup not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.deleteEventGroup(deleteEventGroupRequest { name = EVENT_GROUP_NAME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("eventGroup", EVENT_GROUP_NAME)
  }

  @Test
  fun `batchUpdateEventGroup returns event groups`() {
    val request = batchUpdateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
      requests += updateEventGroupRequest {
        eventGroup = EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchUpdateEventGroups(request) }
      }

    verifyProtoArgument(
        internalEventGroupsMock,
        EventGroupsCoroutineImplBase::batchUpdateEventGroup,
      )
      .isEqualTo(
        internalBatchUpdateEventGroupsRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          requests += internalUpdateEventGroupRequest {
            eventGroup =
              INTERNAL_EVENT_GROUP.copy {
                clearCreateTime()
                clearState()
              }
          }
          requests += internalUpdateEventGroupRequest {
            eventGroup =
              INTERNAL_EVENT_GROUP.copy {
                externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
                clearCreateTime()
                clearState()
              }
          }
        }
      )

    assertThat(result)
      .isEqualTo(
        batchUpdateEventGroupsResponse {
          eventGroups += EVENT_GROUP
          eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
        }
      )
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for invalid parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = "invalid-parent"
                requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing event group in child requests`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                requests += UpdateEventGroupRequest.getDefaultInstance()
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for missing event group name in child requests`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                requests += updateEventGroupRequest {
                  eventGroup = EVENT_GROUP.copy { clearName() }
                }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for invalid event group name in child requests`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                requests += updateEventGroupRequest {
                  eventGroup = EVENT_GROUP.copy { name = "invalid-name" }
                }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws INVALID_ARGUMENT for different parent and child DataProvider`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.batchUpdateEventGroups(
              batchUpdateEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                requests += updateEventGroupRequest {
                  eventGroup = EVENT_GROUP.copy { name = EVENT_GROUP_NAME_4 }
                }
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchUpdateEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = batchUpdateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.batchUpdateEventGroups(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchUpdateEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = batchUpdateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.batchUpdateEventGroups(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchUpdateEventGroup throws PERMISSION_DENIED when edp caller doesn't match `() {
    val request = batchUpdateEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      requests += updateEventGroupRequest { eventGroup = EVENT_GROUP }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.batchUpdateEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }
}
