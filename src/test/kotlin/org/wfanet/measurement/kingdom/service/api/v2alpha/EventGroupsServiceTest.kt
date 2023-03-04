// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2.alpha.ListEventGroupsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.listEventGroupsPageToken
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupKt as internalEventGroupKt
import org.wfanet.measurement.internal.kingdom.EventGroupKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.deleteEventGroupRequest as internalDeleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup as internalEventGroup
import org.wfanet.measurement.internal.kingdom.getEventGroupRequest as internalGetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.streamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.updateEventGroupRequest as internalUpdateEventGroupRequest

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private const val DEFAULT_LIMIT = 50

private const val WILDCARD_NAME = "dataProviders/-"

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)
private val DATA_PROVIDER_EXTERNAL_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"

private val EVENT_GROUP_NAME = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAHs"
private val EVENT_GROUP_NAME_2 = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAJs"
private val EVENT_GROUP_NAME_3 = "$DATA_PROVIDER_NAME/eventGroups/AAAAAAAAAKs"
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumers/BBBBBBBBBHs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private val ENCRYPTED_METADATA = ByteString.copyFromUtf8("encryptedMetadata")
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

private val MEASUREMENT_CONSUMER_PUBLIC_KEY_DATA = ByteString.copyFromUtf8("foodata")
private val MEASUREMENT_CONSUMER_PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("foosig")
private val VID_MODEL_LINES = listOf("model1", "model2")
private val EVENT_TEMPLATE_TYPES = listOf("type1", "type2")
private val EVENT_TEMPLATES =
  EVENT_TEMPLATE_TYPES.map { type -> EventGroupKt.eventTemplate { this.type = type } }
private val INTERNAL_EVENT_TEMPLATES =
  EVENT_TEMPLATE_TYPES.map { type ->
    internalEventGroupKt.eventTemplate { fullyQualifiedType = type }
  }

private val EVENT_GROUP: EventGroup = eventGroup {
  name = EVENT_GROUP_NAME
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
  eventGroupReferenceId = "aaa"
  measurementConsumerPublicKey = signedData {
    data = MEASUREMENT_CONSUMER_PUBLIC_KEY_DATA
    signature = MEASUREMENT_CONSUMER_PUBLIC_KEY_SIGNATURE
  }
  vidModelLines.addAll(VID_MODEL_LINES)
  eventTemplates.addAll(EVENT_TEMPLATES)
  encryptedMetadata = ENCRYPTED_METADATA
  state = EventGroup.State.ACTIVE
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
  externalMeasurementConsumerCertificateId =
    apiIdToExternalId(
      MeasurementConsumerCertificateKey.fromName(MEASUREMENT_CONSUMER_CERTIFICATE_NAME)!!
        .certificateId
    )
  providedEventGroupId = EVENT_GROUP.eventGroupReferenceId
  createTime = CREATE_TIME
  details = details {
    apiVersion = API_VERSION.string
    measurementConsumerPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY_DATA
    measurementConsumerPublicKeySignature = MEASUREMENT_CONSUMER_PUBLIC_KEY_SIGNATURE
    vidModelLines.addAll(VID_MODEL_LINES)
    eventTemplates.addAll(INTERNAL_EVENT_TEMPLATES)
    encryptedMetadata = ENCRYPTED_METADATA
  }
  state = InternalEventGroup.State.ACTIVE
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

  private val internalEventGroupsMock: EventGroupsCoroutineImplBase =
    mockService() {
      onBlocking { getEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
      onBlocking { createEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
      onBlocking { updateEventGroup(any()) }.thenReturn(INTERNAL_EVENT_GROUP)
      onBlocking { deleteEventGroup(any()) }.thenReturn(INTERNAL_DELETED_EVENT_GROUP)
      onBlocking { streamEventGroups(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_EVENT_GROUP,
            INTERNAL_EVENT_GROUP.copy { externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2 },
            INTERNAL_EVENT_GROUP.copy { externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_3 }
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
  fun `createEventGroup returns event group`() {
    val request = createEventGroupRequest {
      parent = DATA_PROVIDER_NAME
      eventGroup = EVENT_GROUP
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroup(request) }
      }

    val expected = EVENT_GROUP

    verifyProtoArgument(internalEventGroupsMock, EventGroupsCoroutineImplBase::createEventGroup)
      .isEqualTo(
        INTERNAL_EVENT_GROUP.copy {
          clearCreateTime()
          clearExternalEventGroupId()
          clearState()
        }
      )

    assertThat(result).isEqualTo(expected)
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
                eventGroup = EVENT_GROUP.copy { clearMeasurementConsumerPublicKey() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroup throws INVALID_ARGUMENT if public key is set without certificate`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroup(
              createEventGroupRequest {
                parent = DATA_PROVIDER_NAME
                eventGroup = EVENT_GROUP.copy { clearMeasurementConsumerCertificate() }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updateEventGroup throws INVALID_ARGUMENT if public key is set without certificate`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.updateEventGroup(
              updateEventGroupRequest {
                eventGroup =
                  EVENT_GROUP.copy {
                    name = EVENT_GROUP_NAME
                    clearMeasurementConsumerCertificate()
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
  fun `listEventGroups with parent uses filter with parent`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    val expected = listEventGroupsResponse {
      eventGroups += EVENT_GROUP
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_3 }
    }

    val streamEventGroupsRequest =
      captureFirst<StreamEventGroupsRequest> {
        verify(internalEventGroupsMock).streamEventGroups(capture())
      }

    assertThat(streamEventGroupsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamEventGroupsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamEventGroupsRequestKt.filter {
              externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
              externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listEventGroups with page token gets the next page`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = 2
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
      }
      filter = filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
      pageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    val expected = listEventGroupsResponse {
      eventGroups += EVENT_GROUP
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = request.pageSize
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID_2
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
      }
      nextPageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    val streamEventGroupsRequest =
      captureFirst<StreamEventGroupsRequest> {
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
              externalDataProviderIdAfter = DATA_PROVIDER_EXTERNAL_ID
              externalEventGroupIdAfter = EVENT_GROUP_EXTERNAL_ID
              externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listEventGroups with new page size replaces page size in page token`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 4
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = 2
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
        externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
      }
      filter = filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
      pageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
      runBlocking { service.listEventGroups(request) }
    }

    val streamEventGroupsRequest =
      captureFirst<StreamEventGroupsRequest> {
        verify(internalEventGroupsMock).streamEventGroups(capture())
      }

    assertThat(streamEventGroupsRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(streamEventGroupsRequest { limit = request.pageSize + 1 })
  }

  @Test
  fun `listEventGroups with no page size uses page size in page token`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = 2
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
      }
      pageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listEventGroups(request) }
    }

    val streamEventGroupsRequest =
      captureFirst<StreamEventGroupsRequest> {
        verify(internalEventGroupsMock).streamEventGroups(capture())
      }

    assertThat(streamEventGroupsRequest)
      .comparingExpectedFieldsOnly()
      .isEqualTo(streamEventGroupsRequest { limit = 3 })
  }

  @Test
  fun `listEventGroups with parent and filter with measurement consumers uses filter with both`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter {
        measurementConsumers += MEASUREMENT_CONSUMER_NAME
        measurementConsumers += MEASUREMENT_CONSUMER_NAME
      }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listEventGroups(request) }
      }

    val expected = listEventGroupsResponse {
      eventGroups += EVENT_GROUP
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_2 }
      eventGroups += EVENT_GROUP.copy { name = EVENT_GROUP_NAME_3 }
    }

    val streamEventGroupsRequest =
      captureFirst<StreamEventGroupsRequest> {
        verify(internalEventGroupsMock).streamEventGroups(capture())
      }

    assertThat(streamEventGroupsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamEventGroupsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamEventGroupsRequestKt.filter {
              externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
              externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
              externalMeasurementConsumerIds += MEASUREMENT_CONSUMER_EXTERNAL_ID
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listEventGroups throws UNAUTHENTICATED when no principal is found`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter { measurementConsumers += MEASUREMENT_CONSUMER_NAME }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listEventGroups(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when edp caller doesn't match`() {
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
  fun `listEventGroups throws PERMISSION_DENIED when mc caller doesn't match filter MC`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      filter = filter {
        measurementConsumers += MEASUREMENT_CONSUMER_NAME
        measurementConsumers += "measurementConsumers/BBBAAAAAAHt"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listEventGroups throws PERMISSION_DENIED when mc caller and missing mc filter`() {
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
  fun `listEventGroups throws INVALID_ARGUMENT when only wildcard parent`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(listEventGroupsRequest { parent = WILDCARD_NAME }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking {
            service.listEventGroups(
              listEventGroupsRequest {
                parent = DATA_PROVIDER_NAME
                filter = filter { measurementConsumers += "asdf" }
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
  fun `listEventGroups throws invalid argument when parent doesn't match parent in page token`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = 2
        externalDataProviderId = 654
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
      }
      pageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listEventGroups throws invalid argument when mc ids don't match ids in page token`() {
    val request = listEventGroupsRequest {
      parent = DATA_PROVIDER_NAME
      pageSize = 2
      val listEventGroupsPageToken = listEventGroupsPageToken {
        pageSize = 2
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        externalMeasurementConsumerIds += 123
        lastEventGroup = previousPageEnd {
          externalEventGroupId = EVENT_GROUP_EXTERNAL_ID
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
        }
      }
      pageToken = listEventGroupsPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listEventGroups(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
