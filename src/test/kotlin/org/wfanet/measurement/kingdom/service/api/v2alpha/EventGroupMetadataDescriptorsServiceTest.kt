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
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor as InternalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor as internalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest as internalGetEventGroupMetadataDescriptorRequest

@RunWith(JUnit4::class)
class EventGroupMetadataDescriptorsServiceTest {

  private val internalEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { getEventGroupMetadataDescriptor(any()) }
        .thenReturn(INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenReturn(INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { updateEventGroupMetadataDescriptor(any()) }
        .thenReturn(INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { streamEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR,
            INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR.copy {
              externalEventGroupMetadataDescriptorId =
                EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID_2.value
            }
          )
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalEventGroupMetadataDescriptorsMock)
  }

  private lateinit var service: EventGroupMetadataDescriptorsService

  @Before
  fun initService() {
    service =
      EventGroupMetadataDescriptorsService(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `getEventGroupMetadataDescriptor with data provider returns descriptor`() {
    val request = getEventGroupMetadataDescriptorRequest {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.getEventGroupMetadataDescriptor(request) }
      }

    val expected = EVENT_GROUP_METADATA_DESCRIPTOR

    verifyProtoArgument(
        internalEventGroupMetadataDescriptorsMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::getEventGroupMetadataDescriptor
      )
      .isEqualTo(
        internalGetEventGroupMetadataDescriptorRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID.value
          externalEventGroupMetadataDescriptorId = EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID.value
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getEventGroupMetadataDescriptor with measurement consumer returns descriptor`() {
    val request = getEventGroupMetadataDescriptorRequest {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.getEventGroupMetadataDescriptor(request) }
      }

    val expected = EVENT_GROUP_METADATA_DESCRIPTOR

    verifyProtoArgument(
        internalEventGroupMetadataDescriptorsMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::getEventGroupMetadataDescriptor
      )
      .isEqualTo(
        internalGetEventGroupMetadataDescriptorRequest {
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID.value
          externalEventGroupMetadataDescriptorId = EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID.value
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getEventGroupMetadataDescriptor throws PERMISSION_DENIED when edp doesn't match`() {
    val request = getEventGroupMetadataDescriptorRequest {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.getEventGroupMetadataDescriptor(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getEventGroupMetadataDescriptor throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.getEventGroupMetadataDescriptor(getEventGroupMetadataDescriptorRequest {})
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createEventGroupMetadataDescriptor returns descriptor`() {
    val request = createEventGroupMetadataDescriptorRequest {
      parent = DATA_PROVIDER_NAME
      eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
      requestId = "type.googleapis.com/example.MetadataMessage"
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.createEventGroupMetadataDescriptor(request) }
      }

    val expected = EVENT_GROUP_METADATA_DESCRIPTOR

    verifyProtoArgument(
        internalEventGroupMetadataDescriptorsMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::createEventGroupMetadataDescriptor
      )
      .isEqualTo(
        INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR.copy {
          clearExternalEventGroupMetadataDescriptorId()
          idempotencyKey = request.requestId
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createEventGroupMetadataDescriptor throws PERMISSION_DENIED when edp doesn't match`() {
    val request = createEventGroupMetadataDescriptorRequest {
      parent = DATA_PROVIDER_NAME
      eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.createEventGroupMetadataDescriptor(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createEventGroupMetadataDescriptor throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking {
            service.createEventGroupMetadataDescriptor(
              createEventGroupMetadataDescriptorRequest {
                eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `updateEventGroupMetadataDescriptor returns descriptor`() {
    val request = updateEventGroupMetadataDescriptorRequest {
      this.eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.updateEventGroupMetadataDescriptor(request) }
      }

    val expected = EVENT_GROUP_METADATA_DESCRIPTOR

    verifyProtoArgument(
        internalEventGroupMetadataDescriptorsMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::updateEventGroupMetadataDescriptor
      )
      .isEqualTo(
        org.wfanet.measurement.internal.kingdom.updateEventGroupMetadataDescriptorRequest {
          eventGroupMetadataDescriptor = INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR.copy {}
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `updateEventGroupMetadataDescriptor throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.updateEventGroupMetadataDescriptor(
            updateEventGroupMetadataDescriptorRequest {
              eventGroupMetadataDescriptor =
                EVENT_GROUP_METADATA_DESCRIPTOR.toBuilder().apply { clearName() }.build()
            }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("EventGroupMetadataDescriptor name is either unspecified or invalid")
  }

  @Test
  fun `updateEventGroup throws UNAUTHENTICATED when no principal is found`() {
    val request = updateEventGroupMetadataDescriptorRequest {
      this.eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.updateEventGroupMetadataDescriptor(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `updateEventGroup throws PERMISSION_DENIED when principal without authorization is found`() {
    val request = updateEventGroupMetadataDescriptorRequest {
      this.eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME_2) {
          runBlocking { service.updateEventGroupMetadataDescriptor(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors returns descriptors in input order`() {
    val request = batchGetEventGroupMetadataDescriptorsRequest {
      parent = DATA_PROVIDER_NAME
      names += listOf(EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2, EVENT_GROUP_METADATA_DESCRIPTOR_NAME)
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.batchGetEventGroupMetadataDescriptors(request) }
      }

    val expected = batchGetEventGroupMetadataDescriptorsResponse {
      eventGroupMetadataDescriptors +=
        listOf(
          EVENT_GROUP_METADATA_DESCRIPTOR.copy { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2 },
          EVENT_GROUP_METADATA_DESCRIPTOR
        )
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors returns descriptors for multiple DataProviders`() {
    internalEventGroupMetadataDescriptorsMock.stub {
      onBlocking { streamEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR,
            INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR.copy {
              externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID_2.value
              externalEventGroupMetadataDescriptorId =
                EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID_2.value
            }
          )
        )
    }
    val eventGroupMetadataDescriptor2Key =
      EventGroupMetadataDescriptorKey(
        DATA_PROVIDER_EXTERNAL_ID_2.apiId.value,
        EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID_2.apiId.value
      )
    val request = batchGetEventGroupMetadataDescriptorsRequest {
      parent = "dataProviders/-"
      names +=
        listOf(EVENT_GROUP_METADATA_DESCRIPTOR_NAME, eventGroupMetadataDescriptor2Key.toName())
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.batchGetEventGroupMetadataDescriptors(request) }
      }

    assertThat(result)
      .isEqualTo(
        batchGetEventGroupMetadataDescriptorsResponse {
          eventGroupMetadataDescriptors +=
            listOf(
              EVENT_GROUP_METADATA_DESCRIPTOR,
              EVENT_GROUP_METADATA_DESCRIPTOR.copy {
                name = eventGroupMetadataDescriptor2Key.toName()
              }
            )
        }
      )
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors throws NOT_FOUND if descriptor not found`() {
    val eventGroupMetadataDescriptorName3 =
      "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/AAAAAAAACHs"
    val request = batchGetEventGroupMetadataDescriptorsRequest {
      parent = DATA_PROVIDER_NAME
      names += listOf(EVENT_GROUP_METADATA_DESCRIPTOR_NAME, eventGroupMetadataDescriptorName3)
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchGetEventGroupMetadataDescriptors(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors throws PERMISSION_DENIED when edp doesn't match`() {
    val request = batchGetEventGroupMetadataDescriptorsRequest {
      parent = DATA_PROVIDER_NAME_2
      names += listOf(EVENT_GROUP_METADATA_DESCRIPTOR_NAME)
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.batchGetEventGroupMetadataDescriptors(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  companion object {
    private val DATA_PROVIDER_EXTERNAL_ID = ExternalId(123L)
    private val DATA_PROVIDER_EXTERNAL_ID_2 = ExternalId(124L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_EXTERNAL_ID.apiId.value)
    private val DATA_PROVIDER_KEY_2 = DataProviderKey(DATA_PROVIDER_EXTERNAL_ID_2.apiId.value)
    private val DATA_PROVIDER_NAME = DATA_PROVIDER_KEY.toName()
    private val DATA_PROVIDER_NAME_2 = DATA_PROVIDER_KEY_2.toName()

    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"

    private val EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID = ExternalId(456L)
    private val EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID_2 = ExternalId(789L)
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      EventGroupMetadataDescriptorKey(
          DATA_PROVIDER_KEY.dataProviderId,
          EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID.apiId.value
        )
        .toName()
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2 =
      EventGroupMetadataDescriptorKey(
          DATA_PROVIDER_KEY.dataProviderId,
          EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID_2.apiId.value
        )
        .toName()
    private val FILE_DESCRIPTOR_SET = FileDescriptorSet.getDefaultInstance()
    private val API_VERSION = Version.V2_ALPHA

    private val EVENT_GROUP_METADATA_DESCRIPTOR: EventGroupMetadataDescriptor =
      eventGroupMetadataDescriptor {
        name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
        descriptorSet = FILE_DESCRIPTOR_SET
      }

    private val INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR: InternalEventGroupMetadataDescriptor =
      internalEventGroupMetadataDescriptor {
        externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID.value
        externalEventGroupMetadataDescriptorId = EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID.value

        details = details {
          apiVersion = API_VERSION.string
          descriptorSet = FILE_DESCRIPTOR_SET
        }
      }
  }
}
