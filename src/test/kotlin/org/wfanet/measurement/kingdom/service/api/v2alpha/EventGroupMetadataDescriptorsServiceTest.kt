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
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor as InternalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorKt.details
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor as internalEventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.getEventGroupMetadataDescriptorRequest as internalGetEventGroupMetadataDescriptorRequest

private val DATA_PROVIDER_NAME = makeDataProvider(123L)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(124L)
private val DATA_PROVIDER_EXTERNAL_ID =
  apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)

private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
  "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/AAAAAAAAAHs"
private val FILE_DESCRIPTOR_SET = FileDescriptorSet.getDefaultInstance()
private val API_VERSION = Version.V2_ALPHA
private val EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID =
  apiIdToExternalId(
    EventGroupMetadataDescriptorKey.fromName(EVENT_GROUP_METADATA_DESCRIPTOR_NAME)!!
      .eventGroupMetadataDescriptorId
  )

private val EVENT_GROUP_METADATA_DESCRIPTOR: EventGroupMetadataDescriptor =
  eventGroupMetadataDescriptor {
    name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
    descriptorSet = FILE_DESCRIPTOR_SET
  }

private val INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR: InternalEventGroupMetadataDescriptor =
  internalEventGroupMetadataDescriptor {
    externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
    externalEventGroupMetadataDescriptorId = EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID

    details = details {
      apiVersion = API_VERSION.string
      descriptorSet = FILE_DESCRIPTOR_SET
    }
  }

@RunWith(JUnit4::class)
class EventGroupMetadataDescriptorsServiceTest {

  private val internalEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService() {
      onBlocking { getEventGroupMetadataDescriptor(any()) }
        .thenReturn(INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenReturn(INTERNAL_EVENT_GROUP_METADATA_DESCRIPTOR)
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
  fun `getEventGroupMetadataDescriptor returns descriptor`() {
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
          externalDataProviderId = DATA_PROVIDER_EXTERNAL_ID
          externalEventGroupMetadataDescriptorId = EVENT_GROUP_METADATA_DESCRIPTOR_EXTERNAL_ID
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
}
