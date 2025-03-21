/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.testing.HeaderCapturingInterceptor
import org.wfanet.measurement.common.testing.verifyProtoArgument

@RunWith(JUnit4::class)
class EventGroupMetadataDescriptorsServiceTest {
  private val publicKingdomEventGroupMetadataDescriptorsMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { getEventGroupMetadataDescriptor(any()) }
        .thenReturn(EVENT_GROUP_METADATA_DESCRIPTOR)
      onBlocking { batchGetEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          batchGetEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR_2
          }
        )
    }
  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doReturn
      checkPermissionsResponse {
        permissions +=
          EventGroupMetadataDescriptorsService.GET_DESCRIPTOR_PERMISSIONS.map { "permissions/$it" }
      }
  }

  private val headerCapturingInterceptor = HeaderCapturingInterceptor()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(
      publicKingdomEventGroupMetadataDescriptorsMock.withInterceptor(headerCapturingInterceptor)
    )
    addService(permissionsServiceMock)
  }

  private lateinit var service: EventGroupMetadataDescriptorsService

  @Before
  fun initService() {
    val authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))
    service =
      EventGroupMetadataDescriptorsService(
        EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel),
        authorization,
        API_AUTHENTICATION_KEY,
      )
  }

  @Test
  fun `getEventGroupMetadataDescriptor returns eventGroupMetadataDescriptor`() = runBlocking {
    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking {
          service.getEventGroupMetadataDescriptor(
            getEventGroupMetadataDescriptorRequest { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
          )
        }
      }

    assertThat(response).isEqualTo(EVENT_GROUP_METADATA_DESCRIPTOR)

    assertThat(
        headerCapturingInterceptor
          .captured(EventGroupMetadataDescriptorsGrpcKt.getEventGroupMetadataDescriptorMethod)
          .single()
          .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
      )
      .isEqualTo(API_AUTHENTICATION_KEY)

    verifyProtoArgument(
        publicKingdomEventGroupMetadataDescriptorsMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::getEventGroupMetadataDescriptor,
      )
      .isEqualTo(
        getEventGroupMetadataDescriptorRequest { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
      )
  }

  @Test
  fun `getEventGroupMetadataDescriptor throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getEventGroupMetadataDescriptor(
            getEventGroupMetadataDescriptorRequest { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
          )
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getEventGroupMetadataDescriptor throws NOT_FOUND when kingdom returns not found`() {
    runBlocking {
      whenever(
          publicKingdomEventGroupMetadataDescriptorsMock.getEventGroupMetadataDescriptor(any())
        )
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val exception =
      assertFailsWith<StatusException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.getEventGroupMetadataDescriptor(
              getEventGroupMetadataDescriptorRequest { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors returns eventGroupMetadataDescriptors`() =
    runBlocking {
      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.batchGetEventGroupMetadataDescriptors(
              batchGetEventGroupMetadataDescriptorsRequest {
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
              }
            )
          }
        }

      assertThat(response.eventGroupMetadataDescriptorsList)
        .containsExactly(EVENT_GROUP_METADATA_DESCRIPTOR, EVENT_GROUP_METADATA_DESCRIPTOR_2)

      assertThat(
          headerCapturingInterceptor
            .captured(
              EventGroupMetadataDescriptorsGrpcKt.batchGetEventGroupMetadataDescriptorsMethod
            )
            .single()
            .get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)
        )
        .isEqualTo(API_AUTHENTICATION_KEY)

      verifyProtoArgument(
          publicKingdomEventGroupMetadataDescriptorsMock,
          EventGroupMetadataDescriptorsCoroutineImplBase::batchGetEventGroupMetadataDescriptors,
        )
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          batchGetEventGroupMetadataDescriptorsRequest {
            parent = DataProviderKey("-").toName()
            names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
          }
        )
    }

  @Test
  fun `batchGetEventGroupMetadataDescriptors throws PERMISSION_DENIED when principal is wrong`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking {
            service.batchGetEventGroupMetadataDescriptors(
              batchGetEventGroupMetadataDescriptorsRequest {
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.batchGetEventGroupMetadataDescriptors(
            batchGetEventGroupMetadataDescriptorsRequest {
              names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
            }
          )
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `batchGetEventGroupMetadataDescriptors throws NOT_FOUND when kingdom returns not found`() {
    runBlocking {
      whenever(
          publicKingdomEventGroupMetadataDescriptorsMock.batchGetEventGroupMetadataDescriptors(
            any()
          )
        )
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val exception =
      assertFailsWith<StatusException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking {
            service.batchGetEventGroupMetadataDescriptors(
              batchGetEventGroupMetadataDescriptorsRequest {
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME
                names += EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  companion object {
    private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"
    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = EventGroupMetadataDescriptorsService.GET_DESCRIPTOR_PERMISSIONS

    private const val DATA_PROVIDER_ID = "1235"
    private val TEST_MESSAGE = testMetadataMessage { publisherId = 15 }
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      EventGroupMetadataDescriptorKey(DATA_PROVIDER_ID, "1236").toName()
    private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
      descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
    }
    private val EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2 =
      EventGroupMetadataDescriptorKey(DATA_PROVIDER_ID, "1237").toName()
    private val EVENT_GROUP_METADATA_DESCRIPTOR_2 = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME_2
      descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
    }
  }
}
