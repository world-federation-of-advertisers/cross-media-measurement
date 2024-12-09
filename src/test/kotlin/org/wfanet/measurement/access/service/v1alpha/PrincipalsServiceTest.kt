/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.rpc.errorInfo
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
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.access.service.Errors
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.PrincipalKt as InternalPrincipalKt
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt as InternalPrincipalsGrpcKt
import org.wfanet.measurement.internal.access.getPrincipalRequest as internalGetPrincipalRequest
import org.wfanet.measurement.internal.access.principal as internalPrincipal

@RunWith(JUnit4::class)
class PrincipalsServiceTest {
  private val internalServiceMock =
    mockService<InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: PrincipalsService

  @Before
  fun initService() {
    service =
      PrincipalsService(InternalPrincipalsGrpcKt.PrincipalsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `getPrincipal returns user Principal`() = runBlocking {
    val internalPrincipal = internalPrincipal {
      principalResourceId = "user-1"
      user =
        InternalPrincipalKt.oAuthUser {
          issuer = "example.com"
          subject = "user1@example.com"
        }
    }
    internalServiceMock.stub { onBlocking { getPrincipal(any()) } doReturn internalPrincipal }

    val request = getPrincipalRequest {
      name = "principals/${internalPrincipal.principalResourceId}"
    }
    val response = service.getPrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::getPrincipal,
      )
      .isEqualTo(
        internalGetPrincipalRequest { principalResourceId = internalPrincipal.principalResourceId }
      )
    assertThat(response)
      .isEqualTo(
        principal {
          name = request.name
          user =
            PrincipalKt.oAuthUser {
              issuer = internalPrincipal.user.issuer
              subject = internalPrincipal.user.subject
            }
        }
      )
  }

  @Test
  fun `getPrincipal returns TLS client Principal`() = runBlocking {
    val internalPrincipal = internalPrincipal {
      principalResourceId = "user-1"
      tlsClient =
        InternalPrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
    }
    internalServiceMock.stub { onBlocking { getPrincipal(any()) } doReturn internalPrincipal }

    val request = getPrincipalRequest {
      name = "principals/${internalPrincipal.principalResourceId}"
    }
    val response = service.getPrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::getPrincipal,
      )
      .isEqualTo(
        internalGetPrincipalRequest { principalResourceId = internalPrincipal.principalResourceId }
      )
    assertThat(response)
      .isEqualTo(
        principal {
          name = request.name
          tlsClient =
            PrincipalKt.tlsClient {
              authorityKeyIdentifier = internalPrincipal.tlsClient.authorityKeyIdentifier
            }
        }
      )
  }

  @Test
  fun `getPrincipal throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getPrincipal(any()) } doThrow
        PrincipalNotFoundException("user-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = getPrincipalRequest { name = "principals/user-1" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getPrincipal(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND.name
          metadata[Errors.Metadata.PRINCIPAL.key] = request.name
        }
      )
  }

  @Test
  fun `getPrincipal throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPrincipal(GetPrincipalRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getPrincipal throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPrincipal(getPrincipalRequest { name = "principles/user-1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }
}
