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

package org.wfanet.measurement.access.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.Errors
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser
import org.wfanet.measurement.internal.access.PrincipalKt.tlsClient
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.createUserPrincipalRequest
import org.wfanet.measurement.internal.access.deletePrincipalRequest
import org.wfanet.measurement.internal.access.getPrincipalRequest
import org.wfanet.measurement.internal.access.lookupPrincipalRequest
import org.wfanet.measurement.internal.access.principal

@RunWith(JUnit4::class)
abstract class PrincipalsServiceTest {
  /** Initializes the service under test. */
  abstract fun initService(
    tlsClientMapping: TlsClientPrincipalMapping,
    idGenerator: IdGenerator,
  ): PrincipalsGrpcKt.PrincipalsCoroutineImplBase

  private fun initService(idGenerator: IdGenerator = IdGenerator.Default) =
    initService(TestConfig.TLS_CLIENT_MAPPING, idGenerator)

  @Test
  fun `getPrincipal returns TLS client principal`() {
    val service = initService()
    val request = getPrincipalRequest {
      principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
    }

    val response: Principal = runBlocking { service.getPrincipal(request) }

    assertThat(response)
      .isEqualTo(
        principal {
          principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
          tlsClient = tlsClient { authorityKeyIdentifier = TestConfig.AUTHORITY_KEY_IDENTIFIER }
        }
      )
  }

  @Test
  fun `createUserPrincipal returns created principal`() = runBlocking {
    val service = initService()
    val request = createUserPrincipalRequest {
      principalResourceId = "user-1"
      user = oAuthUser {
        issuer = "example-issuer"
        subject = "user@example.com"
      }
    }

    val principal: Principal = service.createUserPrincipal(request)

    assertThat(principal)
      .ignoringFields(Principal.CREATE_TIME_FIELD_NUMBER, Principal.UPDATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        principal {
          principalResourceId = request.principalResourceId
          user = request.user
        }
      )
    assertThat(principal.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    assertThat(principal.createTime).isEqualTo(principal.updateTime)
    assertThat(principal)
      .isEqualTo(
        service.getPrincipal(
          getPrincipalRequest { principalResourceId = request.principalResourceId }
        )
      )
  }

  @Test
  fun `createUserPrincipal retries ID generation if ID already in use`(): Unit = runBlocking {
    val principalId1 = 1234L
    val principalId2 = 2345L
    val idGeneratorMock =
      mock<IdGenerator> { on { generateId() }.thenReturn(principalId1, principalId1, principalId2) }
    val service = initService(idGeneratorMock)
    service.createUserPrincipal(
      createUserPrincipalRequest {
        principalResourceId = "user-1"
        user = oAuthUser {
          issuer = "example-issuer"
          subject = "user@example.com"
        }
      }
    )

    service.createUserPrincipal(
      createUserPrincipalRequest {
        principalResourceId = "user-2"
        user = oAuthUser {
          issuer = "example-issuer"
          subject = "user2@example.com"
        }
      }
    )

    verify(idGeneratorMock, times(3)).generateId()
  }

  @Test
  fun `createUserPrincipal throws ALREADY_EXISTS if Principal with resource ID already exists`() =
    runBlocking {
      val service = initService()
      val request = createUserPrincipalRequest {
        principalResourceId = "user-1"
        user = oAuthUser {
          issuer = "example-issuer"
          subject = "user@example.com"
        }
      }
      service.createUserPrincipal(request)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createUserPrincipal(
            request.copy { user = user.copy { subject = "user2@example.com" } }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.PRINCIPAL_ALREADY_EXISTS.name
          }
        )
    }

  @Test
  fun `lookupPrincipal returns TLS client principal`() {
    val service = initService()
    val request = lookupPrincipalRequest {
      tlsClient = tlsClient { authorityKeyIdentifier = TestConfig.AUTHORITY_KEY_IDENTIFIER }
    }

    val response: Principal = runBlocking { service.lookupPrincipal(request) }

    assertThat(response)
      .isEqualTo(
        principal {
          principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
          tlsClient = request.tlsClient
        }
      )
  }

  @Test
  fun `lookupPrincipal returns user principal`() = runBlocking {
    val service = initService()
    val principal =
      service.createUserPrincipal(
        createUserPrincipalRequest {
          principalResourceId = "user-1"
          user = oAuthUser {
            issuer = "example-issuer"
            subject = "user@example.com"
          }
        }
      )
    val request = lookupPrincipalRequest { user = principal.user }

    val response: Principal = runBlocking { service.lookupPrincipal(request) }

    assertThat(response).isEqualTo(principal)
  }

  @Test
  fun `deletePrincipal deletes user Principal`() = runBlocking {
    val service = initService()
    val principal =
      service.createUserPrincipal(
        createUserPrincipalRequest {
          principalResourceId = "user-1"
          user = oAuthUser {
            issuer = "example-issuer"
            subject = "user@example.com"
          }
        }
      )

    service.deletePrincipal(
      deletePrincipalRequest { principalResourceId = principal.principalResourceId }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPrincipal(
          getPrincipalRequest { principalResourceId = principal.principalResourceId }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deletePrincipal throws FAILED_PRECONDITION for TLS client Principal`() = runBlocking {
    val service = initService()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deletePrincipal(
          deletePrincipalRequest {
            principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
          metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = "TLS_CLIENT"
          metadata[Errors.Metadata.PRINCIPAL_RESOURCE_ID.key] =
            TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
        }
      )
  }
}
