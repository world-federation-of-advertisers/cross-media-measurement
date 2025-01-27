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
import com.google.protobuf.Empty
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
import org.wfanet.measurement.access.service.internal.PrincipalAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundForTlsClientException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundForUserException
import org.wfanet.measurement.access.service.internal.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.v1alpha.DeletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.PrincipalKt as InternalPrincipalKt
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt as InternalPrincipalsGrpcKt
import org.wfanet.measurement.internal.access.createUserPrincipalRequest as internalCreateUserPrincipalRequest
import org.wfanet.measurement.internal.access.deletePrincipalRequest as internalDeletePrincipalRequest
import org.wfanet.measurement.internal.access.getPrincipalRequest as internalGetPrincipalRequest
import org.wfanet.measurement.internal.access.lookupPrincipalRequest as internalLookupPrincipalRequest
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

  @Test
  fun `createPrincipal returns user Principal`() = runBlocking {
    val internalPrincipal = internalPrincipal {
      principalResourceId = "user-1"
      user =
        InternalPrincipalKt.oAuthUser {
          issuer = "example.com"
          subject = "user1@example.com"
        }
    }
    internalServiceMock.stub {
      onBlocking { createUserPrincipal(any()) } doReturn internalPrincipal
    }

    val request = createPrincipalRequest {
      principal = principal {
        name = "principals/${internalPrincipal.principalResourceId}"
        user =
          PrincipalKt.oAuthUser {
            issuer = "example.com"
            subject = "user1@example.com"
          }
      }
      principalId = "user-1"
    }
    val response = service.createPrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::createUserPrincipal,
      )
      .isEqualTo(
        internalCreateUserPrincipalRequest {
          principalResourceId = internalPrincipal.principalResourceId
          user = internalPrincipal.user
        }
      )

    assertThat(response)
      .isEqualTo(
        principal {
          name = request.principal.name
          user =
            PrincipalKt.oAuthUser {
              issuer = internalPrincipal.user.issuer
              subject = internalPrincipal.user.subject
            }
        }
      )
  }

  @Test
  fun `createPrincipal throws PRINCIPAL_TYPE_NOT_SUPPORTED when principle identity case is TLS_CLIENT`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPrincipal(
            createPrincipalRequest {
              principal = principal {
                name = "principals/user-1"
                tlsClient =
                  PrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
            metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = "TLS_CLIENT"
          }
        )
    }

  @Test
  fun `createPrincipal throws INVALID_FIELD_VALUE when principle user is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPrincipal(
          createPrincipalRequest {
            principal = principal { name = "principals/user-1" }
            principalId = "user-1"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "principal.identity"
        }
      )
  }

  @Test
  fun `createPrincipal throws REQUIRED_FIELD_NOT_SET when principal user issuer is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPrincipal(
            createPrincipalRequest {
              principal = principal {
                name = "principals/user-1"
                user = PrincipalKt.oAuthUser { subject = "user1@example.com" }
              }
              principalId = "user-1"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "principal.user.issuer"
          }
        )
    }

  @Test
  fun `createPrincipal throws REQUIRED_FIELD_NOT_SET when principal user subject is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPrincipal(
            createPrincipalRequest {
              principal = principal {
                name = "principals/user-1"
                user = PrincipalKt.oAuthUser { issuer = "example.com" }
              }
              principalId = "user-1"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "principal.user.subject"
          }
        )
    }

  @Test
  fun `createPrincipal throws REQUIRED_FIELD_NOT_SET when principle id is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPrincipal(
          createPrincipalRequest {
            principal = principal {
              name = "principals/user-1"
              user =
                PrincipalKt.oAuthUser {
                  issuer = "example.com"
                  subject = "user1@example.com"
                }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "principal_id"
        }
      )
  }

  @Test
  fun `createPrincipal throws INVALID_FIELD_VALUE when principle id does not match RFC_1034_REGEX`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPrincipal(
            createPrincipalRequest {
              principal = principal {
                name = "principals/user-1"
                user =
                  PrincipalKt.oAuthUser {
                    issuer = "example.com"
                    subject = "user1@example.com"
                  }
              }
              principalId = "123"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "principal_id"
          }
        )
    }

  @Test
  fun `createPrincipal throws PRINCIPAL_ALREADY_EXISTS from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createUserPrincipal(any()) } doThrow
        PrincipalAlreadyExistsException().asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = createPrincipalRequest {
      principal = principal {
        name = "principals/user-1"
        user =
          PrincipalKt.oAuthUser {
            issuer = "example.com"
            subject = "user1@example.com"
          }
      }
      principalId = "user-1"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createPrincipal(request) }

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
  fun `deletePrincipal returns empty`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { deletePrincipal(any()) } doReturn Empty.getDefaultInstance()
    }

    val request = deletePrincipalRequest { name = "principals/user-1" }
    val response = service.deletePrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::deletePrincipal,
      )
      .isEqualTo(internalDeletePrincipalRequest { principalResourceId = "user-1" })

    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `deletePrincipal throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { deletePrincipal(any()) } doThrow
        PrincipalNotFoundException("user-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = deletePrincipalRequest { name = "principals/user-1" }
    val exception = assertFailsWith<StatusRuntimeException> { service.deletePrincipal(request) }

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
  fun `deletePrincipal throws PRINCIPAL_TYPE_NOT_SUPPORTED from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { deletePrincipal(any()) } doThrow
        PrincipalTypeNotSupportedException("user-1", Principal.IdentityCase.TLS_CLIENT)
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val request = deletePrincipalRequest { name = "principals/user-1" }
    val exception = assertFailsWith<StatusRuntimeException> { service.deletePrincipal(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
          metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = Principal.IdentityCase.TLS_CLIENT.name
        }
      )
  }

  @Test
  fun `deletePrincipal throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deletePrincipal(DeletePrincipalRequest.getDefaultInstance())
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
  fun `deletePrincipal throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deletePrincipal(deletePrincipalRequest { name = "user-1" })
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

  @Test
  fun `lookupPrincipal returns user Principal`() = runBlocking {
    val internalPrincipal = internalPrincipal {
      principalResourceId = "user-1"
      user =
        InternalPrincipalKt.oAuthUser {
          issuer = "example.com"
          subject = "user1@example.com"
        }
    }
    internalServiceMock.stub { onBlocking { lookupPrincipal(any()) } doReturn internalPrincipal }

    val request = lookupPrincipalRequest {
      user =
        PrincipalKt.oAuthUser {
          issuer = "example.com"
          subject = "user1@example.com"
        }
    }
    val response = service.lookupPrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::lookupPrincipal,
      )
      .isEqualTo(internalLookupPrincipalRequest { user = internalPrincipal.user })

    assertThat(response)
      .isEqualTo(
        principal {
          name = "principals/${internalPrincipal.principalResourceId}"
          user =
            PrincipalKt.oAuthUser {
              issuer = internalPrincipal.user.issuer
              subject = internalPrincipal.user.subject
            }
        }
      )
  }

  @Test
  fun `lookupPrincipal returns tls client Principal`() = runBlocking {
    val internalPrincipal = internalPrincipal {
      principalResourceId = "user-1"
      tlsClient =
        InternalPrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
    }
    internalServiceMock.stub { onBlocking { lookupPrincipal(any()) } doReturn internalPrincipal }

    val request = lookupPrincipalRequest {
      tlsClient = PrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
    }
    val response = service.lookupPrincipal(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPrincipalsGrpcKt.PrincipalsCoroutineImplBase::lookupPrincipal,
      )
      .isEqualTo(internalLookupPrincipalRequest { tlsClient = internalPrincipal.tlsClient })

    assertThat(response)
      .isEqualTo(
        principal {
          name = "principals/${internalPrincipal.principalResourceId}"
          tlsClient = PrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
        }
      )
  }

  @Test
  fun `lookupPrincipal throws REQUIRED_FIELD_NOT_SET when principle user is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPrincipal(lookupPrincipalRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "lookup_key"
          }
        )
    }

  @Test
  fun `lookupPrincipal throws REQUIRED_FIELD_NOT_SET when principal user issuer is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPrincipal(
            lookupPrincipalRequest {
              user = PrincipalKt.oAuthUser { subject = "user1@example.com" }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "user.issuer"
          }
        )
    }

  @Test
  fun `lookupPrincipal throws REQUIRED_FIELD_NOT_SET when principal user subject is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPrincipal(
            lookupPrincipalRequest { user = PrincipalKt.oAuthUser { issuer = "example.com" } }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "user.subject"
          }
        )
    }

  @Test
  fun `lookupPrincipal throws REQUIRED_FIELD_NOT_SET when principal tls client is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPrincipal(lookupPrincipalRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "lookup_key"
          }
        )
    }

  @Test
  fun `lookupPrincipal throws REQUIRED_FIELD_NOT_SET when principal tls client authority key identifier is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPrincipal(lookupPrincipalRequest { tlsClient = PrincipalKt.tlsClient {} })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "tlsclient.authoritykeyidentifier"
          }
        )
    }

  @Test
  fun `lookupPrincipal throws PRINCIPAL_NOT_FOUND_FOR_USER from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { lookupPrincipal(any()) } doThrow
        PrincipalNotFoundForUserException(subject = "user1@example.com", issuer = "exmaple.com")
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = lookupPrincipalRequest {
      user =
        PrincipalKt.oAuthUser {
          issuer = "example.com"
          subject = "user1@example.com"
        }
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.lookupPrincipal(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER.name
          metadata[Errors.Metadata.ISSUER.key] = request.user.issuer
          metadata[Errors.Metadata.SUBJECT.key] = request.user.subject
        }
      )
  }

  @Test
  fun `lookupPrincipal throws PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { lookupPrincipal(any()) } doThrow
        PrincipalNotFoundForTlsClientException(authorityKeyIdentifier = "akid".toByteStringUtf8())
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = lookupPrincipalRequest {
      tlsClient = PrincipalKt.tlsClient { authorityKeyIdentifier = "akid".toByteStringUtf8() }
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.lookupPrincipal(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT.name
          metadata[Errors.Metadata.AUTHORITY_KEY_IDENTIFIER.key] = "61:6B:69:64"
        }
      )
  }
}
