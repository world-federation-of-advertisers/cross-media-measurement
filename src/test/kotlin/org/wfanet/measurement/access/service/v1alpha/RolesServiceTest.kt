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
import org.wfanet.measurement.access.service.internal.EtagMismatchException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.access.v1alpha.updateRoleRequest
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.RolesGrpcKt as InternalRolesGrpcKt
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.role as internalRole

@RunWith(JUnit4::class)
class RolesServiceTest {
  private val internalServiceMock = mockService<InternalRolesGrpcKt.RolesCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: RolesService

  @Before
  fun initService() {
    service = RolesService(InternalRolesGrpcKt.RolesCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `updateRole returns Role`() = runBlocking {
    val internalRole = internalRole {
      roleResourceId = "bookReader"
      permissionResourceIds += "books.get"
      permissionResourceIds += "books.list"
      resourceTypes += "library.googleapis.com/Shelf"
      etag = "response-etag"
    }
    internalServiceMock.stub { onBlocking { updateRole(any()) } doReturn internalRole }

    val request = updateRoleRequest {
      role = role {
        name = "roles/${internalRole.roleResourceId}"
        permissions += "permissions/books.get"
        permissions += "permissions/books.list"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
    }
    val response = service.updateRole(request)

    verifyProtoArgument(internalServiceMock, InternalRolesGrpcKt.RolesCoroutineImplBase::updateRole)
      .isEqualTo(internalRole.copy { etag = request.role.etag })
    assertThat(response).isEqualTo(request.role.copy { etag = internalRole.etag })
  }

  @Test
  fun `updateRole throws ETAG_MISMATCH from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { updateRole(any()) } doThrow
        EtagMismatchException("request-etag", "etag").asStatusRuntimeException(Status.Code.ABORTED)
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.updateRole(
          updateRoleRequest {
            role = role {
              name = "roles/bookReader"
              permissions += "permissions/books.get"
              resourceTypes += "library.googleapis.com/Shelf"
              etag = "request-etag"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.REQUEST_ETAG.key] = "request-etag"
          metadata[Errors.Metadata.ETAG.key] = "etag"
        }
      )
  }

  @Test
  fun `updateRole throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { updateRole(any()) } doThrow
        RoleNotFoundException("bookReader").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = updateRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "permissions/books.get"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.updateRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = request.role.name
        }
      )
  }

  @Test
  fun `updateRole throws INVALID_FIELD_VALUE when Permission name malformed`() = runBlocking {
    val request = updateRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "perms/books.get"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.updateRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "role.permissions"
        }
      )
  }
}
