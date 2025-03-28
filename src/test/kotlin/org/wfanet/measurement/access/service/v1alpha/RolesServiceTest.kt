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
import org.wfanet.measurement.access.service.internal.PermissionNotFoundException
import org.wfanet.measurement.access.service.internal.ResourceTypeNotFoundInPermissionException
import org.wfanet.measurement.access.service.internal.RoleAlreadyExistsException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.access.v1alpha.DeleteRoleRequest
import org.wfanet.measurement.access.v1alpha.GetRoleRequest
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deleteRoleRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.listRolesRequest
import org.wfanet.measurement.access.v1alpha.listRolesResponse
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.access.v1alpha.updateRoleRequest
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.ListRolesPageTokenKt as InternalListRolesPageTokenKt
import org.wfanet.measurement.internal.access.RolesGrpcKt as InternalRolesGrpcKt
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.deleteRoleRequest as internalDeleteRoleRequest
import org.wfanet.measurement.internal.access.getRoleRequest as internalGetRoleRequest
import org.wfanet.measurement.internal.access.listRolesPageToken as internalListRolesPageToken
import org.wfanet.measurement.internal.access.listRolesRequest as internalListRolesRequest
import org.wfanet.measurement.internal.access.listRolesResponse as internalListRolesResponse
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
  fun `getRole returns Role`() = runBlocking {
    val internalRole = internalRole {
      roleResourceId = "bookReader"
      resourceTypes += "library.googleapis.com/Shelf"
      permissionResourceIds += "books.get"
      permissionResourceIds += "books.list"
      etag = "response-etag"
    }
    internalServiceMock.stub { onBlocking { getRole(any()) } doReturn internalRole }

    val request = getRoleRequest { name = "roles/${internalRole.roleResourceId}" }
    val response = service.getRole(request)

    verifyProtoArgument(internalServiceMock, InternalRolesGrpcKt.RolesCoroutineImplBase::getRole)
      .isEqualTo(internalGetRoleRequest { roleResourceId = internalRole.roleResourceId })
    assertThat(response)
      .isEqualTo(
        role {
          name = request.name
          resourceTypes += internalRole.resourceTypesList
          permissions += "permissions/books.get"
          permissions += "permissions/books.list"
          etag = internalRole.etag
        }
      )
  }

  @Test
  fun `getRole throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRole(GetRoleRequest.getDefaultInstance())
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
  fun `getRole throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRole(getRoleRequest { name = "roles" }) }

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
  fun `getRole throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getRole(any()) } doThrow
        RoleNotFoundException("bookReader").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = getRoleRequest { name = "roles/bookReader" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = request.name
        }
      )
  }

  @Test
  fun `listRoles returns Roles`() = runBlocking {
    val internalBookReaderRole = internalRole {
      roleResourceId = "bookReader"
      resourceTypes += "library.googleapis.com/Shelf"
      permissionResourceIds += "books.get"
      permissionResourceIds += "books.list"
      etag = "response-etag"
    }
    val internalListRolesResponse = internalListRolesResponse {
      roles += internalBookReaderRole
      nextPageToken = internalListRolesPageToken {
        after = InternalListRolesPageTokenKt.after { roleResourceId = "bookWriter" }
      }
    }
    internalServiceMock.stub { onBlocking { listRoles(any()) } doReturn internalListRolesResponse }

    val response = service.listRoles(listRolesRequest { pageSize = 1 })

    verifyProtoArgument(internalServiceMock, InternalRolesGrpcKt.RolesCoroutineImplBase::listRoles)
      .isEqualTo(internalListRolesRequest { pageSize = 1 })
    assertThat(response)
      .isEqualTo(
        listRolesResponse {
          roles += internalBookReaderRole.toRole()
          nextPageToken = internalListRolesResponse.nextPageToken.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listRoles throws INVALID_FIELD_VALUE when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRoles(listRolesRequest { pageSize = -1 })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
        }
      )
  }

  @Test
  fun `listRoles throws INVALID_FIELD_VALUE when page token is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRoles(listRolesRequest { pageToken = "1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
        }
      )
  }

  @Test
  fun `createRole returns Role`() = runBlocking {
    val internalRole = internalRole {
      roleResourceId = "bookReader"
      resourceTypes += "library.googleapis.com/Shelf"
      permissionResourceIds += "books.get"
      permissionResourceIds += "books.list"
      etag = "response-etag"
    }
    internalServiceMock.stub { onBlocking { createRole(any()) } doReturn internalRole }

    val request = createRoleRequest {
      role = role {
        name = "roles/${internalRole.roleResourceId}"
        resourceTypes += internalRole.resourceTypesList
        permissions += "permissions/books.get"
        permissions += "permissions/books.list"
        etag = "request-etag"
      }
      roleId = "bookReader"
    }
    val response = service.createRole(request)

    verifyProtoArgument(internalServiceMock, InternalRolesGrpcKt.RolesCoroutineImplBase::createRole)
      .isEqualTo(internalRole.copy { etag = request.role.etag })
    assertThat(response).isEqualTo(request.role.copy { etag = internalRole.etag })
  }

  @Test
  fun `createRole throws REQUIRED_FIELD_NOT_SET when role is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRole(createRoleRequest { roleId = "bookReader" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "role"
        }
      )
  }

  @Test
  fun `createRole throws REQUIRED_FIELD_NOT_SET when role id is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRole(
          createRoleRequest {
            role = role {
              name = "roles/bookReader"
              permissions += "permissions/books.get"
              permissions += "permissions/books.list"
              resourceTypes += "library.googleapis.com/Shelf"
              etag = "request-etag"
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "role_id"
        }
      )
  }

  @Test
  fun `createRole throws INVALID_FIELD_VALUE when role id is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRole(
          createRoleRequest {
            role = role {
              name = "roles/bookReader"
              permissions += "permissions/books.get"
              permissions += "permissions/books.list"
              resourceTypes += "library.googleapis.com/Shelf"
              etag = "request-etag"
            }
            roleId = "123"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "role_id"
        }
      )
  }

  @Test
  fun `createRole throws INVALID_FIELD_VALUE when Permission name is malformed`() = runBlocking {
    val request = createRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "perms/books.get"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
      roleId = "bookReader"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

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

  @Test
  fun `createRole throws PERMISSION_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createRole(any()) } doThrow
        PermissionNotFoundException("books.get").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = createRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "permissions/books.get"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
      roleId = "bookReader"
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PERMISSION_NOT_FOUND.name
          metadata[Errors.Metadata.PERMISSION.key] = "permissions/books.get"
        }
      )
  }

  @Test
  fun `createRole throws RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createRole(any()) } doThrow
        ResourceTypeNotFoundInPermissionException("library.googleapis.com/Shelf", "books.get")
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = createRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "permissions/books.get"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
      roleId = "bookReader"
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION.name
          metadata[Errors.Metadata.PERMISSION.key] = request.role.permissionsList.single()
          metadata[Errors.Metadata.RESOURCE_TYPE.key] = request.role.resourceTypesList.single()
        }
      )
  }

  @Test
  fun `createRole throws ROLE_ALREADY_EXISTS from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createRole(any()) } doThrow
        RoleAlreadyExistsException().asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = createRoleRequest {
      role = role {
        name = "roles/bookReader"
        permissions += "permissions/books.get"
        permissions += "permissions/books.list"
        resourceTypes += "library.googleapis.com/Shelf"
        etag = "request-etag"
      }
      roleId = "bookReader"
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_ALREADY_EXISTS.name
          metadata[Errors.Metadata.ROLE.key] = "roles/bookReader"
        }
      )
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

  @Test
  fun `deleteRole returns empty`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { deleteRole(any()) } doReturn Empty.getDefaultInstance()
    }

    val request = deleteRoleRequest { name = "roles/bookReader" }
    val response = service.deleteRole(request)

    verifyProtoArgument(internalServiceMock, InternalRolesGrpcKt.RolesCoroutineImplBase::deleteRole)
      .isEqualTo(internalDeleteRoleRequest { roleResourceId = "bookReader" })
    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `deleteRole throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deleteRole(DeleteRoleRequest.getDefaultInstance())
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
  fun `deleteRole throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deleteRole(deleteRoleRequest { name = "roles" })
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
  fun `deleteRow throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { deleteRole(any()) } doThrow
        RoleNotFoundException("bookReader").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = deleteRoleRequest { name = "roles/bookReader" }
    val exception = assertFailsWith<StatusRuntimeException> { service.deleteRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = request.name
        }
      )
  }
}
