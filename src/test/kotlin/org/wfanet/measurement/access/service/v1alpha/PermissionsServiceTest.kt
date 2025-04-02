// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.access.service.v1alpha

import com.google.common.truth.Truth.assertThat
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
import org.wfanet.measurement.access.service.internal.PermissionNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.GetPermissionRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.checkPermissionsRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.getPermissionRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsResponse
import org.wfanet.measurement.access.v1alpha.permission
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.ListPermissionsPageTokenKt as InternalListPermissionsPageTokenKt
import org.wfanet.measurement.internal.access.PermissionsGrpcKt as InternalPermissionsGrpcKt
import org.wfanet.measurement.internal.access.checkPermissionsRequest as internalCheckPermissionsRequest
import org.wfanet.measurement.internal.access.checkPermissionsResponse as internalCheckPermissionsResponse
import org.wfanet.measurement.internal.access.getPermissionRequest as internalGetPermissionRequest
import org.wfanet.measurement.internal.access.listPermissionsPageToken as internalListPermissionsPageToken
import org.wfanet.measurement.internal.access.listPermissionsRequest as internalListPermissionsRequest
import org.wfanet.measurement.internal.access.listPermissionsResponse as internalListPermissionsResponse
import org.wfanet.measurement.internal.access.permission as internalPermission

@RunWith(JUnit4::class)
class PermissionsServiceTest {
  private val internalServiceMock =
    mockService<InternalPermissionsGrpcKt.PermissionsCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: PermissionsService

  @Before
  fun initService() {
    service =
      PermissionsService(InternalPermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `getPermission returns Permission`() = runBlocking {
    val internalPermission = internalPermission {
      permissionResourceId = "books.get"
      resourceTypes += "library.googleapis.com/Shelf"
    }
    internalServiceMock.stub { onBlocking { getPermission(any()) } doReturn internalPermission }

    val request = getPermissionRequest {
      name = "permissions/${internalPermission.permissionResourceId}"
    }
    val response: Permission = service.getPermission(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPermissionsGrpcKt.PermissionsCoroutineImplBase::getPermission,
      )
      .isEqualTo(
        internalGetPermissionRequest {
          permissionResourceId = internalPermission.permissionResourceId
        }
      )
    assertThat(response)
      .isEqualTo(
        permission {
          name = request.name
          resourceTypes += internalPermission.resourceTypesList
        }
      )
  }

  @Test
  fun `getPermission throws PERMISSION_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getPermission(any()) } doThrow
        PermissionNotFoundException("books.get").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = getPermissionRequest { name = "permissions/books.get" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getPermission(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PERMISSION_NOT_FOUND.name
          metadata[Errors.Metadata.PERMISSION.key] = request.name
        }
      )
  }

  @Test
  fun `getPermission throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPermission(GetPermissionRequest.getDefaultInstance())
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
  fun `getPermission throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPermission(getPermissionRequest { name = "books.get" })
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
  fun `listPermissions returns Permissions`() = runBlocking {
    val internalBooksGetPermission = internalPermission {
      permissionResourceId = "books.get"
      resourceTypes += "library.googleapis.com/Shelf"
    }
    val internalListPermissionsResponse = internalListPermissionsResponse {
      permissions += internalBooksGetPermission
      nextPageToken = internalListPermissionsPageToken {
        after = InternalListPermissionsPageTokenKt.after { permissionResourceId = "books.write" }
      }
    }
    internalServiceMock.stub {
      onBlocking { listPermissions(any()) } doReturn internalListPermissionsResponse
    }

    val response: ListPermissionsResponse =
      service.listPermissions(listPermissionsRequest { pageSize = 1 })

    verifyProtoArgument(
        internalServiceMock,
        InternalPermissionsGrpcKt.PermissionsCoroutineImplBase::listPermissions,
      )
      .isEqualTo(internalListPermissionsRequest { pageSize = 1 })
    assertThat(response)
      .isEqualTo(
        listPermissionsResponse {
          permissions += internalBooksGetPermission.toPermission()
          nextPageToken =
            internalListPermissionsResponse.nextPageToken.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listPermissions throws INVALID_FIELD_VALUE when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listPermissions(listPermissionsRequest { pageSize = -1 })
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
  fun `listPermissions throws INVALID_FIELD_VALUE when page token is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listPermissions(listPermissionsRequest { pageToken = "1" })
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
  fun `checkPermissions returns Permissions`() = runBlocking {
    val internalCheckPermissionsResponse = internalCheckPermissionsResponse {
      permissionResourceIds += "books.get"
    }
    internalServiceMock.stub {
      onBlocking { checkPermissions(any()) } doReturn internalCheckPermissionsResponse
    }

    val response: CheckPermissionsResponse =
      service.checkPermissions(
        checkPermissionsRequest {
          principal = "principals/user-1"
          permissions += "permissions/books.get"
          permissions += "permissions/books.write"
        }
      )

    verifyProtoArgument(
        internalServiceMock,
        InternalPermissionsGrpcKt.PermissionsCoroutineImplBase::checkPermissions,
      )
      .isEqualTo(
        internalCheckPermissionsRequest {
          principalResourceId = "user-1"
          permissionResourceIds += "books.get"
          permissionResourceIds += "books.write"
        }
      )
    assertThat(response)
      .isEqualTo(checkPermissionsResponse { permissions += "permissions/books.get" })
  }

  @Test
  fun `checkPermissions throws REQUIRED_FIELD_NOT_SET when principal is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.checkPermissions(CheckPermissionsRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "principal"
        }
      )
  }

  @Test
  fun `checkPermissions throws INVALID_FIELD_VALUE when principal is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.checkPermissions(checkPermissionsRequest { principal = "user-1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "principal"
        }
      )
  }

  @Test
  fun `checkPermissions throws REQUIRED_FIELD_NOT_SET when permissions is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.checkPermissions(checkPermissionsRequest { principal = "principals/user-1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "permissions"
        }
      )
  }

  @Test
  fun `checkPermissions throws REQUIRED_FIELD_NOT_SET when permissions is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.checkPermissions(
            checkPermissionsRequest {
              principal = "principals/user-1"
              permissions += "books.get"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "permissions"
          }
        )
    }

  @Test
  fun `checkPermissions throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { checkPermissions(any()) } doThrow
        PrincipalNotFoundException("user-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = checkPermissionsRequest {
      principal = "principals/user-1"
      permissions += "permissions/books.get"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.checkPermissions(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND.name
          metadata[Errors.Metadata.PRINCIPAL.key] = request.principal
        }
      )
  }

  @Test
  fun `checkPermissions throws PERMISSION_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { checkPermissions(any()) } doThrow
        PermissionNotFoundException("books.get").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = checkPermissionsRequest {
      principal = "principals/user-1"
      permissions += "permissions/books.get"
      permissions += "permissions/books.write"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.checkPermissions(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PERMISSION_NOT_FOUND.name
          metadata[Errors.Metadata.PERMISSION.key] = request.permissionsList[0]
        }
      )
  }
}
