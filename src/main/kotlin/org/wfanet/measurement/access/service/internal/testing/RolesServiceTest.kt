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
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.access.ListRolesPageTokenKt
import org.wfanet.measurement.internal.access.ListRolesRequest
import org.wfanet.measurement.internal.access.ListRolesResponse
import org.wfanet.measurement.internal.access.PoliciesGrpcKt
import org.wfanet.measurement.internal.access.PolicyKt
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.Role
import org.wfanet.measurement.internal.access.RolesGrpcKt
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.createUserPrincipalRequest
import org.wfanet.measurement.internal.access.deleteRoleRequest
import org.wfanet.measurement.internal.access.getRoleRequest
import org.wfanet.measurement.internal.access.listRolesPageToken
import org.wfanet.measurement.internal.access.listRolesRequest
import org.wfanet.measurement.internal.access.listRolesResponse
import org.wfanet.measurement.internal.access.policy
import org.wfanet.measurement.internal.access.role

@RunWith(JUnit4::class)
abstract class RolesServiceTest {
  /** Initializes the service under test. */
  abstract fun initService(
    permissionMapping: PermissionMapping,
    idGenerator: IdGenerator,
  ): RolesGrpcKt.RolesCoroutineImplBase

  private fun initService(idGenerator: IdGenerator = IdGenerator.Default) =
    initService(TestConfig.PERMISSION_MAPPING, idGenerator)

  protected data class Services(
    /** Service under test. */
    val service: RolesGrpcKt.RolesCoroutineImplBase,
    val principalsService: PrincipalsGrpcKt.PrincipalsCoroutineImplBase,
    val policiesServices: PoliciesGrpcKt.PoliciesCoroutineImplBase,
  )

  protected abstract fun initServices(
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
    idGenerator: IdGenerator,
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default) =
    initServices(TestConfig.PERMISSION_MAPPING, TestConfig.TLS_CLIENT_MAPPING, idGenerator)

  @Test
  fun `getRole throws NOT_FOUND when Role not found`() = runBlocking {
    val service = initService()
    val request = getRoleRequest { roleResourceId = "not-found" }

    val exception = assertFailsWith<StatusRuntimeException> { service.getRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE_RESOURCE_ID.key] = request.roleResourceId
        }
      )
  }

  @Test
  fun `createRole returns created Role`() = runBlocking {
    val service = initService()
    val request = role {
      roleResourceId = "shelfBookReader"
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
      resourceTypes += TestConfig.ResourceType.SHELF
    }

    val response: Role = service.createRole(request)

    assertThat(response)
      .ignoringFields(
        Role.CREATE_TIME_FIELD_NUMBER,
        Role.UPDATE_TIME_FIELD_NUMBER,
        Role.ETAG_FIELD_NUMBER,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(request)
    assertThat(response.createTime.toInstant()).isGreaterThan(Instant.now().minusSeconds(10))
    assertThat(response.updateTime).isEqualTo(response.createTime)
    assertThat(response.etag).isNotEmpty()
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(service.getRole(getRoleRequest { roleResourceId = request.roleResourceId }))
  }

  @Test
  fun `createRole throws ALREADY_EXISTS if Role with resource ID already exists`() = runBlocking {
    val service = initService()
    val request = role {
      roleResourceId = "bookReader"
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
      resourceTypes += TestConfig.ResourceType.BOOK
    }
    service.createRole(request)

    val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_ALREADY_EXISTS.name
        }
      )
  }

  @Test
  fun `createRole retries ID generation if ID already in use`(): Unit = runBlocking {
    val principalId1 = 1234L
    val principalId2 = 2345L
    val idGeneratorMock =
      mock<IdGenerator> { on { generateId() }.thenReturn(principalId1, principalId1, principalId2) }
    val service = initService(idGeneratorMock)
    service.createRole(
      role {
        roleResourceId = "shelfBookReader"
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
        resourceTypes += TestConfig.ResourceType.SHELF
      }
    )

    service.createRole(
      role {
        roleResourceId = "bookReader"
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        resourceTypes += TestConfig.ResourceType.BOOK
      }
    )

    verify(idGeneratorMock, times(3)).generateId()
  }

  @Test
  fun `createRole throws FAILED_PRECONDITION if resource type not found in Permission`() =
    runBlocking {
      val service = initService()
      val request = role {
        roleResourceId = "bookWriter"
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        resourceTypes += TestConfig.ResourceType.BOOK
        resourceTypes += TestConfig.ResourceType.SHELF
      }

      val exception = assertFailsWith<StatusRuntimeException> { service.createRole(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION.name
            metadata[Errors.Metadata.RESOURCE_TYPE.key] = TestConfig.ResourceType.BOOK
            metadata[Errors.Metadata.PERMISSION_RESOURCE_ID.key] =
              TestConfig.PermissionResourceId.BOOKS_CREATE
          }
        )
    }

  @Test
  fun `updateRole returns updated Role`() = runBlocking {
    val service = initService()
    val role =
      service.createRole(
        role {
          roleResourceId = "bookUser"
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_DELETE
        }
      )

    val request =
      role.copy {
        resourceTypes.clear()
        resourceTypes += TestConfig.ResourceType.SHELF

        permissionResourceIds.clear()
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
        permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
      }
    val response: Role = service.updateRole(request)

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(Role.UPDATE_TIME_FIELD_NUMBER, Role.ETAG_FIELD_NUMBER)
      .isEqualTo(request)
    assertThat(response.updateTime.toInstant()).isGreaterThan(role.updateTime.toInstant())
    assertThat(response.etag).isNotEqualTo(role.etag)
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(service.getRole(getRoleRequest { roleResourceId = role.roleResourceId }))
  }

  @Test
  fun `updateRole throws INVALID_ARGUMENT if etag not set`() = runBlocking {
    val service = initService()
    val role =
      service.createRole(
        role {
          roleResourceId = "bookUser"
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_DELETE
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> { service.updateRole(role.copy { clearEtag() }) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "etag"
        }
      )
  }

  @Test
  fun `updateRole throws ABORTED if etag does not match`() = runBlocking {
    val service = initService()
    val role =
      service.createRole(
        role {
          roleResourceId = "bookUser"
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        }
      )

    val request = role.copy { etag = "W/\"foo\"" }
    val exception = assertFailsWith<StatusRuntimeException> { service.updateRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.ETAG.key] = role.etag
          metadata[Errors.Metadata.REQUEST_ETAG.key] = request.etag
        }
      )
  }

  @Test
  fun `updateRole throws FAILED_PRECONDITION if resource type not found in new Permission`() =
    runBlocking {
      val service = initService()
      val role =
        service.createRole(
          role {
            roleResourceId = "bookUser"
            resourceTypes += TestConfig.ResourceType.BOOK
            permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
            permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_DELETE
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.updateRole(
            role.copy { permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION.name
            metadata[Errors.Metadata.RESOURCE_TYPE.key] = TestConfig.ResourceType.BOOK
            metadata[Errors.Metadata.PERMISSION_RESOURCE_ID.key] =
              TestConfig.PermissionResourceId.BOOKS_CREATE
          }
        )
    }

  @Test
  fun `updateRole throws FAILED_PRECONDITION if resource type not found in existing Permission`() =
    runBlocking {
      val service = initService()
      val role =
        service.createRole(
          role {
            roleResourceId = "bookAdmin"
            resourceTypes += TestConfig.ResourceType.SHELF
            permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
            permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
            permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_DELETE
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.updateRole(role.copy { resourceTypes += TestConfig.ResourceType.BOOK })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION.name
            metadata[Errors.Metadata.RESOURCE_TYPE.key] = TestConfig.ResourceType.BOOK
            metadata[Errors.Metadata.PERMISSION_RESOURCE_ID.key] =
              TestConfig.PermissionResourceId.BOOKS_CREATE
          }
        )
    }

  @Test
  fun `deleteRole deletes Role`() = runBlocking {
    val service = initService()
    val role =
      service.createRole(
        role {
          roleResourceId = "bookReader"
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        }
      )

    service.deleteRole(deleteRoleRequest { roleResourceId = role.roleResourceId })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRole(getRoleRequest { roleResourceId = role.roleResourceId })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteRole deletes Role when policy exists`() = runBlocking {
    val (service, principalsService, policiesService) = initServices()

    val createPrincipalRequest = createUserPrincipalRequest {
      principalResourceId = "user-1"
      user = oAuthUser {
        issuer = "example-issuer"
        subject = "user@example.com"
      }
    }
    val principal = principalsService.createUserPrincipal(createPrincipalRequest)

    val role =
      service.createRole(
        role {
          roleResourceId = "bookReader"
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        }
      )

    val createPolicyRequest = policy {
      policyResourceId = "fantasy-shelf-policy"
      protectedResourceName = "shelves/fantasy"
      bindings[role.roleResourceId] =
        PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
    }
    policiesService.createPolicy(createPolicyRequest)

    service.deleteRole(deleteRoleRequest { roleResourceId = role.roleResourceId })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRole(getRoleRequest { roleResourceId = role.roleResourceId })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `deleteRole throws NOT_FOUND when role not found`() = runBlocking {
    val service = initService()
    val request = deleteRoleRequest { roleResourceId = "not-found" }

    val exception = assertFailsWith<StatusRuntimeException> { service.deleteRole(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE_RESOURCE_ID.key] = request.roleResourceId
        }
      )
  }

  @Test
  fun `listRoles returns roles ordered by resource ID`() = runBlocking {
    val service = initService()
    val roles: List<Role> = createRoles(service, 10)

    val response: ListRolesResponse = service.listRoles(ListRolesRequest.getDefaultInstance())

    assertThat(response).isEqualTo(listRolesResponse { this.roles += roles })
  }

  @Test
  fun `listRoles returns roles when page size is specified`() = runBlocking {
    val service = initService()
    val roles: List<Role> = createRoles(service, 10)

    val response: ListRolesResponse = service.listRoles(listRolesRequest { pageSize = 10 })

    assertThat(response).isEqualTo(listRolesResponse { this.roles += roles })
  }

  @Test
  fun `listRoles returns next page token when there are more results`() = runBlocking {
    val service = initService()
    val roles: List<Role> = createRoles(service, 10)

    val request = listRolesRequest { pageSize = 5 }
    val response: ListRolesResponse = service.listRoles(request)

    assertThat(response)
      .isEqualTo(
        listRolesResponse {
          this.roles += roles.take(request.pageSize)
          nextPageToken = listRolesPageToken {
            after = ListRolesPageTokenKt.after { roleResourceId = "role-0000000005" }
          }
        }
      )
  }

  @Test
  fun `listRoles returns results after page token`() = runBlocking {
    val service = initService()
    val roles: List<Role> = createRoles(service, 10)

    val request = listRolesRequest {
      pageSize = 2
      pageToken = listRolesPageToken {
        after = ListRolesPageTokenKt.after { roleResourceId = "role-0000000005" }
      }
    }
    val response: ListRolesResponse = service.listRoles(request)

    assertThat(response)
      .isEqualTo(
        listRolesResponse {
          this.roles += roles.subList(5, 7)
          nextPageToken = listRolesPageToken {
            after = ListRolesPageTokenKt.after { roleResourceId = "role-0000000007" }
          }
        }
      )
  }

  private suspend fun createRoles(
    service: RolesGrpcKt.RolesCoroutineImplBase,
    count: Int,
  ): List<Role> {
    return (1..count).map {
      service.createRole(
        role {
          roleResourceId = String.format("role-%010d", it)
          resourceTypes += TestConfig.ResourceType.BOOK
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
        }
      )
    }
  }
}
