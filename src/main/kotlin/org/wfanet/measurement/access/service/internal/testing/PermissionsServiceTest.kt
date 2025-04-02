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
import com.google.protobuf.Descriptors
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.Errors
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.toPermission
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.access.CheckPermissionsResponse
import org.wfanet.measurement.internal.access.ListPermissionsPageTokenKt
import org.wfanet.measurement.internal.access.ListPermissionsRequest
import org.wfanet.measurement.internal.access.ListPermissionsResponse
import org.wfanet.measurement.internal.access.Permission
import org.wfanet.measurement.internal.access.PermissionsGrpcKt
import org.wfanet.measurement.internal.access.PoliciesGrpcKt
import org.wfanet.measurement.internal.access.PolicyKt
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.RolesGrpcKt
import org.wfanet.measurement.internal.access.checkPermissionsRequest
import org.wfanet.measurement.internal.access.checkPermissionsResponse
import org.wfanet.measurement.internal.access.createUserPrincipalRequest
import org.wfanet.measurement.internal.access.getPermissionRequest
import org.wfanet.measurement.internal.access.listPermissionsPageToken
import org.wfanet.measurement.internal.access.listPermissionsRequest
import org.wfanet.measurement.internal.access.listPermissionsResponse
import org.wfanet.measurement.internal.access.policy
import org.wfanet.measurement.internal.access.role

@RunWith(JUnit4::class)
abstract class PermissionsServiceTest {
  protected data class Services(
    /** Service under test. */
    val service: PermissionsGrpcKt.PermissionsCoroutineImplBase,
    val principalsService: PrincipalsGrpcKt.PrincipalsCoroutineImplBase,
    val rolesServices: RolesGrpcKt.RolesCoroutineImplBase,
    val policiesService: PoliciesGrpcKt.PoliciesCoroutineImplBase,
  )

  protected abstract fun initServices(
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
    idGenerator: IdGenerator,
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default) =
    initServices(TestConfig.PERMISSION_MAPPING, TestConfig.TLS_CLIENT_MAPPING, idGenerator)

  @Test
  fun `getPermission returns Permission`() = runBlocking {
    val service = initServices().service

    val response: Permission =
      service.getPermission(
        getPermissionRequest { permissionResourceId = PERMISSIONS.first().permissionResourceId }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(RESOURCE_TYPES_FIELD)
      .isEqualTo(PERMISSIONS.first())
  }

  @Test
  fun `getPermission throws NOT_FOUND when Permission not found`() = runBlocking {
    val service = initServices().service

    val request = getPermissionRequest { permissionResourceId = "not-found" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getPermission(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PERMISSION_NOT_FOUND.name
          metadata[Errors.Metadata.PERMISSION_RESOURCE_ID.key] = request.permissionResourceId
        }
      )
  }

  @Test
  fun `listPermissions returns Permissions ordered by resource ID`() {
    runBlocking {
      val service = initServices().service

      val response: ListPermissionsResponse =
        service.listPermissions(ListPermissionsRequest.getDefaultInstance())

      assertThat(response)
        .ignoringRepeatedFieldOrderOfFieldDescriptors(RESOURCE_TYPES_FIELD)
        .isEqualTo(listPermissionsResponse { permissions += PERMISSIONS })
    }
  }

  @Test
  fun `listPermissions returns Permissions when page size is specified`() = runBlocking {
    val service = initServices().service

    val response: ListPermissionsResponse =
      service.listPermissions(listPermissionsRequest { pageSize = PERMISSIONS.size })

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(RESOURCE_TYPES_FIELD)
      .isEqualTo(listPermissionsResponse { permissions += PERMISSIONS })
  }

  @Test
  fun `listPermissions returns next page token when there are more results`() = runBlocking {
    val service = initServices().service

    val response: ListPermissionsResponse =
      service.listPermissions(listPermissionsRequest { pageSize = 2 })

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(RESOURCE_TYPES_FIELD)
      .isEqualTo(
        listPermissionsResponse {
          permissions += PERMISSIONS.take(2)
          nextPageToken = listPermissionsPageToken {
            after =
              ListPermissionsPageTokenKt.after {
                permissionResourceId =
                  this@listPermissionsResponse.permissions.last().permissionResourceId
              }
          }
        }
      )
  }

  @Test
  fun `listPermissions returns Permissions after page token`() = runBlocking {
    val service = initServices().service

    val response: ListPermissionsResponse =
      service.listPermissions(
        listPermissionsRequest {
          pageToken = listPermissionsPageToken {
            after =
              ListPermissionsPageTokenKt.after {
                permissionResourceId = PERMISSIONS[1].permissionResourceId
              }
          }
        }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(RESOURCE_TYPES_FIELD)
      .isEqualTo(listPermissionsResponse { permissions += PERMISSIONS.drop(2) })
  }

  @Test
  fun `checkPermissions returns all requested permissions for TLS client principal`() {
    val service = initServices().service
    val request = checkPermissionsRequest {
      protectedResourceName = TestConfig.TLS_CLIENT_PROTECTED_RESOURCE_NAME
      principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
    }

    val response: CheckPermissionsResponse = runBlocking { service.checkPermissions(request) }

    assertThat(response)
      .isEqualTo(
        checkPermissionsResponse { permissionResourceIds += request.permissionResourceIdsList }
      )
  }

  @Test
  fun `checkPermissions returns no permissions for TLS client principal with wrong protected resource`() {
    val service = initServices().service
    val request = checkPermissionsRequest {
      protectedResourceName = "shelves/404"
      principalResourceId = TestConfig.TLS_CLIENT_PRINCIPAL_RESOURCE_ID
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
    }

    val response: CheckPermissionsResponse = runBlocking { service.checkPermissions(request) }

    assertThat(response).isEqualTo(CheckPermissionsResponse.getDefaultInstance())
  }

  @Test
  fun `checkPermissions returns permissions for user Principal`(): Unit = runBlocking {
    val (service, principalsService, rolesService, policiesService) = initServices()
    val principal =
      principalsService.createUserPrincipal(
        createUserPrincipalRequest {
          principalResourceId = "user-1"
          user = oAuthUser {
            issuer = "example-issuer"
            subject = "user@example.com"
          }
        }
      )
    val role =
      rolesService.createRole(
        role {
          roleResourceId = "shelfBookReader"
          resourceTypes += TestConfig.ResourceType.SHELF
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
        }
      )
    val policy =
      policiesService.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
          bindings[role.roleResourceId] =
            PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
        }
      )

    val response =
      service.checkPermissions(
        checkPermissionsRequest {
          protectedResourceName = policy.protectedResourceName
          principalResourceId = principal.principalResourceId
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
          permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_CREATE
        }
      )

    assertThat(response.permissionResourceIdsList)
      .containsExactly(TestConfig.PermissionResourceId.BOOKS_LIST)
  }

  companion object {
    private val RESOURCE_TYPES_FIELD: Descriptors.FieldDescriptor =
      Permission.getDescriptor().findFieldByNumber(Permission.RESOURCE_TYPES_FIELD_NUMBER)

    private val PERMISSIONS: List<Permission> =
      TestConfig.PERMISSION_MAPPING.permissions.map { it.toPermission() }
  }
}
