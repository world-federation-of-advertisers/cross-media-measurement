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

package org.wfanet.measurement.access.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.abs
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.checkPermissions
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPrincipalIdByResourceId
import org.wfanet.measurement.access.service.internal.InvalidFieldValueException
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.PermissionNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.internal.toPermission
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.access.CheckPermissionsRequest
import org.wfanet.measurement.internal.access.CheckPermissionsResponse
import org.wfanet.measurement.internal.access.GetPermissionRequest
import org.wfanet.measurement.internal.access.ListPermissionsPageTokenKt
import org.wfanet.measurement.internal.access.ListPermissionsRequest
import org.wfanet.measurement.internal.access.ListPermissionsResponse
import org.wfanet.measurement.internal.access.Permission
import org.wfanet.measurement.internal.access.PermissionsGrpcKt
import org.wfanet.measurement.internal.access.checkPermissionsResponse
import org.wfanet.measurement.internal.access.listPermissionsPageToken
import org.wfanet.measurement.internal.access.listPermissionsResponse

class SpannerPermissionsService(
  private val databaseClient: AsyncDatabaseClient,
  private val permissionMapping: PermissionMapping,
  private val tlsClientMapping: TlsClientPrincipalMapping,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PermissionsGrpcKt.PermissionsCoroutineImplBase(coroutineContext) {
  override suspend fun getPermission(request: GetPermissionRequest): Permission {
    if (request.permissionResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("permission_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val mappingPermission: PermissionMapping.Permission =
      permissionMapping.getPermissionByResourceId(request.permissionResourceId)
        ?: throw PermissionNotFoundException(request.permissionResourceId)
          .asStatusRuntimeException(Status.Code.NOT_FOUND)

    return mappingPermission.toPermission()
  }

  override suspend fun listPermissions(request: ListPermissionsRequest): ListPermissionsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val pageSize: Int =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val mappingPermissions: List<PermissionMapping.Permission> = permissionMapping.permissions
    val fromIndex: Int =
      if (request.hasPageToken()) {
        val searchResult: Int =
          mappingPermissions.binarySearchBy(request.pageToken.after.permissionResourceId) {
            it.permissionResourceId
          }
        abs(searchResult + 1)
      } else {
        0
      }
    val toIndex: Int = (fromIndex + pageSize).coerceAtMost(mappingPermissions.size)
    val permissions: List<Permission> =
      mappingPermissions.subList(fromIndex, toIndex).map { it.toPermission() }

    return listPermissionsResponse {
      this.permissions += permissions
      if (toIndex < mappingPermissions.size) {
        nextPageToken = listPermissionsPageToken {
          after =
            ListPermissionsPageTokenKt.after {
              permissionResourceId = permissions.last().permissionResourceId
            }
        }
      }
    }
  }

  override suspend fun checkPermissions(
    request: CheckPermissionsRequest
  ): CheckPermissionsResponse {
    if (request.principalResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("principal_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.permissionResourceIdsList.isEmpty()) {
      throw RequiredFieldNotSetException("permission_resource_ids")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val permissionIds =
      request.permissionResourceIdsList.map {
        val mappingPermission =
          permissionMapping.getPermissionByResourceId(it)
            ?: throw PermissionNotFoundException(it)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        mappingPermission.permissionId
      }

    val tlsClient: TlsClientPrincipalMapping.TlsClient? =
      tlsClientMapping.getByPrincipalResourceId(request.principalResourceId)
    if (tlsClient != null) {
      val permissionResourceIds: List<String> =
        if (request.protectedResourceName in tlsClient.protectedResourceNames) {
          request.permissionResourceIdsList
        } else {
          emptyList()
        }
      return checkPermissionsResponse { this.permissionResourceIds += permissionResourceIds }
    }

    return try {
      val grantedPermissionIds: List<Long> =
        databaseClient.readOnlyTransaction().use { txn ->
          val principalId: Long = txn.getPrincipalIdByResourceId(request.principalResourceId)
          txn.checkPermissions(request.protectedResourceName, principalId, permissionIds)
        }
      checkPermissionsResponse {
        permissionResourceIds +=
          grantedPermissionIds.map {
            checkNotNull(permissionMapping.getPermissionById(it)).permissionResourceId
          }
      }
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
