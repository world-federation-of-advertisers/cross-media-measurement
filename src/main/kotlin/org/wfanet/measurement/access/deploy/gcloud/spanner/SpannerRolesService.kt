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

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Empty
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.RoleResult
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deletePolicyBinding
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deleteRole
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deleteRolePermission
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deleteRoleResourceType
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getRoleByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getRoleIdByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertRole
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertRolePermission
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertRoleResourceType
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.readPolicyBindingsByRoleId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.readRoles
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.roleExists
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.updateRole
import org.wfanet.measurement.access.service.internal.EtagMismatchException
import org.wfanet.measurement.access.service.internal.InvalidFieldValueException
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.PermissionNotFoundException
import org.wfanet.measurement.access.service.internal.PermissionNotFoundForRoleException
import org.wfanet.measurement.access.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.internal.ResourceTypeNotFoundInPermissionException
import org.wfanet.measurement.access.service.internal.RoleAlreadyExistsException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.access.DeleteRoleRequest
import org.wfanet.measurement.internal.access.GetRoleRequest
import org.wfanet.measurement.internal.access.ListRolesPageTokenKt
import org.wfanet.measurement.internal.access.ListRolesRequest
import org.wfanet.measurement.internal.access.ListRolesResponse
import org.wfanet.measurement.internal.access.Role
import org.wfanet.measurement.internal.access.RolesGrpcKt
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.listRolesPageToken
import org.wfanet.measurement.internal.access.listRolesResponse
import org.wfanet.measurement.internal.access.role

class SpannerRolesService(
  private val databaseClient: AsyncDatabaseClient,
  private val permissionMapping: PermissionMapping,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RolesGrpcKt.RolesCoroutineImplBase(coroutineContext) {
  override suspend fun getRole(request: GetRoleRequest): Role {
    if (request.roleResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("role_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val roleResult: RoleResult =
      try {
        databaseClient.singleUse().use { txn ->
          txn.getRoleByResourceId(permissionMapping, request.roleResourceId)
        }
      } catch (e: PermissionNotFoundForRoleException) {
        // This means that an expected Permission is missing from the mapping.
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      } catch (e: RoleNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return roleResult.role
  }

  override suspend fun listRoles(request: ListRolesRequest): ListRolesResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("max_page_size") { fieldName ->
        "$fieldName must be non-negative"
      }
    }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }
    val after = if (request.hasPageToken()) request.pageToken.after else null

    return try {
      databaseClient.singleUse().use { txn ->
        val roles: Flow<Role> =
          txn.readRoles(permissionMapping, pageSize + 1, after).map { it.role }
        listRolesResponse {
          roles.collectIndexed { index, role ->
            if (index == pageSize) {
              nextPageToken = listRolesPageToken {
                this.after =
                  ListRolesPageTokenKt.after {
                    roleResourceId = this@listRolesResponse.roles.last().roleResourceId
                  }
              }
            } else {
              this.roles += role
            }
          }
        }
      }
    } catch (e: PermissionNotFoundForRoleException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL)
    }
  }

  override suspend fun createRole(request: Role): Role {
    try {
      validateRequestRole(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val permissionIds =
      request.permissionResourceIdsList.map { permissionResourceId ->
        try {
          getPermissionByResourceId(permissionResourceId)
            .also { it.checkResourceTypes(request.resourceTypesList) }
            .permissionId
        } catch (e: PermissionNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: ResourceTypeNotFoundInPermissionException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }
      }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=createRole"))
    val commitTimestamp: Timestamp =
      try {
        transactionRunner.run { txn ->
          val roleId = idGenerator.generateNewId { id -> txn.roleExists(id) }
          txn.insertRole(roleId, request.roleResourceId)
          for (permissionId in permissionIds) {
            txn.insertRolePermission(roleId, permissionId)
          }
          for (resourceType in request.resourceTypesList) {
            txn.insertRoleResourceType(roleId, resourceType)
          }
        }
        transactionRunner.getCommitTimestamp().toProto()
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RoleAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        } else {
          throw e
        }
      }

    return role {
      roleResourceId = request.roleResourceId
      createTime = commitTimestamp
      updateTime = commitTimestamp
      permissionResourceIds += request.permissionResourceIdsList
      resourceTypes += request.resourceTypesList
      etag = ETags.computeETag(updateTime.toInstant())
    }
  }

  override suspend fun updateRole(request: Role): Role {
    try {
      validateRequestRole(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=updateRole"))
    transactionRunner.run { txn ->
      val (roleId: Long, role: Role) =
        try {
          txn.getRoleByResourceId(permissionMapping, request.roleResourceId)
        } catch (e: RoleNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: PermissionNotFoundForRoleException) {
          throw e.asStatusRuntimeException(Status.Code.INTERNAL)
        }
      try {
        EtagMismatchException.check(request.etag, role.etag)
      } catch (e: EtagMismatchException) {
        throw e.asStatusRuntimeException(Status.Code.ABORTED)
      }

      txn.updateRole(roleId)

      val requestResourceTypes = request.resourceTypesList.toSet()
      val existingResourceTypes = role.resourceTypesList.toSet()
      for (resourceType in requestResourceTypes.subtract(existingResourceTypes)) {
        txn.insertRoleResourceType(roleId, resourceType)
      }
      for (resourceType in existingResourceTypes.subtract(requestResourceTypes)) {
        txn.deleteRoleResourceType(roleId, resourceType)
      }

      val requestPermissionResourceIds = request.permissionResourceIdsList.toSet()
      val existingPermissionResourceIds = role.permissionResourceIdsList.toSet()
      for (permissionResourceId in
        requestPermissionResourceIds.union(existingPermissionResourceIds)) {
        val permissionId =
          try {
            getPermissionByResourceId(permissionResourceId)
              .also { it.checkResourceTypes(requestResourceTypes) }
              .permissionId
          } catch (e: PermissionNotFoundException) {
            val statusCode =
              if (permissionResourceId in existingPermissionResourceIds) {
                Status.Code.INTERNAL // Existing permission should be found.
              } else {
                Status.Code.FAILED_PRECONDITION
              }
            throw e.asStatusRuntimeException(statusCode)
          } catch (e: ResourceTypeNotFoundInPermissionException) {
            throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          }

        if (permissionResourceId !in requestPermissionResourceIds) {
          txn.deleteRolePermission(roleId, permissionId)
        } else if (permissionResourceId !in existingPermissionResourceIds) {
          txn.insertRolePermission(roleId, permissionId)
        }
      }
    }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return request.copy {
      updateTime = commitTimestamp
      etag = ETags.computeETag(updateTime.toInstant())
    }
  }

  override suspend fun deleteRole(request: DeleteRoleRequest): Empty {
    if (request.roleResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("role_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      databaseClient.readWriteTransaction(Options.tag("action=deleteRole")).run { txn ->
        val roleId = txn.getRoleIdByResourceId(request.roleResourceId)

        txn.readPolicyBindingsByRoleId(roleId).collect { policyBindingResult ->
          txn.deletePolicyBinding(
            policyId = policyBindingResult.policyId,
            roleId = policyBindingResult.roleId,
            principalId = policyBindingResult.principalId,
          )
        }

        txn.deleteRole(roleId)
      }
    } catch (e: RoleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    return Empty.getDefaultInstance()
  }

  /**
   * Returns the [PermissionMapping.Permission] with the specified [permissionResourceId].
   *
   * @throws PermissionNotFoundException
   */
  private fun getPermissionByResourceId(
    permissionResourceId: String
  ): PermissionMapping.Permission {
    return permissionMapping.getPermissionByResourceId(permissionResourceId)
      ?: throw PermissionNotFoundException(permissionResourceId)
  }

  /**
   * Checks whether the specified request role is valid.
   *
   * @throws RequiredFieldNotSetException
   */
  private fun validateRequestRole(role: Role) {
    if (role.roleResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("role_resource_id")
    }
    if (role.permissionResourceIdsList.isEmpty()) {
      throw RequiredFieldNotSetException("permission_resource_ids")
    }
    if (role.resourceTypesList.isEmpty()) {
      throw RequiredFieldNotSetException("resource_types")
    }
  }

  /**
   * Checks whether this permission has all the resource types in [resourceTypes].
   *
   * @throws ResourceTypeNotFoundInPermissionException
   */
  private fun PermissionMapping.Permission.checkResourceTypes(resourceTypes: Iterable<String>) {
    for (resourceType in resourceTypes) {
      if (!protectedResourceTypes.contains(resourceType)) {
        throw ResourceTypeNotFoundInPermissionException(resourceType, permissionResourceId)
      }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
