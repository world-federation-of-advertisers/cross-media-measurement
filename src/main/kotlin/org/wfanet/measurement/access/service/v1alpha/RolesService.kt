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

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.access.service.EtagMismatchException
import org.wfanet.measurement.access.service.InvalidFieldValueException
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PermissionNotFoundException
import org.wfanet.measurement.access.service.PermissionNotFoundForRoleException
import org.wfanet.measurement.access.service.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.ResourceTypeNotFoundInPermissionException
import org.wfanet.measurement.access.service.RoleAlreadyExistsException
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.service.RoleNotFoundException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.CreateRoleRequest
import org.wfanet.measurement.access.v1alpha.DeleteRoleRequest
import org.wfanet.measurement.access.v1alpha.GetRoleRequest
import org.wfanet.measurement.access.v1alpha.ListRolesRequest
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt
import org.wfanet.measurement.access.v1alpha.UpdateRoleRequest
import org.wfanet.measurement.access.v1alpha.listRolesResponse
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.access.ListRolesPageToken as InternalListRolesPageToken
import org.wfanet.measurement.internal.access.Role as InternalRole
import org.wfanet.measurement.internal.access.RolesGrpcKt.RolesCoroutineStub as InternalRolesCoroutineStub
import org.wfanet.measurement.internal.access.deleteRoleRequest as internalDeleteRoleRequest
import org.wfanet.measurement.internal.access.getRoleRequest as internalGetRoleRequest
import org.wfanet.measurement.internal.access.listRolesRequest as internalListRolesRequest
import org.wfanet.measurement.internal.access.role as internalRole

class RolesService(
  private val internalRolesStub: InternalRolesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RolesGrpcKt.RolesCoroutineImplBase(coroutineContext) {
  override suspend fun getRole(request: GetRoleRequest): Role {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      RoleKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalRole =
      try {
        internalRolesStub.getRole(internalGetRoleRequest { roleResourceId = key.roleId })
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.ROLE_NOT_FOUND ->
            RoleNotFoundException(request.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toRole()
  }

  override suspend fun listRoles(request: ListRolesRequest): ListRolesResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName -> "$fieldName cannot be negative" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalPageToken: InternalListRolesPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListRolesPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val resourceLists =
      internalRolesStub.listResources(pageSize, internalPageToken) { pageToken, remaining ->
        val response =
          listRoles(
            internalListRolesRequest {
              this.pageSize = remaining
              if (pageToken != null) {
                this.pageToken = pageToken
              }
            }
          )
        val nextPageToken =
          if (response.hasNextPageToken()) {
            response.nextPageToken
          } else {
            null
          }
        ResourceList(response.rolesList, nextPageToken)
      }

    return listRolesResponse {
      resourceLists.collect { resourceList ->
        roles += resourceList.resources.map { it.toRole() }
        val internalNextPageToken: InternalListRolesPageToken? = resourceList.nextPageToken
        if (internalNextPageToken == null) {
          clearNextPageToken()
        } else {
          nextPageToken = internalNextPageToken.toByteString().base64UrlEncode()
        }
      }
    }
  }

  override suspend fun createRole(request: CreateRoleRequest): Role {
    if (!request.hasRole()) {
      throw RequiredFieldNotSetException("role")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.roleId.isEmpty()) {
      throw RequiredFieldNotSetException("role_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!ResourceIds.RFC_1034_REGEX.matches(request.roleId)) {
      throw InvalidFieldValueException("role_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val permissionKeys =
      request.role.permissionsList.map {
        PermissionKey.fromName(it)
          ?: throw InvalidFieldValueException("role.permissions")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    val internalResponse: InternalRole =
      try {
        internalRolesStub.createRole(
          internalRole {
            roleResourceId = request.roleId
            resourceTypes += request.role.resourceTypesList
            permissionResourceIds += permissionKeys.map { it.permissionId }
            etag = request.role.etag
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PERMISSION_NOT_FOUND ->
            PermissionNotFoundException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION ->
            ResourceTypeNotFoundInPermissionException.fromInternal(e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.ROLE_ALREADY_EXISTS ->
            RoleAlreadyExistsException(request.role.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toRole()
  }

  override suspend fun updateRole(request: UpdateRoleRequest): Role {
    if (request.role.name.isEmpty()) {
      throw RequiredFieldNotSetException("role.name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      RoleKey.fromName(request.role.name)
        ?: throw InvalidFieldValueException("role.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    val permissionKeys =
      request.role.permissionsList.map {
        PermissionKey.fromName(it)
          ?: throw InvalidFieldValueException("role.permissions")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    val internalResponse: InternalRole =
      try {
        internalRolesStub.updateRole(
          internalRole {
            roleResourceId = key.roleId
            resourceTypes += request.role.resourceTypesList
            permissionResourceIds += permissionKeys.map { it.permissionId }
            etag = request.role.etag
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.ROLE_NOT_FOUND ->
            RoleNotFoundException(request.role.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE ->
            PermissionNotFoundForRoleException(request.role.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PERMISSION_NOT_FOUND ->
            PermissionNotFoundException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION ->
            ResourceTypeNotFoundInPermissionException.fromInternal(e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toRole()
  }

  override suspend fun deleteRole(request: DeleteRoleRequest): Empty {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      RoleKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    try {
      internalRolesStub.deleteRole(internalDeleteRoleRequest { roleResourceId = key.roleId })
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.ROLE_NOT_FOUND ->
          RoleNotFoundException(request.name, e).asStatusRuntimeException(e.status.code)
        InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
        InternalErrors.Reason.ETAG_MISMATCH,
        InternalErrors.Reason.PERMISSION_NOT_FOUND,
        InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
        InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
        InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
        InternalErrors.Reason.ROLE_ALREADY_EXISTS,
        InternalErrors.Reason.POLICY_NOT_FOUND,
        InternalErrors.Reason.POLICY_ALREADY_EXISTS,
        InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
        InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE,
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
        InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }

    return Empty.getDefaultInstance()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}
