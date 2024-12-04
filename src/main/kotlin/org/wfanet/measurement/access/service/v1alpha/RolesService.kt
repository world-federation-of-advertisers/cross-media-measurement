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

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.access.service.EtagMismatchException
import org.wfanet.measurement.access.service.InvalidFieldValueException
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PermissionNotFoundException
import org.wfanet.measurement.access.service.PermissionNotFoundForRoleException
import org.wfanet.measurement.access.service.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.ResourceTypeNotFoundInPermissionException
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.service.RoleNotFoundException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt
import org.wfanet.measurement.access.v1alpha.UpdateRoleRequest
import org.wfanet.measurement.internal.access.RolesGrpcKt.RolesCoroutineStub as InternalRolesCoroutineStub
import org.wfanet.measurement.internal.access.role as internalRole

class RolesService(private val internalRolesStub: InternalRolesCoroutineStub) :
  RolesGrpcKt.RolesCoroutineImplBase() {
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

    val internalResponse =
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
        val exception: StatusRuntimeException =
          when (InternalErrors.getReason(e)) {
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
        throw exception
      }

    return internalResponse.toRole()
  }
}
