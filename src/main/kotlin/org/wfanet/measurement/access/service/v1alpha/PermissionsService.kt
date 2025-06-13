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

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.service.InvalidFieldValueException
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PermissionNotFoundException
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.PrincipalNotFoundException
import org.wfanet.measurement.access.service.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.GetPermissionRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.listPermissionsResponse
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.access.CheckPermissionsResponse as InternalCheckPermissionsResponse
import org.wfanet.measurement.internal.access.ListPermissionsPageToken as InternalListPermissionsPageToken
import org.wfanet.measurement.internal.access.ListPermissionsResponse as InternalListPermissionsResponse
import org.wfanet.measurement.internal.access.Permission as InternalPermission
import org.wfanet.measurement.internal.access.PermissionsGrpcKt.PermissionsCoroutineStub as InternalPermissionsCoroutineStub
import org.wfanet.measurement.internal.access.checkPermissionsRequest as internalCheckPermissionsRequest
import org.wfanet.measurement.internal.access.getPermissionRequest as internalGetPermissionRequest
import org.wfanet.measurement.internal.access.listPermissionsRequest as internalListPermissionsRequest

class PermissionsService(
  private val internalPermissionStub: InternalPermissionsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PermissionsGrpcKt.PermissionsCoroutineImplBase(coroutineContext) {
  override suspend fun getPermission(request: GetPermissionRequest): Permission {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val permissionKey =
      PermissionKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalPermission =
      try {
        internalPermissionStub.getPermission(
          internalGetPermissionRequest { permissionResourceId = permissionKey.permissionId }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PERMISSION_NOT_FOUND ->
            PermissionNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPermission()
  }

  override suspend fun listPermissions(request: ListPermissionsRequest): ListPermissionsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName -> "$fieldName cannot be negative" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalPageToken: InternalListPermissionsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListPermissionsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListPermissionsResponse =
      try {
        internalPermissionStub.listPermissions(
          internalListPermissionsRequest {
            pageSize = request.pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return listPermissionsResponse {
      permissions += internalResponse.permissionsList.map { it.toPermission() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun checkPermissions(
    request: CheckPermissionsRequest
  ): CheckPermissionsResponse {
    if (request.principal.isEmpty()) {
      throw RequiredFieldNotSetException("principal")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val principalKey =
      PrincipalKey.fromName(request.principal)
        ?: throw InvalidFieldValueException("principal")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.permissionsList.isEmpty()) {
      throw RequiredFieldNotSetException("permissions")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val permissionResourceIds =
      request.permissionsList.map { permission ->
        val permissionKey =
          PermissionKey.fromName(permission)
            ?: throw InvalidFieldValueException("permissions")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        permissionKey.permissionId
      }

    val internalResponse: InternalCheckPermissionsResponse =
      try {
        internalPermissionStub.checkPermissions(
          internalCheckPermissionsRequest {
            protectedResourceName = request.protectedResource
            principalResourceId = principalKey.principalId
            this.permissionResourceIds += permissionResourceIds
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
            PrincipalNotFoundException(request.principal, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PERMISSION_NOT_FOUND ->
            PermissionNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return checkPermissionsResponse {
      permissions += internalResponse.permissionResourceIdsList.map { PermissionKey(it).toName() }
    }
  }
}
