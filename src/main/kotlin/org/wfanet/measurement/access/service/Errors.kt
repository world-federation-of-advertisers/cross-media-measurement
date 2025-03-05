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

package org.wfanet.measurement.access.service

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo

object Errors {
  const val DOMAIN = "access.halo-cmm.org"

  enum class Reason {
    PRINCIPAL_NOT_FOUND,
    PRINCIPAL_NOT_FOUND_FOR_USER,
    PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
    PRINCIPAL_ALREADY_EXISTS,
    PRINCIPAL_TYPE_NOT_SUPPORTED,
    PERMISSION_NOT_FOUND,
    PERMISSION_NOT_FOUND_FOR_ROLE,
    ROLE_NOT_FOUND,
    ROLE_ALREADY_EXISTS,
    POLICY_NOT_FOUND,
    POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
    POLICY_ALREADY_EXISTS,
    POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
    POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
    RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
    REQUIRED_FIELD_NOT_SET,
    INVALID_FIELD_VALUE,
    ETAG_MISMATCH,
  }

  enum class Metadata(val key: String) {
    PRINCIPAL("principal"),
    PERMISSION("permission"),
    ROLE("role"),
    POLICY("policy"),
    RESOURCE_TYPE("resourceType"),
    PROTECTED_RESOURCE("protectedResource"),
    PRINCIPAL_TYPE("principalType"),
    FIELD_NAME("fieldName"),
    AUTHORITY_KEY_IDENTIFIER("authorityKeyIdentifier"),
    ISSUER("issuer"),
    SUBJECT("subject"),
    REQUEST_ETAG("requestEtag"),
    ETAG("etag"),
  }
}

sealed class ServiceException(
  private val reason: Errors.Reason,
  message: String,
  private val metadata: Map<Errors.Metadata, String>,
  cause: Throwable?,
) : Exception(message, cause) {
  override val message: String
    get() = super.message!!

  fun asStatusRuntimeException(code: Status.Code): StatusRuntimeException {
    val source = this
    val errorInfo = errorInfo {
      domain = Errors.DOMAIN
      reason = source.reason.name
      metadata.putAll(source.metadata.mapKeys { it.key.key })
    }
    return CommonErrors.buildStatusRuntimeException(code, message, errorInfo, this)
  }

  abstract class Factory<T : ServiceException> {
    protected abstract val reason: Errors.Reason

    protected abstract fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): T

    fun fromInternal(cause: StatusException): T {
      val errorInfo = requireNotNull(cause.errorInfo)
      require(errorInfo.domain == InternalErrors.DOMAIN)
      require(errorInfo.reason == reason.name)
      return fromInternal(InternalErrors.parseMetadata(errorInfo), cause)
    }
  }
}

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "Required field $fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class InvalidFieldValueException(
  fieldName: String,
  cause: Throwable? = null,
  buildMessage: (fieldName: String) -> String = { "Invalid value for field $fieldName" },
) :
  ServiceException(
    Errors.Reason.INVALID_FIELD_VALUE,
    buildMessage(fieldName),
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class EtagMismatchException(requestEtag: String, etag: String, cause: Throwable? = null) :
  ServiceException(
    reason,
    "Request etag $requestEtag does not match actual etag $etag",
    mapOf(Errors.Metadata.REQUEST_ETAG to requestEtag, Errors.Metadata.ETAG to etag),
    cause,
  ) {
  companion object : Factory<EtagMismatchException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.ETAG_MISMATCH

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): EtagMismatchException {
      return EtagMismatchException(
        internalMetadata.getValue(InternalErrors.Metadata.REQUEST_ETAG),
        internalMetadata.getValue(InternalErrors.Metadata.ETAG),
        cause,
      )
    }
  }
}

class PrincipalNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    reason,
    "Principal $name not found",
    mapOf(Errors.Metadata.PRINCIPAL to name),
    cause,
  ) {
  companion object : Factory<PrincipalNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.PRINCIPAL_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): PrincipalNotFoundException {
      val principalKey =
        PrincipalKey(internalMetadata.getValue(InternalErrors.Metadata.PRINCIPAL_RESOURCE_ID))
      return PrincipalNotFoundException(principalKey.toName(), cause)
    }
  }
}

class PrincipalAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_ALREADY_EXISTS,
    "Principal already exists",
    emptyMap(),
    cause,
  )

class PrincipalTypeNotSupportedException(
  identityCase: Principal.IdentityCase,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
    "Principal type ${identityCase.name} not supported",
    mapOf(Errors.Metadata.PRINCIPAL_TYPE to identityCase.name),
    cause,
  )

class PrincipalNotFoundForUserException(issuer: String, subject: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
    "Principal not found for user with issuer $issuer and subject $subject",
    mapOf(Errors.Metadata.ISSUER to issuer, Errors.Metadata.SUBJECT to subject),
    cause,
  )

class PrincipalNotFoundForTlsClientException(
  authorityKeyIdentifier: String,
  cause: Throwable? = null,
) :
  ServiceException(
    reason,
    "Principal not found for tls client with authority key identifier $authorityKeyIdentifier",
    mapOf(Errors.Metadata.AUTHORITY_KEY_IDENTIFIER to authorityKeyIdentifier),
    cause,
  ) {
  companion object : Factory<PrincipalNotFoundForTlsClientException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): PrincipalNotFoundForTlsClientException {
      return PrincipalNotFoundForTlsClientException(
        internalMetadata.getValue(InternalErrors.Metadata.AUTHORITY_KEY_IDENTIFIER),
        cause,
      )
    }
  }
}

class PermissionNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    reason,
    "Permission $name not found",
    mapOf(Errors.Metadata.PERMISSION to name),
    cause,
  ) {
  constructor(key: PermissionKey, cause: Throwable? = null) : this(key.toName(), cause)

  companion object : Factory<PermissionNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.PERMISSION_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): PermissionNotFoundException {
      return PermissionNotFoundException(
        PermissionKey(internalMetadata.getValue(InternalErrors.Metadata.PERMISSION_RESOURCE_ID)),
        cause,
      )
    }
  }
}

class PermissionNotFoundForRoleException(role: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
    "Permission not found for role $role",
    mapOf(Errors.Metadata.ROLE to role),
    cause,
  )

class RoleNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(reason, "Role $name not found", mapOf(Errors.Metadata.ROLE to name), cause) {
  companion object : Factory<RoleNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.ROLE_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): RoleNotFoundException {
      val roleKey = RoleKey(internalMetadata.getValue(InternalErrors.Metadata.ROLE_RESOURCE_ID))
      return RoleNotFoundException(roleKey.toName(), cause)
    }
  }
}

class RoleAlreadyExistsException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.ROLE_ALREADY_EXISTS,
    "Role $name already exists",
    mapOf(Errors.Metadata.ROLE to name),
    cause,
  )

class ResourceTypeNotFoundInPermissionException(
  resourceType: String,
  permission: String,
  cause: Throwable? = null,
) :
  ServiceException(
    reason,
    "Resource type $resourceType not found in Permission $permission",
    mapOf(Errors.Metadata.RESOURCE_TYPE to resourceType, Errors.Metadata.PERMISSION to permission),
    cause,
  ) {
  companion object : Factory<ResourceTypeNotFoundInPermissionException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): ResourceTypeNotFoundInPermissionException {
      val permissionKey =
        PermissionKey(internalMetadata.getValue(InternalErrors.Metadata.PERMISSION_RESOURCE_ID))
      return ResourceTypeNotFoundInPermissionException(
        internalMetadata.getValue(InternalErrors.Metadata.RESOURCE_TYPE),
        permissionKey.toName(),
        cause,
      )
    }
  }
}

class PolicyNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.POLICY_NOT_FOUND,
    "Policy $name not found",
    mapOf(Errors.Metadata.POLICY to name),
    cause,
  )

class PolicyNotFoundForProtectedResourceException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
    "Policy with protected resource $name not found",
    mapOf(Errors.Metadata.PROTECTED_RESOURCE to name),
    cause,
  )

class PolicyAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(Errors.Reason.POLICY_ALREADY_EXISTS, "Policy already exists", emptyMap(), cause)

class PolicyBindingMembershipAlreadyExistsException(
  policyName: String,
  roleName: String,
  principalName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    reason,
    "Principal $principalName is already a member of role $roleName on policy $policyName",
    mapOf(
      Errors.Metadata.POLICY to policyName,
      Errors.Metadata.ROLE to roleName,
      Errors.Metadata.PRINCIPAL to principalName,
    ),
    cause,
  ) {
  companion object : Factory<PolicyBindingMembershipAlreadyExistsException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): PolicyBindingMembershipAlreadyExistsException {
      val policyKey =
        PolicyKey(internalMetadata.getValue(InternalErrors.Metadata.POLICY_RESOURCE_ID))
      val roleKey = RoleKey(internalMetadata.getValue(InternalErrors.Metadata.ROLE_RESOURCE_ID))
      val principalKey =
        PrincipalKey(internalMetadata.getValue(InternalErrors.Metadata.PRINCIPAL_RESOURCE_ID))
      return PolicyBindingMembershipAlreadyExistsException(
        policyKey.toName(),
        roleKey.toName(),
        principalKey.toName(),
        cause,
      )
    }
  }
}

class PolicyBindingMembershipNotFoundException(
  policyName: String,
  roleName: String,
  principalName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    reason,
    "Principal $principalName is not a member of role $roleName on policy $policyName",
    mapOf(
      Errors.Metadata.POLICY to policyName,
      Errors.Metadata.ROLE to roleName,
      Errors.Metadata.PRINCIPAL to principalName,
    ),
    cause,
  ) {
  companion object : Factory<PolicyBindingMembershipNotFoundException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): PolicyBindingMembershipNotFoundException {
      val policyKey =
        PolicyKey(internalMetadata.getValue(InternalErrors.Metadata.POLICY_RESOURCE_ID))
      val roleKey = RoleKey(internalMetadata.getValue(InternalErrors.Metadata.ROLE_RESOURCE_ID))
      val principalKey =
        PrincipalKey(internalMetadata.getValue(InternalErrors.Metadata.PRINCIPAL_RESOURCE_ID))
      return PolicyBindingMembershipNotFoundException(
        policyKey.toName(),
        roleKey.toName(),
        principalKey.toName(),
        cause,
      )
    }
  }
}
