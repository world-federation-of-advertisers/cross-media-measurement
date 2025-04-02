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

package org.wfanet.measurement.access.service.internal

import com.google.protobuf.ByteString
import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.access.Principal

object Errors {
  const val DOMAIN = "internal.access.halo-cmm.org"

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
    PRINCIPAL_RESOURCE_ID("principalResourceId"),
    PERMISSION_RESOURCE_ID("permissionResourceId"),
    ROLE_RESOURCE_ID("roleResourceId"),
    POLICY_RESOURCE_ID("policyResourceId"),
    RESOURCE_TYPE("resourceType"),
    PROTECTED_RESOURCE_NAME("protectedResourceName"),
    PRINCIPAL_TYPE("principalType"),
    FIELD_NAME("fieldName"),
    AUTHORITY_KEY_IDENTIFIER("authorityKeyIdentifier"),
    ISSUER("issuer"),
    SUBJECT("subject"),
    REQUEST_ETAG("requestEtag"),
    ETAG("etag");

    companion object {
      private val METADATA_BY_KEY by lazy { entries.associateBy { it.key } }

      fun fromKey(key: String): Metadata = METADATA_BY_KEY.getValue(key)
    }
  }

  /**
   * Returns the [Reason] extracted from [exception], or `null` if [exception] is not this type of
   * error.
   */
  fun getReason(exception: StatusException): Reason? {
    val errorInfo = exception.errorInfo ?: return null
    return getReason(errorInfo)
  }

  /**
   * Returns the [Reason] extracted from [errorInfo], or `null` if [errorInfo] is not this type of
   * error.
   */
  fun getReason(errorInfo: ErrorInfo): Reason? {
    if (errorInfo.domain != DOMAIN) {
      return null
    }

    return Reason.valueOf(errorInfo.reason)
  }

  fun parseMetadata(errorInfo: ErrorInfo): Map<Metadata, String> {
    require(errorInfo.domain == DOMAIN) { "Error domain is not $DOMAIN" }
    return errorInfo.metadataMap.mapKeys { Metadata.fromKey(it.key) }
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
}

class PrincipalNotFoundException(principalResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_NOT_FOUND,
    "Principal with resource ID $principalResourceId not found",
    mapOf(Errors.Metadata.PRINCIPAL_RESOURCE_ID to principalResourceId),
    cause,
  )

class PrincipalNotFoundForTlsClientException
private constructor(akidString: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
    "Principal with AKID $akidString not found",
    mapOf(Errors.Metadata.AUTHORITY_KEY_IDENTIFIER to akidString),
    cause,
  ) {
  constructor(
    authorityKeyIdentifier: ByteString,
    cause: Throwable? = null,
  ) : this(authorityKeyIdentifier.toKeyIdString(), cause)

  @OptIn(ExperimentalStdlibApi::class) // For `HexFormat` and `toHexString`.
  companion object {
    private val KEY_ID_FORMAT = HexFormat {
      upperCase = true
      bytes.byteSeparator = ":"
    }

    private fun ByteString.toKeyIdString() = toByteArray().toHexString(KEY_ID_FORMAT)
  }
}

class PrincipalNotFoundForUserException(issuer: String, subject: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
    "Principal with issuer $issuer and subject $subject not found",
    mapOf(Errors.Metadata.ISSUER to issuer, Errors.Metadata.SUBJECT to subject),
    cause,
  )

class PrincipalAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PRINCIPAL_ALREADY_EXISTS,
    "Principal already exists",
    emptyMap(),
    cause,
  )

class PrincipalTypeNotSupportedException(
  principalResourceId: String,
  principalType: Principal.IdentityCase,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
    "Principal with resource ID $principalResourceId has unsupported identity type $principalType",
    mapOf(
      Errors.Metadata.PRINCIPAL_RESOURCE_ID to principalResourceId,
      Errors.Metadata.PRINCIPAL_TYPE to principalType.name,
    ),
    cause,
  )

class PermissionNotFoundException(permissionResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PERMISSION_NOT_FOUND,
    "Permission with resource ID $permissionResourceId not found",
    mapOf(Errors.Metadata.PERMISSION_RESOURCE_ID to permissionResourceId),
    cause,
  )

class PermissionNotFoundForRoleException(roleResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
    "Permission for Role with resource ID $roleResourceId not found",
    mapOf(Errors.Metadata.ROLE_RESOURCE_ID to roleResourceId),
    cause,
  )

class RoleNotFoundException(roleResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.ROLE_NOT_FOUND,
    "Role with resource ID $roleResourceId not found",
    mapOf(Errors.Metadata.ROLE_RESOURCE_ID to roleResourceId),
    cause,
  )

class RoleAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(Errors.Reason.ROLE_ALREADY_EXISTS, "Role already exists", emptyMap(), cause)

class PolicyNotFoundException(policyResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.POLICY_NOT_FOUND,
    "Policy with resource ID $policyResourceId not found",
    mapOf(Errors.Metadata.POLICY_RESOURCE_ID to policyResourceId),
    cause,
  )

class PolicyNotFoundForProtectedResourceException(
  protectedResourceName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
    "Policy with protected resource name $protectedResourceName not found",
    mapOf(Errors.Metadata.PROTECTED_RESOURCE_NAME to protectedResourceName),
    cause,
  )

class PolicyAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(Errors.Reason.POLICY_ALREADY_EXISTS, "Policy already exists", emptyMap(), cause)

class PolicyBindingMembershipAlreadyExistsException(
  policyResourceId: String,
  roleResourceId: String,
  principalResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
    "Principal $principalResourceId is already a member of role " +
      "$roleResourceId on policy $policyResourceId",
    mapOf(
      Errors.Metadata.POLICY_RESOURCE_ID to policyResourceId,
      Errors.Metadata.ROLE_RESOURCE_ID to roleResourceId,
      Errors.Metadata.PRINCIPAL_RESOURCE_ID to principalResourceId,
    ),
    cause,
  )

class PolicyBindingMembershipNotFoundException(
  policyResourceId: String,
  roleResourceId: String,
  principalResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
    "Principal $principalResourceId is not a member of role " +
      "$roleResourceId on policy $policyResourceId",
    mapOf(
      Errors.Metadata.POLICY_RESOURCE_ID to policyResourceId,
      Errors.Metadata.ROLE_RESOURCE_ID to roleResourceId,
      Errors.Metadata.PRINCIPAL_RESOURCE_ID to principalResourceId,
    ),
    cause,
  )

class ResourceTypeNotFoundInPermissionException(
  resourceType: String,
  permissionResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
    "Resource type $resourceType not found in Permission with resource ID $permissionResourceId",
    mapOf(
      Errors.Metadata.RESOURCE_TYPE to resourceType,
      Errors.Metadata.PERMISSION_RESOURCE_ID to permissionResourceId,
    ),
    cause,
  )

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
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
    Errors.Reason.ETAG_MISMATCH,
    "Request etag $requestEtag does not match actual etag $etag",
    mapOf(Errors.Metadata.REQUEST_ETAG to requestEtag, Errors.Metadata.ETAG to etag),
    cause,
  ) {
  companion object {
    /**
     * Checks whether [requestEtag] matches [etag].
     *
     * @throws EtagMismatchException if the etags do not match
     */
    fun check(requestEtag: String, etag: String) {
      if (requestEtag != etag) {
        throw EtagMismatchException(requestEtag, etag)
      }
    }
  }
}
