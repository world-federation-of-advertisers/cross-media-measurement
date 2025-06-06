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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.service.InvalidFieldValueException
import org.wfanet.measurement.access.service.PrincipalAlreadyExistsException
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.PrincipalNotFoundException
import org.wfanet.measurement.access.service.PrincipalNotFoundForTlsClientException
import org.wfanet.measurement.access.service.PrincipalNotFoundForUserException
import org.wfanet.measurement.access.service.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.service.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.CreatePrincipalRequest
import org.wfanet.measurement.access.v1alpha.DeletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.LookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.internal.access.Principal as InternalPrincipal
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt.PrincipalsCoroutineStub as InternalPrincipalsCoroutineStub
import org.wfanet.measurement.internal.access.createUserPrincipalRequest as internalCreateUserPrincipalRequest
import org.wfanet.measurement.internal.access.deletePrincipalRequest as internalDeletePrincipalRequest
import org.wfanet.measurement.internal.access.getPrincipalRequest as internalGetPrincipalRequest
import org.wfanet.measurement.internal.access.lookupPrincipalRequest as internalLookupPrincipalRequest

class PrincipalsService(
  private val internalPrincipalsStub: InternalPrincipalsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PrincipalsGrpcKt.PrincipalsCoroutineImplBase(coroutineContext) {
  override suspend fun getPrincipal(request: GetPrincipalRequest): Principal {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val principalKey =
      PrincipalKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalPrincipal =
      try {
        internalPrincipalsStub.getPrincipal(
          internalGetPrincipalRequest { principalResourceId = principalKey.principalId }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
            PrincipalNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
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

    return internalResponse.toPrincipal()
  }

  override suspend fun createPrincipal(request: CreatePrincipalRequest): Principal {
    when (request.principal.identityCase) {
      Principal.IdentityCase.USER -> {}
      Principal.IdentityCase.TLS_CLIENT ->
        throw PrincipalTypeNotSupportedException(request.principal.identityCase)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      else ->
        throw RequiredFieldNotSetException("principal.identity")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.principal.user.issuer.isEmpty()) {
      throw RequiredFieldNotSetException("principal.user.issuer")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.principal.user.subject.isEmpty()) {
      throw RequiredFieldNotSetException("principal.user.subject")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.principalId.isEmpty()) {
      throw RequiredFieldNotSetException("principal_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!ResourceIds.RFC_1034_REGEX.matches(request.principalId)) {
      throw InvalidFieldValueException("principal_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalPrincipal =
      try {
        internalPrincipalsStub.createUserPrincipal(
          internalCreateUserPrincipalRequest {
            principalResourceId = request.principalId
            user = request.principal.user.toInternal()
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS ->
            PrincipalAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
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

    return internalResponse.toPrincipal()
  }

  override suspend fun deletePrincipal(request: DeletePrincipalRequest): Empty {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val principalKey =
      PrincipalKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    try {
      internalPrincipalsStub.deletePrincipal(
        internalDeletePrincipalRequest { principalResourceId = principalKey.principalId }
      )
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
          PrincipalNotFoundException(request.name, e)
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED ->
          PrincipalTypeNotSupportedException(Principal.IdentityCase.TLS_CLIENT, e)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
        InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
        InternalErrors.Reason.PERMISSION_NOT_FOUND,
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

    return Empty.getDefaultInstance()
  }

  override suspend fun lookupPrincipal(request: LookupPrincipalRequest): Principal {
    when (request.lookupKeyCase) {
      LookupPrincipalRequest.LookupKeyCase.USER -> {
        if (request.user.issuer.isEmpty()) {
          throw RequiredFieldNotSetException("user.issuer")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        if (request.user.subject.isEmpty()) {
          throw RequiredFieldNotSetException("user.subject")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
      LookupPrincipalRequest.LookupKeyCase.TLS_CLIENT -> {
        if (request.tlsClient.authorityKeyIdentifier.isEmpty) {
          throw RequiredFieldNotSetException("tlsclient.authoritykeyidentifier")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
      else ->
        throw RequiredFieldNotSetException("lookup_key")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalPrincipal =
      try {
        internalPrincipalsStub.lookupPrincipal(
          internalLookupPrincipalRequest {
            if (request.lookupKeyCase == LookupPrincipalRequest.LookupKeyCase.USER) {
              user = request.user.toInternal()
            } else {
              tlsClient = request.tlsClient.toInternal()
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER ->
            PrincipalNotFoundForUserException(request.user.issuer, request.user.subject, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT ->
            PrincipalNotFoundForTlsClientException.fromInternal(e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
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

    return internalResponse.toPrincipal()
  }
}
