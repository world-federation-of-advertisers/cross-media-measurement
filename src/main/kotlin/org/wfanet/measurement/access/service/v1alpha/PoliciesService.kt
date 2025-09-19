// Copyright 2025 The Cross-Media Measurement Authors
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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.service.EtagMismatchException
import org.wfanet.measurement.access.service.InvalidFieldValueException
import org.wfanet.measurement.access.service.PolicyAlreadyExistsException
import org.wfanet.measurement.access.service.PolicyBindingMembershipAlreadyExistsException
import org.wfanet.measurement.access.service.PolicyBindingMembershipNotFoundException
import org.wfanet.measurement.access.service.PolicyKey
import org.wfanet.measurement.access.service.PolicyNotFoundException
import org.wfanet.measurement.access.service.PolicyNotFoundForProtectedResourceException
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.PrincipalNotFoundException
import org.wfanet.measurement.access.service.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.service.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.service.RoleNotFoundException
import org.wfanet.measurement.access.service.internal.Errors as InternalErrors
import org.wfanet.measurement.access.v1alpha.AddPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.CreatePolicyRequest
import org.wfanet.measurement.access.v1alpha.GetPolicyRequest
import org.wfanet.measurement.access.v1alpha.LookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.PoliciesGrpcKt
import org.wfanet.measurement.access.v1alpha.Policy
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.RemovePolicyBindingMembersRequest
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.internal.access.PoliciesGrpcKt.PoliciesCoroutineStub as InternalPoliciesCoroutineStub
import org.wfanet.measurement.internal.access.Policy as InternalPolicy
import org.wfanet.measurement.internal.access.Policy.Members
import org.wfanet.measurement.internal.access.PolicyKt
import org.wfanet.measurement.internal.access.addPolicyBindingMembersRequest as internalAddPolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.getPolicyRequest as internalGetPolicyRequest
import org.wfanet.measurement.internal.access.lookupPolicyRequest as internalLookupPolicyRequest
import org.wfanet.measurement.internal.access.policy as internalPolicy
import org.wfanet.measurement.internal.access.removePolicyBindingMembersRequest as internalRemovePolicyBindingMembersRequest

class PoliciesService(
  private val internalPoliciesStub: InternalPoliciesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PoliciesGrpcKt.PoliciesCoroutineImplBase(coroutineContext) {
  override suspend fun getPolicy(request: GetPolicyRequest): Policy {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val policyKey =
      PolicyKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalPolicy =
      try {
        internalPoliciesStub.getPolicy(
          internalGetPolicyRequest { policyResourceId = policyKey.policyId }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.POLICY_NOT_FOUND ->
            PolicyNotFoundException(request.name, e).asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
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

    return internalResponse.toPolicy()
  }

  override suspend fun createPolicy(request: CreatePolicyRequest): Policy {
    if (!request.hasPolicy()) {
      throw RequiredFieldNotSetException("policy")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.policyId.isEmpty()) {
      throw RequiredFieldNotSetException("policy_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!ResourceIds.RFC_1034_REGEX.matches(request.policyId)) {
      throw InvalidFieldValueException("policy_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val internalBindingMap = mutableMapOf<String, Members>()

    request.policy.bindingsList.forEach { binding ->
      val roleKey =
        RoleKey.fromName(binding.role)
          ?: throw InvalidFieldValueException("policy.bindings.role")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

      val members =
        PolicyKt.members {
          memberPrincipalResourceIds +=
            binding.membersList.map {
              val principalKey =
                PrincipalKey.fromName(it)
                  ?: throw InvalidFieldValueException("policy.bindings.members")
                    .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
              principalKey.principalId
            }
        }

      internalBindingMap[roleKey.roleId] = members
    }

    val internalResponse: InternalPolicy =
      try {
        internalPoliciesStub.createPolicy(
          internalPolicy {
            policyResourceId = request.policyId
            protectedResourceName = request.policy.protectedResource
            bindings.putAll(internalBindingMap)
            etag = request.policy.etag
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED ->
            PrincipalTypeNotSupportedException(Principal.IdentityCase.TLS_CLIENT, e)
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          InternalErrors.Reason.POLICY_ALREADY_EXISTS ->
            PolicyAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.ROLE_NOT_FOUND ->
            RoleNotFoundException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
            PrincipalNotFoundException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPolicy()
  }

  override suspend fun lookupPolicy(request: LookupPolicyRequest): Policy {
    if (!request.hasProtectedResource()) {
      throw RequiredFieldNotSetException("protected_resource")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalPolicy =
      try {
        internalPoliciesStub.lookupPolicy(
          internalLookupPolicyRequest { protectedResourceName = request.protectedResource }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE ->
            PolicyNotFoundForProtectedResourceException(request.protectedResource, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.ROLE_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_NOT_FOUND,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPolicy()
  }

  override suspend fun addPolicyBindingMembers(request: AddPolicyBindingMembersRequest): Policy {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val policyKey =
      PolicyKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.role.isEmpty()) {
      throw RequiredFieldNotSetException("role")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val roleKey =
      RoleKey.fromName(request.role)
        ?: throw InvalidFieldValueException("role")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.membersList.isEmpty()) {
      throw RequiredFieldNotSetException("members")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val memberKeys =
      request.membersList.map { member ->
        PrincipalKey.fromName(member)
          ?: throw InvalidFieldValueException("members")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    val internalResponse: InternalPolicy =
      try {
        internalPoliciesStub.addPolicyBindingMembers(
          internalAddPolicyBindingMembersRequest {
            policyResourceId = policyKey.policyId
            roleResourceId = roleKey.roleId
            memberPrincipalResourceIds += memberKeys.map { it.principalId }
            etag = request.etag
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.POLICY_NOT_FOUND ->
            PolicyNotFoundException(request.name).asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.ROLE_NOT_FOUND ->
            RoleNotFoundException(request.role)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
            PrincipalNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED ->
            PrincipalTypeNotSupportedException(Principal.IdentityCase.TLS_CLIENT)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS ->
            PolicyBindingMembershipAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPolicy()
  }

  override suspend fun removePolicyBindingMembers(
    request: RemovePolicyBindingMembersRequest
  ): Policy {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val policyKey =
      PolicyKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.role.isEmpty()) {
      throw RequiredFieldNotSetException("role")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val roleKey =
      RoleKey.fromName(request.role)
        ?: throw InvalidFieldValueException("role")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.membersList.isEmpty()) {
      throw RequiredFieldNotSetException("members")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val memberKeys =
      request.membersList.map { member ->
        PrincipalKey.fromName(member)
          ?: throw InvalidFieldValueException("members")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    val internalResponse: InternalPolicy =
      try {
        internalPoliciesStub.removePolicyBindingMembers(
          internalRemovePolicyBindingMembersRequest {
            policyResourceId = policyKey.policyId
            roleResourceId = roleKey.roleId
            memberPrincipalResourceIds += memberKeys.map { it.principalId }
            etag = request.etag
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.POLICY_NOT_FOUND ->
            PolicyNotFoundException(request.name).asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.ROLE_NOT_FOUND ->
            RoleNotFoundException(request.role).asStatusRuntimeException(Status.Code.INTERNAL)
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND ->
            PrincipalNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.INTERNAL)
          InternalErrors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED ->
            PrincipalTypeNotSupportedException(Principal.IdentityCase.TLS_CLIENT)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND ->
            PolicyBindingMembershipNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE,
          InternalErrors.Reason.POLICY_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_USER,
          InternalErrors.Reason.PRINCIPAL_NOT_FOUND_FOR_TLS_CLIENT,
          InternalErrors.Reason.PRINCIPAL_ALREADY_EXISTS,
          InternalErrors.Reason.PERMISSION_NOT_FOUND_FOR_ROLE,
          InternalErrors.Reason.ROLE_ALREADY_EXISTS,
          InternalErrors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS,
          InternalErrors.Reason.RESOURCE_TYPE_NOT_FOUND_IN_PERMISSION,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toPolicy()
  }
}
