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
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deletePolicyBinding
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPolicyByProtectedResourceName
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPolicyByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPrincipalIdsByResourceIds
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getRoleIdByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getRoleIdsByResourceIds
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertPolicy
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertPolicyBinding
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.policyExists
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.updatePolicy
import org.wfanet.measurement.access.service.internal.EtagMismatchException
import org.wfanet.measurement.access.service.internal.PolicyAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PolicyBindingMembershipAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PolicyBindingMembershipNotFoundException
import org.wfanet.measurement.access.service.internal.PolicyNotFoundException
import org.wfanet.measurement.access.service.internal.PolicyNotFoundForProtectedResourceException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.access.AddPolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.GetPolicyRequest
import org.wfanet.measurement.internal.access.LookupPolicyRequest
import org.wfanet.measurement.internal.access.PoliciesGrpcKt
import org.wfanet.measurement.internal.access.Policy
import org.wfanet.measurement.internal.access.PolicyKt
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.RemovePolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.policy

class SpannerPoliciesService(
  private val databaseClient: AsyncDatabaseClient,
  private val tlsClientMapping: TlsClientPrincipalMapping,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : PoliciesGrpcKt.PoliciesCoroutineImplBase(coroutineContext) {
  override suspend fun getPolicy(request: GetPolicyRequest): Policy {
    if (request.policyResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("policy_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn.getPolicyByResourceId(request.policyResourceId).policy
      }
    } catch (e: PolicyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun lookupPolicy(request: LookupPolicyRequest): Policy {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf oneof case enums cannot be null.
    return when (request.lookupKeyCase) {
      LookupPolicyRequest.LookupKeyCase.PROTECTED_RESOURCE_NAME -> {
        try {
          databaseClient.singleUse().use { txn ->
            txn.getPolicyByProtectedResourceName(request.protectedResourceName).policy
          }
        } catch (e: PolicyNotFoundForProtectedResourceException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
      LookupPolicyRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
        throw RequiredFieldNotSetException("lookup_key")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  override suspend fun createPolicy(request: Policy): Policy {
    if (request.policyResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("policy_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val bindings: Map<String, Set<String>> =
      request.bindingsMap.mapValues { (_, value) ->
        value.memberPrincipalResourceIdsList.toSet().also {
          if (it.isEmpty())
            throw RequiredFieldNotSetException("member_principal_resource_ids")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    val principalResourceIds = buildSet { bindings.values.forEach { addAll(it) } }
    try {
      checkPrincipalTypes(principalResourceIds)
    } catch (e: PrincipalTypeNotSupportedException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=createPolicy"))
    return try {
      transactionRunner.run { txn ->
        val policyId = idGenerator.generateNewId { id -> txn.policyExists(id) }
        txn.insertPolicy(policyId, request.policyResourceId, request.protectedResourceName)
        val roleIdByResourceId: Map<String, Long> = txn.getRoleIdsByResourceIds(bindings.keys)
        val principalIdByResourceId: Map<String, Long> =
          txn.getPrincipalIdsByResourceIds(principalResourceIds)
        for ((roleResourceId, memberPrincipalResourceIds) in bindings) {
          val roleId = roleIdByResourceId.getValue(roleResourceId)
          for (principalResourceId in memberPrincipalResourceIds) {
            val principalId = principalIdByResourceId.getValue(principalResourceId)
            txn.insertPolicyBinding(policyId, roleId, principalId)
          }
        }
      }
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

      policy {
        policyResourceId = request.policyResourceId
        protectedResourceName = request.protectedResourceName
        createTime = commitTimestamp
        updateTime = commitTimestamp
        etag = ETags.computeETag(updateTime.toInstant())

        for ((roleResourceId, memberPrincipalResourceIds) in bindings) {
          this.bindings[roleResourceId] =
            PolicyKt.members { this.memberPrincipalResourceIds += memberPrincipalResourceIds }
        }
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw PolicyAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      } else {
        throw e
      }
    } catch (e: RoleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun addPolicyBindingMembers(request: AddPolicyBindingMembersRequest): Policy {
    if (request.policyResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("policy_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.roleResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("role_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.memberPrincipalResourceIdsList.isEmpty()) {
      throw RequiredFieldNotSetException("member_principal_resource_ids")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val memberPrincipalResourceIds: Set<String> = request.memberPrincipalResourceIdsList.toSet()
    try {
      checkPrincipalTypes(memberPrincipalResourceIds)
    } catch (e: PrincipalTypeNotSupportedException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=addPolicyBindingMembers"))
    return try {
      val policy =
        transactionRunner.run { txn ->
          val (policyId, policy) = txn.getPolicyByResourceId(request.policyResourceId)
          if (request.etag.isNotEmpty()) {
            EtagMismatchException.check(request.etag, policy.etag)
          }
          val existingPrincipalResourceIds =
            policy.bindingsMap
              .getOrDefault(request.roleResourceId, Policy.Members.getDefaultInstance())
              .memberPrincipalResourceIdsList
              .toSet()
          for (principalResourceId in memberPrincipalResourceIds) {
            if (principalResourceId in existingPrincipalResourceIds) {
              throw PolicyBindingMembershipAlreadyExistsException(
                request.policyResourceId,
                request.roleResourceId,
                principalResourceId,
              )
            }
          }

          val roleId = txn.getRoleIdByResourceId(request.roleResourceId)
          val principalIdByResourceId = txn.getPrincipalIdsByResourceIds(memberPrincipalResourceIds)
          for (principalId in principalIdByResourceId.values) {
            txn.insertPolicyBinding(policyId, roleId, principalId)
          }
          txn.updatePolicy(policyId)
          policy
        }
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

      policy.copy {
        updateTime = commitTimestamp
        etag = ETags.computeETag(updateTime.toInstant())
        bindings[request.roleResourceId] =
          bindings.getOrDefault(request.roleResourceId, Policy.Members.getDefaultInstance()).copy {
            this.memberPrincipalResourceIds += memberPrincipalResourceIds
          }
      }
    } catch (e: PolicyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: RoleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: PolicyBindingMembershipAlreadyExistsException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  override suspend fun removePolicyBindingMembers(
    request: RemovePolicyBindingMembersRequest
  ): Policy {
    if (request.policyResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("policy_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.roleResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("role_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.memberPrincipalResourceIdsList.isEmpty()) {
      throw RequiredFieldNotSetException("member_principal_resource_ids")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val memberPrincipalResourceIds: Set<String> = request.memberPrincipalResourceIdsList.toSet()

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=removePolicyBindingMembers"))
    return try {
      val policy: Policy =
        transactionRunner.run { txn ->
          val (policyId, policy) = txn.getPolicyByResourceId(request.policyResourceId)
          if (request.etag.isNotEmpty()) {
            EtagMismatchException.check(request.etag, policy.etag)
          }
          val existingPrincipalResourceIds =
            policy.bindingsMap
              .getOrDefault(request.roleResourceId, Policy.Members.getDefaultInstance())
              .memberPrincipalResourceIdsList
              .toSet()
          for (principalResourceId in memberPrincipalResourceIds) {
            if (principalResourceId !in existingPrincipalResourceIds) {
              throw PolicyBindingMembershipNotFoundException(
                request.policyResourceId,
                request.roleResourceId,
                principalResourceId,
              )
            }
          }

          val roleId = txn.getRoleIdByResourceId(request.roleResourceId)
          val principalIdByResourceId = txn.getPrincipalIdsByResourceIds(memberPrincipalResourceIds)
          for (principalId in principalIdByResourceId.values) {
            txn.deletePolicyBinding(policyId, roleId, principalId)
          }
          txn.updatePolicy(policyId)
          policy
        }
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

      val updatedMemberPrincipalResourceIds: List<String> =
        policy.bindingsMap.getValue(request.roleResourceId).memberPrincipalResourceIdsList.filter {
          it !in memberPrincipalResourceIds
        }
      policy.copy {
        updateTime = commitTimestamp
        etag = ETags.computeETag(updateTime.toInstant())
        if (updatedMemberPrincipalResourceIds.isEmpty()) {
          bindings.remove(request.roleResourceId)
        } else {
          bindings[request.roleResourceId] =
            PolicyKt.members {
              this.memberPrincipalResourceIds += updatedMemberPrincipalResourceIds
            }
        }
      }
    } catch (e: PolicyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: RoleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL)
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL)
    } catch (e: PolicyBindingMembershipNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  /**
   * Checks that each [Principal] represented by [principalResourceIds] has a supported type.
   *
   * @throws PrincipalTypeNotSupportedException
   */
  private fun checkPrincipalTypes(principalResourceIds: Iterable<String>) {
    for (principalResourceId in principalResourceIds) {
      if (tlsClientMapping.getByPrincipalResourceId(principalResourceId) != null) {
        throw PrincipalTypeNotSupportedException(
          principalResourceId,
          Principal.IdentityCase.TLS_CLIENT,
        )
      }
    }
  }
}
