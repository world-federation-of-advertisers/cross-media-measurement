/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.ReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountKey
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.replaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.unlinkedClientAccount
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroupKt as InternalEventGroupKt
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount as InternalUnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineStub as InternalUnlinkedClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.replaceUnlinkedClientAccountsRequest as internalReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount as internalUnlinkedClientAccount

/**
 * Public API implementation for the UnlinkedClientAccounts service.
 *
 * Translates between the public API resource names (e.g.,
 * `dataProviders/{dp}/unlinkedClientAccounts/{uca}`) and the internal API external IDs.
 * `ReplaceUnlinkedClientAccounts` is a full-set reconcile scoped to a single DataProvider.
 *
 * @param internalUnlinkedClientAccountsStub stub for the internal UnlinkedClientAccounts service
 * @param coroutineContext coroutine context for the service
 */
class UnlinkedClientAccountsService(
  private val internalUnlinkedClientAccountsStub: InternalUnlinkedClientAccountsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsCoroutineImplBase(coroutineContext) {

  private fun permissionDeniedStatus(name: String): Status =
    Status.PERMISSION_DENIED.withDescription(
      "Permission denied on resource $name (or it might not exist)"
    )

  override suspend fun replaceUnlinkedClientAccounts(
    request: ReplaceUnlinkedClientAccountsRequest
  ): ReplaceUnlinkedClientAccountsResponse {
    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus("${request.parent}/unlinkedClientAccounts").asRuntimeException()
    }

    grpcRequire(request.unlinkedClientAccountsList.size <= MAX_BATCH_SIZE) {
      "unlinked_client_accounts count exceeds maximum batch size of $MAX_BATCH_SIZE"
    }

    val referenceIds = mutableSetOf<String>()
    request.unlinkedClientAccountsList.forEachIndexed { index, account ->
      grpcRequire(account.clientAccountReferenceId.isNotEmpty()) {
        "unlinked_client_accounts.$index.client_account_reference_id must be specified"
      }
      grpcRequire(referenceIds.add(account.clientAccountReferenceId)) {
        "unlinked_client_accounts.$index.client_account_reference_id is a duplicate"
      }
    }

    val internalRequest = internalReplaceUnlinkedClientAccountsRequest {
      externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
      unlinkedClientAccounts += request.unlinkedClientAccountsList.map { it.toInternal() }
    }

    val internalResponse =
      try {
        internalUnlinkedClientAccountsStub.replaceUnlinkedClientAccounts(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return replaceUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        internalResponse.unlinkedClientAccountsList.map { it.toUnlinkedClientAccount() }
    }
  }

  companion object {
    private const val MAX_BATCH_SIZE = 1000
  }
}

/** Converts a public [UnlinkedClientAccount] to an internal [InternalUnlinkedClientAccount]. */
private fun UnlinkedClientAccount.toInternal(): InternalUnlinkedClientAccount {
  val source = this
  return internalUnlinkedClientAccount {
    clientAccountReferenceId = source.clientAccountReferenceId
    brands += source.brandsList
    when (source.observedEventGroupCase) {
      UnlinkedClientAccount.ObservedEventGroupCase.EVENT_GROUP_REFERENCE_ID ->
        eventGroupReferenceId = source.eventGroupReferenceId
      UnlinkedClientAccount.ObservedEventGroupCase.ENTITY_KEY ->
        entityKey =
          InternalEventGroupKt.entityKey {
            entityType = source.entityKey.entityType
            entityId = source.entityKey.entityId
          }
      UnlinkedClientAccount.ObservedEventGroupCase.OBSERVEDEVENTGROUP_NOT_SET -> {}
    }
  }
}

/** Converts an internal [InternalUnlinkedClientAccount] to a public [UnlinkedClientAccount]. */
private fun InternalUnlinkedClientAccount.toUnlinkedClientAccount(): UnlinkedClientAccount {
  val source = this
  return unlinkedClientAccount {
    name =
      UnlinkedClientAccountKey(
          externalIdToApiId(source.externalDataProviderId),
          source.clientAccountReferenceId,
        )
        .toName()
    clientAccountReferenceId = source.clientAccountReferenceId
    brands += source.brandsList
    when (source.observedEventGroupCase) {
      InternalUnlinkedClientAccount.ObservedEventGroupCase.EVENT_GROUP_REFERENCE_ID ->
        eventGroupReferenceId = source.eventGroupReferenceId
      InternalUnlinkedClientAccount.ObservedEventGroupCase.ENTITY_KEY ->
        entityKey =
          EventGroupKt.entityKey {
            entityType = source.entityKey.entityType
            entityId = source.entityKey.entityId
          }
      InternalUnlinkedClientAccount.ObservedEventGroupCase.OBSERVEDEVENTGROUP_NOT_SET -> {}
    }
    firstObservedTime = source.firstObservedTime
  }
}
