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

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.BatchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.BatchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.BatchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.CreateUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.GetUnlinkedClientAccountRequest
import org.wfanet.measurement.api.v2alpha.ListUnlinkedClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.ListUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccount
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountKey
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.batchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.listUnlinkedClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.unlinkedClientAccount
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.EventGroupKt as InternalEventGroupKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsPageToken as InternalListUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequestKt
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount as InternalUnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineStub as InternalUnlinkedClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.batchCreateUnlinkedClientAccountsRequest as internalBatchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.batchDeleteUnlinkedClientAccountsRequest as internalBatchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.createUnlinkedClientAccountRequest as internalCreateUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteUnlinkedClientAccountRequest as internalDeleteUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getUnlinkedClientAccountRequest as internalGetUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsRequest as internalListUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount as internalUnlinkedClientAccount

/**
 * Public API implementation for the UnlinkedClientAccounts service.
 *
 * Translates between the public API resource names (e.g.,
 * `dataProviders/{dp}/unlinkedClientAccounts/{uca}`) and the internal API external IDs. Each
 * `UnlinkedClientAccount` is scoped to a single parent `DataProvider`, and the resource ID is the
 * `client_account_reference_id`.
 *
 * @param internalUnlinkedClientAccountsStub stub for the internal UnlinkedClientAccounts service
 * @param coroutineContext coroutine context for the service
 */
class UnlinkedClientAccountsService(
  private val internalUnlinkedClientAccountsStub: InternalUnlinkedClientAccountsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    GET,
    LIST,
    CREATE,
    DELETE;

    fun deniedStatus(name: String): Status =
      Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)"
      )
  }

  override suspend fun createUnlinkedClientAccount(
    request: CreateUnlinkedClientAccountRequest
  ): UnlinkedClientAccount {
    fun permissionDeniedStatus() =
      Permission.CREATE.deniedStatus("${request.parent}/unlinkedClientAccounts")

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.hasUnlinkedClientAccount()) { "unlinked_client_account must be specified" }
    validateReferenceId(request.unlinkedClientAccount.clientAccountReferenceId)

    val externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
    val internalRequest = internalCreateUnlinkedClientAccountRequest {
      unlinkedClientAccount = request.unlinkedClientAccount.toInternal(externalDataProviderId)
    }

    return try {
      internalUnlinkedClientAccountsStub
        .createUnlinkedClientAccount(internalRequest)
        .toUnlinkedClientAccount()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.ALREADY_EXISTS -> Status.ALREADY_EXISTS
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun batchCreateUnlinkedClientAccounts(
    request: BatchCreateUnlinkedClientAccountsRequest
  ): BatchCreateUnlinkedClientAccountsResponse {
    fun permissionDeniedStatus() =
      Permission.CREATE.deniedStatus("${request.parent}/unlinkedClientAccounts")

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.requestsCount <= MAX_BATCH_SIZE) {
      "requests count exceeds maximum batch size of $MAX_BATCH_SIZE"
    }

    val referenceIds = mutableSetOf<String>()
    for (subRequest in request.requestsList) {
      if (subRequest.parent.isNotEmpty() && subRequest.parent != request.parent) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "Parent in child request does not match batch parent"
          )
          .asRuntimeException()
      }
      grpcRequire(subRequest.hasUnlinkedClientAccount()) {
        "unlinked_client_account must be specified in child request"
      }
      val referenceId = subRequest.unlinkedClientAccount.clientAccountReferenceId
      validateReferenceId(referenceId)
      grpcRequire(referenceIds.add(referenceId)) {
        "client_account_reference_id $referenceId is a duplicate"
      }
    }

    val externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
    val internalRequest = internalBatchCreateUnlinkedClientAccountsRequest {
      this.externalDataProviderId = externalDataProviderId
      for (subRequest in request.requestsList) {
        requests += internalCreateUnlinkedClientAccountRequest {
          unlinkedClientAccount =
            subRequest.unlinkedClientAccount.toInternal(externalDataProviderId)
        }
      }
    }

    val internalResponse =
      try {
        internalUnlinkedClientAccountsStub.batchCreateUnlinkedClientAccounts(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.ALREADY_EXISTS -> Status.ALREADY_EXISTS
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return batchCreateUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        internalResponse.unlinkedClientAccountsList.map { it.toUnlinkedClientAccount() }
    }
  }

  override suspend fun getUnlinkedClientAccount(
    request: GetUnlinkedClientAccountRequest
  ): UnlinkedClientAccount {
    fun permissionDeniedStatus() = Permission.GET.deniedStatus(request.name)

    val key =
      grpcRequireNotNull(UnlinkedClientAccountKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != key.parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val internalRequest = internalGetUnlinkedClientAccountRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      clientAccountReferenceId = key.unlinkedClientAccountId
    }

    return try {
      internalUnlinkedClientAccountsStub
        .getUnlinkedClientAccount(internalRequest)
        .toUnlinkedClientAccount()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listUnlinkedClientAccounts(
    request: ListUnlinkedClientAccountsRequest
  ): ListUnlinkedClientAccountsResponse {
    fun permissionDeniedStatus() =
      Permission.LIST.deniedStatus("${request.parent}/unlinkedClientAccounts")

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)

    val internalPageToken: InternalListUnlinkedClientAccountsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListUnlinkedClientAccountsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: Exception) {
          throw Status.INVALID_ARGUMENT.withDescription("page_token is malformed")
            .withCause(e)
            .asRuntimeException()
        }
      }

    if (internalPageToken != null) {
      grpcRequire(internalPageToken.after.externalDataProviderId == externalDataProviderId) {
        "Arguments other than page_size must remain the same for subsequent page requests"
      }
    }

    val internalRequest = internalListUnlinkedClientAccountsRequest {
      filter =
        ListUnlinkedClientAccountsRequestKt.filter {
          this.externalDataProviderId = externalDataProviderId
        }
      pageSize =
        if (request.pageSize == 0) DEFAULT_PAGE_SIZE
        else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      if (internalPageToken != null) {
        pageToken = internalPageToken
      }
    }

    val internalResponse =
      try {
        internalUnlinkedClientAccountsStub.listUnlinkedClientAccounts(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (internalResponse.unlinkedClientAccountsList.isEmpty()) {
      return ListUnlinkedClientAccountsResponse.getDefaultInstance()
    }

    return listUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        internalResponse.unlinkedClientAccountsList.map(
          InternalUnlinkedClientAccount::toUnlinkedClientAccount
        )
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun deleteUnlinkedClientAccount(
    request: DeleteUnlinkedClientAccountRequest
  ): Empty {
    fun permissionDeniedStatus() = Permission.DELETE.deniedStatus(request.name)

    val key =
      grpcRequireNotNull(UnlinkedClientAccountKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != key.parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val internalRequest = internalDeleteUnlinkedClientAccountRequest {
      externalDataProviderId = apiIdToExternalId(key.dataProviderId)
      clientAccountReferenceId = key.unlinkedClientAccountId
    }

    try {
      internalUnlinkedClientAccountsStub.deleteUnlinkedClientAccount(internalRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun batchDeleteUnlinkedClientAccounts(
    request: BatchDeleteUnlinkedClientAccountsRequest
  ): Empty {
    fun permissionDeniedStatus() =
      Permission.DELETE.deniedStatus("${request.parent}/unlinkedClientAccounts")

    val parentKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is DataProviderPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.namesCount <= MAX_BATCH_SIZE) {
      "names count exceeds maximum batch size of $MAX_BATCH_SIZE"
    }

    val referenceIds = mutableSetOf<String>()
    val keys =
      request.namesList.map { name ->
        val key =
          grpcRequireNotNull(UnlinkedClientAccountKey.fromName(name)) {
            "Resource name $name is invalid"
          }
        if (key.parentKey != parentKey) {
          throw Status.INVALID_ARGUMENT.withDescription(
              "Resource $name does not match parent ${request.parent}"
            )
            .asRuntimeException()
        }
        grpcRequire(referenceIds.add(key.unlinkedClientAccountId)) {
          "Resource name $name is a duplicate"
        }
        key
      }

    val internalRequest = internalBatchDeleteUnlinkedClientAccountsRequest {
      externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
      for (key in keys) {
        requests += internalDeleteUnlinkedClientAccountRequest {
          externalDataProviderId = apiIdToExternalId(key.dataProviderId)
          clientAccountReferenceId = key.unlinkedClientAccountId
        }
      }
    }

    try {
      internalUnlinkedClientAccountsStub.batchDeleteUnlinkedClientAccounts(internalRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }

  private fun validateReferenceId(referenceId: String) {
    grpcRequire(referenceId.isNotEmpty()) {
      "unlinked_client_account.client_account_reference_id must be specified"
    }
    grpcRequire(referenceId.length <= MAX_REFERENCE_ID_LENGTH) {
      "unlinked_client_account.client_account_reference_id must be <= $MAX_REFERENCE_ID_LENGTH characters"
    }
    grpcRequire(RESOURCE_ID_REGEX.matches(referenceId)) {
      "unlinked_client_account.client_account_reference_id must be URL-safe"
    }
  }

  companion object {
    private const val MAX_BATCH_SIZE = 1000
    private const val MAX_REFERENCE_ID_LENGTH = 36
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
    // Unreserved characters per RFC 3986; URL-encoding or -decoding is a no-op.
    private val RESOURCE_ID_REGEX = Regex("^[a-zA-Z0-9._~-]+$")
  }
}

/** Converts a public [UnlinkedClientAccount] to an internal [InternalUnlinkedClientAccount]. */
private fun UnlinkedClientAccount.toInternal(
  externalDataProviderId: Long
): InternalUnlinkedClientAccount {
  val source = this
  return internalUnlinkedClientAccount {
    this.externalDataProviderId = externalDataProviderId
    clientAccountReferenceId = source.clientAccountReferenceId
    if (source.hasEntityMetadata()) {
      entityMetadata = source.entityMetadata
    }
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
    if (source.hasEntityMetadata()) {
      entityMetadata = source.entityMetadata
    }
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
    if (source.hasCreateTime()) {
      createTime = source.createTime
    }
  }
}
