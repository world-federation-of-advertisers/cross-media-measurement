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
import kotlin.math.min
import org.wfanet.measurement.api.v2alpha.BatchCreateClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.BatchCreateClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.BatchDeleteClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.ClientAccount
import org.wfanet.measurement.api.v2alpha.ClientAccountKey
import org.wfanet.measurement.api.v2alpha.ClientAccountsGrpcKt.ClientAccountsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CreateClientAccountRequest
import org.wfanet.measurement.api.v2alpha.DataProviderClientAccountKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DeleteClientAccountRequest
import org.wfanet.measurement.api.v2alpha.GetClientAccountRequest
import org.wfanet.measurement.api.v2alpha.ListClientAccountsPageToken
import org.wfanet.measurement.api.v2alpha.ListClientAccountsPageTokenKt.parentKey
import org.wfanet.measurement.api.v2alpha.ListClientAccountsRequest
import org.wfanet.measurement.api.v2alpha.ListClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerClientAccountKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.batchCreateClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.clientAccount
import org.wfanet.measurement.api.v2alpha.listClientAccountsPageToken
import org.wfanet.measurement.api.v2alpha.listClientAccountsResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ClientAccount as InternalClientAccount
import org.wfanet.measurement.internal.kingdom.ClientAccountsGrpcKt.ClientAccountsCoroutineStub as InternalClientAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ListClientAccountsRequest as InternalListClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.ListClientAccountsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCreateClientAccountsRequest as internalBatchCreateClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.batchDeleteClientAccountsRequest as internalBatchDeleteClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.clientAccount as internalClientAccount
import org.wfanet.measurement.internal.kingdom.createClientAccountRequest as internalCreateClientAccountRequest
import org.wfanet.measurement.internal.kingdom.deleteClientAccountRequest as internalDeleteClientAccountRequest
import org.wfanet.measurement.internal.kingdom.getClientAccountRequest as internalGetClientAccountRequest
import org.wfanet.measurement.internal.kingdom.listClientAccountsRequest as internalListClientAccountsRequest

/**
 * Public API implementation for ClientAccounts service.
 *
 * Translates between the public API resource names (e.g.,
 * `measurementConsumers/{mc}/clientAccounts/{ca}`) and the internal API external IDs.
 *
 * @param internalClientAccountsStub stub for the internal ClientAccounts service
 * @param coroutineContext coroutine context for the service
 */
class ClientAccountsService(
  private val internalClientAccountsStub: InternalClientAccountsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ClientAccountsCoroutineImplBase(coroutineContext) {

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

  override suspend fun createClientAccount(request: CreateClientAccountRequest): ClientAccount {
    fun permissionDeniedStatus() =
      Permission.CREATE.deniedStatus("${request.parent}/clientAccounts")

    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.hasClientAccount()) { "client_account must be specified" }
    grpcRequire(request.clientAccount.dataProvider.isNotEmpty()) {
      "client_account.data_provider must be specified"
    }
    grpcRequire(request.clientAccount.clientAccountReferenceId.isNotEmpty()) {
      "client_account.client_account_reference_id must be specified"
    }
    grpcRequire(request.clientAccount.clientAccountReferenceId.length <= 36) {
      "client_account.client_account_reference_id must be <= 36 characters"
    }

    val dataProviderKey =
      grpcRequireNotNull(DataProviderKey.fromName(request.clientAccount.dataProvider)) {
        "client_account.data_provider is invalid"
      }

    val internalRequest = internalCreateClientAccountRequest {
      clientAccount = internalClientAccount {
        externalMeasurementConsumerId = apiIdToExternalId(parentKey.measurementConsumerId)
        externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
        clientAccountReferenceId = request.clientAccount.clientAccountReferenceId
      }
    }

    return try {
      internalClientAccountsStub.createClientAccount(internalRequest).toClientAccount()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.ALREADY_EXISTS -> Status.ALREADY_EXISTS
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun batchCreateClientAccounts(
    request: BatchCreateClientAccountsRequest
  ): BatchCreateClientAccountsResponse {
    fun permissionDeniedStatus() =
      Permission.CREATE.deniedStatus("${request.parent}/clientAccounts")

    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.requestsCount <= MAX_BATCH_SIZE) {
      "requests count exceeds maximum batch size of $MAX_BATCH_SIZE"
    }

    for (subRequest in request.requestsList) {
      if (subRequest.parent.isNotEmpty() && subRequest.parent != request.parent) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "Parent in child request does not match batch parent"
          )
          .asRuntimeException()
      }

      grpcRequire(subRequest.hasClientAccount()) {
        "client_account must be specified in child request"
      }
      grpcRequire(subRequest.clientAccount.dataProvider.isNotEmpty()) {
        "client_account.data_provider must be specified"
      }
      grpcRequire(subRequest.clientAccount.clientAccountReferenceId.isNotEmpty()) {
        "client_account.client_account_reference_id must be specified"
      }
      grpcRequire(subRequest.clientAccount.clientAccountReferenceId.length <= 36) {
        "client_account.client_account_reference_id must be <= 36 characters"
      }
    }

    val internalRequest = internalBatchCreateClientAccountsRequest {
      externalMeasurementConsumerId = apiIdToExternalId(parentKey.measurementConsumerId)
      for (subRequest in request.requestsList) {
        val dataProviderKey =
          grpcRequireNotNull(DataProviderKey.fromName(subRequest.clientAccount.dataProvider)) {
            "client_account.data_provider is invalid"
          }

        requests +=
          org.wfanet.measurement.internal.kingdom.createClientAccountRequest {
            clientAccount = internalClientAccount {
              externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
              clientAccountReferenceId = subRequest.clientAccount.clientAccountReferenceId
            }
          }
      }
    }

    val internalResponse =
      try {
        internalClientAccountsStub.batchCreateClientAccounts(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND
          Status.Code.ALREADY_EXISTS -> Status.ALREADY_EXISTS
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    return batchCreateClientAccountsResponse {
      clientAccounts += internalResponse.clientAccountsList.map { it.toClientAccount() }
    }
  }

  override suspend fun getClientAccount(request: GetClientAccountRequest): ClientAccount {
    fun permissionDeniedStatus() = Permission.GET.deniedStatus(request.name)

    val key: ClientAccountKey =
      grpcRequireNotNull(ClientAccountKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext

    val internalRequest =
      when (key) {
        is MeasurementConsumerClientAccountKey -> {
          if (
            principal !is MeasurementConsumerPrincipal || principal.resourceKey != key.parentKey
          ) {
            throw permissionDeniedStatus().asRuntimeException()
          }
          internalGetClientAccountRequest {
            externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
            externalClientAccountId = apiIdToExternalId(key.clientAccountId)
          }
        }
        is DataProviderClientAccountKey -> {
          if (principal !is DataProviderPrincipal || principal.resourceKey != key.parentKey) {
            throw permissionDeniedStatus().asRuntimeException()
          }
          internalGetClientAccountRequest {
            externalDataProviderId = apiIdToExternalId(key.dataProviderId)
            externalClientAccountId = apiIdToExternalId(key.clientAccountId)
          }
        }
        else -> error("Unexpected resource key type: $key")
      }

    return try {
      internalClientAccountsStub.getClientAccount(internalRequest).toClientAccount()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listClientAccounts(
    request: ListClientAccountsRequest
  ): ListClientAccountsResponse {
    fun permissionDeniedStatus() = Permission.LIST.deniedStatus("${request.parent}/clientAccounts")

    grpcRequire(request.pageSize >= 0) { "Page size cannot be less than 0" }

    val parentKey: ResourceKey =
      MeasurementConsumerKey.fromName(request.parent)
        ?: DataProviderKey.fromName(request.parent)
        ?: throw Status.INVALID_ARGUMENT.withDescription("parent unspecified or invalid")
          .asRuntimeException()

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (parentKey != principal.resourceKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val pageToken: ListClientAccountsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        ListClientAccountsPageToken.parseFrom(request.pageToken.base64UrlDecode())
      }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val internalRequest =
      buildInternalListClientAccountsRequest(request, parentKey, pageSize, pageToken)

    val internalResponse =
      try {
        internalClientAccountsStub.listClientAccounts(internalRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.toExternalStatusRuntimeException(e)
      }

    if (internalResponse.clientAccountsList.isEmpty()) {
      return ListClientAccountsResponse.getDefaultInstance()
    }

    return listClientAccountsResponse {
      clientAccounts +=
        internalResponse.clientAccountsList
          .subList(0, min(internalResponse.clientAccountsCount, pageSize))
          .map(InternalClientAccount::toClientAccount)
      if (internalResponse.hasNextPageToken()) {
        nextPageToken =
          buildNextPageToken(
              parentKey,
              request.filter.clientAccountReferenceId,
              internalResponse.nextPageToken,
            )
            .toByteString()
            .base64UrlEncode()
      }
    }
  }

  override suspend fun deleteClientAccount(request: DeleteClientAccountRequest): Empty {
    fun permissionDeniedStatus() = Permission.DELETE.deniedStatus(request.name)

    val key: ClientAccountKey =
      grpcRequireNotNull(ClientAccountKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext

    // Only MeasurementConsumer can delete
    if (
      principal !is MeasurementConsumerPrincipal ||
        key !is MeasurementConsumerClientAccountKey ||
        principal.resourceKey != key.parentKey
    ) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    val internalRequest = internalDeleteClientAccountRequest {
      externalMeasurementConsumerId = apiIdToExternalId(key.measurementConsumerId)
      externalClientAccountId = apiIdToExternalId(key.clientAccountId)
    }

    try {
      internalClientAccountsStub.deleteClientAccount(internalRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun batchDeleteClientAccounts(request: BatchDeleteClientAccountsRequest): Empty {
    fun permissionDeniedStatus() =
      Permission.DELETE.deniedStatus("${request.parent}/clientAccounts")

    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val principal: MeasurementPrincipal = principalFromCurrentContext
    if (principal !is MeasurementConsumerPrincipal || principal.resourceKey != parentKey) {
      throw permissionDeniedStatus().asRuntimeException()
    }

    grpcRequire(request.namesCount <= MAX_BATCH_SIZE) {
      "names count exceeds maximum batch size of $MAX_BATCH_SIZE"
    }

    for (name in request.namesList) {
      val key: ClientAccountKey =
        grpcRequireNotNull(ClientAccountKey.fromName(name)) { "Resource name $name is invalid" }

      if (key !is MeasurementConsumerClientAccountKey || key.parentKey != parentKey) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "Resource $name does not match parent ${request.parent}"
          )
          .asRuntimeException()
      }
    }

    val internalRequest = internalBatchDeleteClientAccountsRequest {
      externalMeasurementConsumerId = apiIdToExternalId(parentKey.measurementConsumerId)
      for (name in request.namesList) {
        val key = MeasurementConsumerClientAccountKey.fromName(name)!!
        requests += internalDeleteClientAccountRequest {
          externalClientAccountId = apiIdToExternalId(key.clientAccountId)
        }
      }
    }

    try {
      internalClientAccountsStub.batchDeleteClientAccounts(internalRequest)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> permissionDeniedStatus()
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }

    return Empty.getDefaultInstance()
  }

  private fun buildInternalListClientAccountsRequest(
    request: ListClientAccountsRequest,
    parentKey: ResourceKey,
    pageSize: Int,
    pageToken: ListClientAccountsPageToken?,
  ): InternalListClientAccountsRequest {
    return internalListClientAccountsRequest {
      filter =
        ListClientAccountsRequestKt.filter {
          when (parentKey) {
            is MeasurementConsumerKey ->
              externalMeasurementConsumerId = apiIdToExternalId(parentKey.measurementConsumerId)
            is DataProviderKey ->
              externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
          }

          if (request.filter.measurementConsumer.isNotEmpty()) {
            val mcKey =
              grpcRequireNotNull(
                MeasurementConsumerKey.fromName(request.filter.measurementConsumer)
              ) {
                "Invalid resource name in filter.measurement_consumer"
              }
            externalMeasurementConsumerId = apiIdToExternalId(mcKey.measurementConsumerId)
          }
          if (request.filter.dataProvider.isNotEmpty()) {
            val dpKey =
              grpcRequireNotNull(DataProviderKey.fromName(request.filter.dataProvider)) {
                "Invalid resource name in filter.data_provider"
              }
            externalDataProviderId = apiIdToExternalId(dpKey.dataProviderId)
          }
          if (request.filter.clientAccountReferenceId.isNotEmpty()) {
            clientAccountReferenceId = request.filter.clientAccountReferenceId
          }
        }
      this.pageSize = pageSize

      if (pageToken != null) {
        // Validate page token matches filter
        val tokenParentKey = pageToken.parentKey
        val isValidToken =
          when {
            tokenParentKey.hasExternalMeasurementConsumerId() ->
              parentKey is MeasurementConsumerKey &&
                tokenParentKey.externalMeasurementConsumerId ==
                  apiIdToExternalId(parentKey.measurementConsumerId)
            tokenParentKey.hasExternalDataProviderId() ->
              parentKey is DataProviderKey &&
                tokenParentKey.externalDataProviderId == apiIdToExternalId(parentKey.dataProviderId)
            else -> false
          }

        grpcRequire(
          isValidToken &&
            pageToken.clientAccountReferenceId == request.filter.clientAccountReferenceId
        ) {
          "Arguments other than page_size must remain the same for subsequent page requests"
        }

        // Deserialize the internal page token
        this.pageToken =
          org.wfanet.measurement.internal.kingdom.ListClientAccountsPageToken.parseFrom(
            pageToken.internalPageToken
          )
      }
    }
  }

  private fun buildNextPageToken(
    parentKey: ResourceKey,
    clientAccountReferenceId: String,
    internalPageToken: org.wfanet.measurement.internal.kingdom.ListClientAccountsPageToken,
  ): ListClientAccountsPageToken {
    return listClientAccountsPageToken {
      this.parentKey = parentKey {
        when (parentKey) {
          is MeasurementConsumerKey ->
            externalMeasurementConsumerId = apiIdToExternalId(parentKey.measurementConsumerId)
          is DataProviderKey -> externalDataProviderId = apiIdToExternalId(parentKey.dataProviderId)
        }
      }
      this.internalPageToken = internalPageToken.toByteString()
      this.clientAccountReferenceId = clientAccountReferenceId
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
    private const val MAX_BATCH_SIZE = 1000
  }
}

/** Converts an internal [InternalClientAccount] to a public [ClientAccount]. */
private fun InternalClientAccount.toClientAccount(): ClientAccount {
  val clientAccountApiId = externalIdToApiId(externalClientAccountId)
  val measurementConsumerApiId = externalIdToApiId(externalMeasurementConsumerId)
  val dataProviderApiId = externalIdToApiId(externalDataProviderId)

  return clientAccount {
    name =
      MeasurementConsumerClientAccountKey(measurementConsumerApiId, clientAccountApiId).toName()
    dataProvider = DataProviderKey(dataProviderApiId).toName()
    clientAccountReferenceId = this@toClientAccount.clientAccountReferenceId
  }
}
