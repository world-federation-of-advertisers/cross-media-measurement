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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import com.google.protobuf.Empty
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.BatchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.BatchDeleteUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.CreateUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.DeleteUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.GetUnlinkedClientAccountRequest
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamUnlinkedClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchCreateUnlinkedClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.BatchDeleteUnlinkedClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateUnlinkedClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteUnlinkedClientAccount

/** Google Cloud Spanner implementation of the internal UnlinkedClientAccounts service. */
class SpannerUnlinkedClientAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsCoroutineImplBase(coroutineContext) {
  override suspend fun createUnlinkedClientAccount(
    request: CreateUnlinkedClientAccountRequest
  ): UnlinkedClientAccount {
    if (!request.hasUnlinkedClientAccount()) {
      throw RequiredFieldNotSetException("unlinked_client_account")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.unlinkedClientAccount.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("unlinked_client_account.external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    validateReferenceId(
      "unlinked_client_account.client_account_reference_id",
      request.unlinkedClientAccount.clientAccountReferenceId,
    )

    return handleCreateExceptions {
      CreateUnlinkedClientAccount(request.unlinkedClientAccount).execute(client, idGenerator)
    }
  }

  override suspend fun batchCreateUnlinkedClientAccounts(
    request: BatchCreateUnlinkedClientAccountsRequest
  ): BatchCreateUnlinkedClientAccountsResponse {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") { fieldPath ->
          "Number of $fieldPath must be at most $MAX_BATCH_SIZE"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val referenceIds = mutableSetOf<String>()
    for ((index, subRequest) in request.requestsList.withIndex()) {
      if (!subRequest.hasUnlinkedClientAccount()) {
        throw RequiredFieldNotSetException("requests.$index.unlinked_client_account")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      val account = subRequest.unlinkedClientAccount
      if (
        account.externalDataProviderId != 0L &&
          account.externalDataProviderId != request.externalDataProviderId
      ) {
        throw InvalidFieldValueException(
            "requests.$index.unlinked_client_account.external_data_provider_id"
          ) { fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      validateReferenceId(
        "requests.$index.unlinked_client_account.client_account_reference_id",
        account.clientAccountReferenceId,
      )
      if (!referenceIds.add(account.clientAccountReferenceId)) {
        throw InvalidFieldValueException(
            "requests.$index.unlinked_client_account.client_account_reference_id"
          ) { fieldPath ->
            "Value of $fieldPath is a duplicate"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleCreateExceptions {
      BatchCreateUnlinkedClientAccounts(request).execute(client, idGenerator)
    }
  }

  override suspend fun getUnlinkedClientAccount(
    request: GetUnlinkedClientAccountRequest
  ): UnlinkedClientAccount {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.clientAccountReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException("client_account_reference_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val result =
      UnlinkedClientAccountReader()
        .readByDataProviderAndReferenceId(
          client.singleUse(),
          externalDataProviderId,
          request.clientAccountReferenceId,
        )
        ?: throw UnlinkedClientAccountNotFoundException(
            externalDataProviderId,
            request.clientAccountReferenceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)

    return result.unlinkedClientAccount
  }

  override suspend fun deleteUnlinkedClientAccount(
    request: DeleteUnlinkedClientAccountRequest
  ): UnlinkedClientAccount {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.clientAccountReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException("client_account_reference_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return handleDeleteExceptions {
      DeleteUnlinkedClientAccount(
          ExternalId(request.externalDataProviderId),
          request.clientAccountReferenceId,
        )
        .execute(client, idGenerator)
    }
  }

  override suspend fun batchDeleteUnlinkedClientAccounts(
    request: BatchDeleteUnlinkedClientAccountsRequest
  ): Empty {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") { fieldPath ->
          "Number of $fieldPath must be at most $MAX_BATCH_SIZE"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for ((index, subRequest) in request.requestsList.withIndex()) {
      if (
        subRequest.externalDataProviderId != 0L &&
          subRequest.externalDataProviderId != request.externalDataProviderId
      ) {
        throw InvalidFieldValueException("requests.$index.external_data_provider_id") { fieldPath ->
            "Value of $fieldPath differs from that of the parent request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.client_account_reference_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    return handleDeleteExceptions {
      BatchDeleteUnlinkedClientAccounts(request).execute(client, idGenerator)
    }
  }

  override suspend fun listUnlinkedClientAccounts(
    request: ListUnlinkedClientAccountsRequest
  ): ListUnlinkedClientAccountsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { "Page size cannot be less than 0" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after = if (request.hasPageToken()) request.pageToken.after else null

    val accountList =
      StreamUnlinkedClientAccounts(request.filter, pageSize + 1, after)
        .execute(client.singleUse())
        .toList()

    if (accountList.isEmpty()) {
      return ListUnlinkedClientAccountsResponse.getDefaultInstance()
    }

    return listUnlinkedClientAccountsResponse {
      for ((index, result) in accountList.withIndex()) {
        if (index == pageSize) {
          val lastAccount = unlinkedClientAccounts.last()
          nextPageToken = listUnlinkedClientAccountsPageToken {
            this.after =
              ListUnlinkedClientAccountsPageTokenKt.after {
                externalDataProviderId = lastAccount.externalDataProviderId
                clientAccountReferenceId = lastAccount.clientAccountReferenceId
              }
          }
        } else {
          unlinkedClientAccounts += result.unlinkedClientAccount
        }
      }
    }
  }

  private fun validateReferenceId(fieldName: String, referenceId: String) {
    if (referenceId.isEmpty()) {
      throw RequiredFieldNotSetException(fieldName)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (referenceId.length > MAX_REFERENCE_ID_LENGTH) {
      throw InvalidFieldValueException(fieldName) { fieldPath ->
          "Length of $fieldPath must be at most $MAX_REFERENCE_ID_LENGTH characters"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private suspend fun <T> handleCreateExceptions(block: suspend () -> T): T {
    try {
      return block()
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: UnlinkedClientAccountAlreadyExistsException) {
      throw e.asStatusRuntimeException(
        Status.Code.ALREADY_EXISTS,
        "UnlinkedClientAccount with this reference ID already exists for DataProvider.",
      )
    } catch (e: ClientAccountAlreadyExistsException) {
      throw e.asStatusRuntimeException(
        Status.Code.ALREADY_EXISTS,
        "A ClientAccount with this reference ID already exists for DataProvider.",
      )
    }
  }

  private suspend fun <T> handleDeleteExceptions(block: suspend () -> T): T {
    try {
      return block()
    } catch (e: UnlinkedClientAccountNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  companion object {
    private const val MAX_BATCH_SIZE = 1000
    private const val MAX_PAGE_SIZE = 1000
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_REFERENCE_ID_LENGTH = 36
  }
}
