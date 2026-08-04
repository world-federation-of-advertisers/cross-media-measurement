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

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsPageTokenKt
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.ReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.ReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.listUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.replaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamUnlinkedClientAccounts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceUnlinkedClientAccounts

/** Google Cloud Spanner implementation of the internal UnlinkedClientAccounts service. */
class SpannerUnlinkedClientAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsCoroutineImplBase(coroutineContext) {
  override suspend fun replaceUnlinkedClientAccounts(
    request: ReplaceUnlinkedClientAccountsRequest
  ): ReplaceUnlinkedClientAccountsResponse {
    if (request.externalDataProviderId == 0L) {
      throw RequiredFieldNotSetException("external_data_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.unlinkedClientAccountsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("unlinked_client_accounts") { fieldPath ->
          "Number of $fieldPath must be at most $MAX_BATCH_SIZE"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val referenceIds = mutableSetOf<String>()
    for ((index, account) in request.unlinkedClientAccountsList.withIndex()) {
      if (account.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (account.clientAccountReferenceId.length > 36) {
        throw InvalidFieldValueException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          ) { fieldPath ->
            "Length of $fieldPath must be at most 36 characters"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!referenceIds.add(account.clientAccountReferenceId)) {
        throw InvalidFieldValueException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          ) { fieldPath ->
            "Value of $fieldPath is a duplicate"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val reconciled =
      try {
        ReplaceUnlinkedClientAccounts(
            ExternalId(request.externalDataProviderId),
            request.unlinkedClientAccountsList,
          )
          .execute(client, idGenerator)
      } catch (e: DataProviderNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return replaceUnlinkedClientAccountsResponse { unlinkedClientAccounts += reconciled }
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

  companion object {
    private const val MAX_BATCH_SIZE = 1000
    private const val MAX_PAGE_SIZE = 1000
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
