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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Options
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readUnlinkedClientAccounts
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.replaceUnlinkedClientAccounts
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.ListUnlinkedClientAccountsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.edpaggregator.ListUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.edpaggregator.ReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.edpaggregator.ReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccount
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.edpaggregator.listUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.edpaggregator.replaceUnlinkedClientAccountsResponse

/**
 * Google Cloud Spanner implementation of the internal `UnlinkedClientAccountsService`.
 *
 * Persists, per DataProvider, the set of advertiser client-account reference IDs that
 * EventGroupSync could not resolve to any MeasurementConsumer ("unlinked").
 * `ReplaceUnlinkedClientAccounts` reconciles the stored set against an incoming full set in a
 * single read-write transaction.
 */
class SpannerUnlinkedClientAccountsService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsServiceCoroutineImplBase(coroutineContext) {

  override suspend fun replaceUnlinkedClientAccounts(
    request: ReplaceUnlinkedClientAccountsRequest
  ): ReplaceUnlinkedClientAccountsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val referenceIdSet = mutableSetOf<String>()
    request.unlinkedClientAccountsList.forEachIndexed { index, account ->
      if (account.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!referenceIdSet.add(account.clientAccountReferenceId)) {
        val referenceId = account.clientAccountReferenceId
        throw InvalidFieldValueException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          ) {
            "duplicate client_account_reference_id \"$referenceId\" in the request"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=replaceUnlinkedClientAccounts"))

    val results: List<UnlinkedClientAccount> =
      transactionRunner.run { txn ->
        txn.replaceUnlinkedClientAccounts(
          request.dataProviderResourceId,
          request.unlinkedClientAccountsList,
        )
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return replaceUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        results.map { result ->
          if (result.hasFirstObservedTime()) {
            result
          } else {
            result.copy { firstObservedTime = commitTimestamp }
          }
        }
    }
  }

  override suspend fun listUnlinkedClientAccounts(
    request: ListUnlinkedClientAccountsRequest
  ): ListUnlinkedClientAccountsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val unlinkedClientAccountsFlow: Flow<UnlinkedClientAccount> =
        txn.readUnlinkedClientAccounts(request.dataProviderResourceId, pageSize + 1, after)
      return listUnlinkedClientAccountsResponse {
        unlinkedClientAccountsFlow.collectIndexed { index, unlinkedClientAccount ->
          if (index == pageSize) {
            nextPageToken = listUnlinkedClientAccountsPageToken {
              this.after =
                ListUnlinkedClientAccountsPageTokenKt.after {
                  clientAccountReferenceId =
                    this@listUnlinkedClientAccountsResponse.unlinkedClientAccounts
                      .last()
                      .clientAccountReferenceId
                }
            }
          } else {
            this.unlinkedClientAccounts += unlinkedClientAccount
          }
        }
      }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
  }
}
