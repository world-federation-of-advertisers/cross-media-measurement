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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.kingdom.BatchCreateUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.batchCreateUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader

/**
 * Creates UnlinkedClientAccounts in a batch atomically within a single transaction.
 *
 * Throws a subclass of
 * [org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException] on
 * [execute].
 *
 * @throws [DataProviderNotFoundException] when the DataProvider is not found
 * @throws [UnlinkedClientAccountAlreadyExistsException] when an UnlinkedClientAccount with the same
 *   reference ID already exists for the DataProvider
 */
class BatchCreateUnlinkedClientAccounts(
  private val request: BatchCreateUnlinkedClientAccountsRequest
) :
  SpannerWriter<
    BatchCreateUnlinkedClientAccountsResponse,
    BatchCreateUnlinkedClientAccountsResponse,
  >() {

  override suspend fun TransactionScope.runTransaction():
    BatchCreateUnlinkedClientAccountsResponse {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = InternalId(dataProviderResult.dataProviderId)

    for (subRequest in request.requestsList) {
      val existing =
        UnlinkedClientAccountReader()
          .readByDataProviderAndReferenceId(
            transactionContext,
            externalDataProviderId,
            subRequest.unlinkedClientAccount.clientAccountReferenceId,
          )
      if (existing != null) {
        throw UnlinkedClientAccountAlreadyExistsException(
          externalDataProviderId,
          subRequest.unlinkedClientAccount.clientAccountReferenceId,
        )
      }

      val existingClientAccount =
        ClientAccountReader()
          .readByDataProviderAndReferenceId(
            transactionContext,
            externalDataProviderId,
            subRequest.unlinkedClientAccount.clientAccountReferenceId,
          )
      if (existingClientAccount != null) {
        throw ClientAccountAlreadyExistsException(
          externalDataProviderId,
          subRequest.unlinkedClientAccount.clientAccountReferenceId,
        )
      }
    }

    val createdAccounts =
      request.requestsList.map { subRequest ->
        createUnlinkedClientAccount(
          dataProviderId,
          externalDataProviderId,
          subRequest.unlinkedClientAccount,
        )
      }

    return batchCreateUnlinkedClientAccountsResponse { unlinkedClientAccounts += createdAccounts }
  }

  private fun TransactionScope.createUnlinkedClientAccount(
    dataProviderId: InternalId,
    externalDataProviderId: ExternalId,
    account: UnlinkedClientAccount,
  ): UnlinkedClientAccount {
    transactionContext.bufferInsertMutation("UnlinkedClientAccounts") {
      set("DataProviderId").to(dataProviderId.value)
      set("ClientAccountReferenceId").to(account.clientAccountReferenceId)
      if (account.hasEntityMetadata()) {
        set("EntityMetadata").to(account.entityMetadata)
      }
      setObservedEventGroupColumns(account)
      set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    }

    return account.copy { this.externalDataProviderId = externalDataProviderId.value }
  }

  override fun ResultScope<BatchCreateUnlinkedClientAccountsResponse>.buildResult():
    BatchCreateUnlinkedClientAccountsResponse {
    val commitTime = commitTimestamp.toProto()
    return batchCreateUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        checkNotNull(transactionResult).unlinkedClientAccountsList.map {
          it.copy { createTime = commitTime }
        }
    }
  }
}
