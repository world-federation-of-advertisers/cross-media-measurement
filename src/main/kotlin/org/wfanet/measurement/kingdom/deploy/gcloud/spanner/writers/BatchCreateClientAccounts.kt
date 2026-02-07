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
import org.wfanet.measurement.common.generateNewExternalId
import org.wfanet.measurement.common.generateNewInternalId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.BatchCreateClientAccountsRequest
import org.wfanet.measurement.internal.kingdom.BatchCreateClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.batchCreateClientAccountsResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates ClientAccounts in a batch atomically within a single transaction.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] when the MeasurementConsumer is not found
 * @throws [DataProviderNotFoundException] when a DataProvider for a ClientAccount is not found
 * @throws [ClientAccountAlreadyExistsException] when a ClientAccount with the same reference ID
 *   already exists for the DataProvider
 */
class BatchCreateClientAccounts(private val request: BatchCreateClientAccountsRequest) :
  SpannerWriter<BatchCreateClientAccountsResponse, BatchCreateClientAccountsResponse>() {

  override suspend fun TransactionScope.runTransaction(): BatchCreateClientAccountsResponse {
    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    val measurementConsumerId =
      MeasurementConsumerReader.readMeasurementConsumerId(
        transactionContext,
        externalMeasurementConsumerId,
      ) ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

    val dataProviderIdMap = mutableMapOf<ExternalId, InternalId>()

    for (subRequest in request.requestsList) {
      val externalDataProviderId = ExternalId(subRequest.clientAccount.externalDataProviderId)

      val existingByReferenceId =
        ClientAccountReader()
          .readByDataProviderAndReferenceId(
            transactionContext,
            externalDataProviderId,
            subRequest.clientAccount.clientAccountReferenceId,
          )
      if (existingByReferenceId != null) {
        throw ClientAccountAlreadyExistsException(
          externalDataProviderId,
          subRequest.clientAccount.clientAccountReferenceId,
        )
      }

      // Lookup DataProvider if not already cached
      if (!dataProviderIdMap.containsKey(externalDataProviderId)) {
        val dataProviderResult =
          DataProviderReader()
            .readByExternalDataProviderId(transactionContext, externalDataProviderId)
            ?: throw DataProviderNotFoundException(externalDataProviderId)
        dataProviderIdMap[externalDataProviderId] = InternalId(dataProviderResult.dataProviderId)
      }
    }

    val createdAccounts =
      request.requestsList.map { subRequest ->
        val clientAccount = subRequest.clientAccount
        val externalDataProviderId = ExternalId(clientAccount.externalDataProviderId)
        val dataProviderId = dataProviderIdMap.getValue(externalDataProviderId)

        createClientAccount(
          measurementConsumerId,
          dataProviderId,
          externalMeasurementConsumerId,
          externalDataProviderId,
          clientAccount,
        )
      }

    return batchCreateClientAccountsResponse { clientAccounts += createdAccounts }
  }

  private suspend fun TransactionScope.createClientAccount(
    measurementConsumerId: InternalId,
    dataProviderId: InternalId,
    externalMeasurementConsumerId: ExternalId,
    externalDataProviderId: ExternalId,
    clientAccount: ClientAccount,
  ): ClientAccount {
    val internalClientAccountId =
      idGenerator.generateNewInternalId { id ->
        ClientAccountReader.existsByInternalId(transactionContext, measurementConsumerId, id)
      }

    val externalClientAccountId =
      idGenerator.generateNewExternalId { id ->
        ClientAccountReader.existsByExternalId(transactionContext, measurementConsumerId, id) ||
          ClientAccountReader()
            .readByDataProvider(transactionContext, externalDataProviderId, id) != null
      }

    transactionContext.bufferInsertMutation("ClientAccounts") {
      set("MeasurementConsumerId" to measurementConsumerId)
      set("ClientAccountId" to internalClientAccountId)
      set("ExternalClientAccountId" to externalClientAccountId)
      set("DataProviderId" to dataProviderId)
      set("ClientAccountReferenceId" to clientAccount.clientAccountReferenceId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return clientAccount.copy { this.externalClientAccountId = externalClientAccountId.value }
  }

  override fun ResultScope<BatchCreateClientAccountsResponse>.buildResult():
    BatchCreateClientAccountsResponse {
    val commitTime = commitTimestamp.toProto()
    return batchCreateClientAccountsResponse {
      clientAccounts +=
        checkNotNull(transactionResult).clientAccountsList.map {
          it.copy { createTime = commitTime }
        }
    }
  }
}
