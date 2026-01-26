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

import com.google.cloud.spanner.ErrorCode as SpannerErrorCode
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.generateNewExternalId
import org.wfanet.measurement.common.generateNewInternalId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

class CreateClientAccount(private val clientAccount: ClientAccount) :
  SpannerWriter<ClientAccount, ClientAccount>() {
  override suspend fun TransactionScope.runTransaction(): ClientAccount {
    val externalMeasurementConsumerId = ExternalId(clientAccount.externalMeasurementConsumerId)
    val measurementConsumerId =
      MeasurementConsumerReader.readMeasurementConsumerId(
        transactionContext,
        externalMeasurementConsumerId,
      ) ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

    val externalDataProviderId = ExternalId(clientAccount.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = InternalId(dataProviderResult.dataProviderId)

    val existingByReferenceId =
      ClientAccountReader()
        .readByDataProviderAndReferenceId(
          transactionContext,
          externalDataProviderId,
          clientAccount.clientAccountReferenceId,
        )
    if (existingByReferenceId != null) {
      throw ClientAccountAlreadyExistsException(
        externalDataProviderId,
        clientAccount.clientAccountReferenceId,
      )
    }

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

  override fun ResultScope<ClientAccount>.buildResult(): ClientAccount {
    return checkNotNull(transactionResult).copy { createTime = commitTimestamp.toProto() }
  }

  override suspend fun handleSpannerException(e: SpannerException): ClientAccount? {
    when (e.errorCode) {
      SpannerErrorCode.ALREADY_EXISTS ->
        throw ClientAccountAlreadyExistsException(
          ExternalId(clientAccount.externalDataProviderId),
          clientAccount.clientAccountReferenceId,
          e,
        )
      else -> throw e
    }
  }
}
