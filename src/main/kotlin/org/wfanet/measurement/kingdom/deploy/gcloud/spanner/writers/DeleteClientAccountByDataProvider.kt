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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

/**
 * Deletes a [ClientAccount] from the database by DataProvider.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ClientAccountNotFoundException] ClientAccount not found
 * @throws [DataProviderNotFoundException] DataProvider not found
 */
class DeleteClientAccountByDataProvider(
  private val externalDataProviderId: ExternalId,
  private val externalClientAccountId: ExternalId,
) : SimpleSpannerWriter<ClientAccount>() {

  override suspend fun TransactionScope.runTransaction(): ClientAccount {
    DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
      ?: throw DataProviderNotFoundException(externalDataProviderId)

    val clientAccountResult =
      ClientAccountReader()
        .readByDataProvider(
          transactionContext,
          externalDataProviderId,
          externalClientAccountId,
        )
        ?: throw ClientAccountNotFoundException(
          externalDataProviderId,
          externalClientAccountId,
        )

    transactionContext.buffer(
      Mutation.delete(
        "ClientAccounts",
        KeySet.singleKey(
          Key.of(
            clientAccountResult.measurementConsumerId.value,
            clientAccountResult.clientAccountId.value,
          )
        ),
      )
    )

    return clientAccountResult.clientAccount
  }
}

