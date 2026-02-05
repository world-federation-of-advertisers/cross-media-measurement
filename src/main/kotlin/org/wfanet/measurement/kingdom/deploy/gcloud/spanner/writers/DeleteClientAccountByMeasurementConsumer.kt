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
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Deletes a [ClientAccount] from the database by MeasurementConsumer.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ClientAccountNotFoundException] ClientAccount not found
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class DeleteClientAccountByMeasurementConsumer(
  private val externalMeasurementConsumerId: ExternalId,
  private val externalClientAccountId: ExternalId,
) : SimpleSpannerWriter<ClientAccount>() {
  override suspend fun TransactionScope.runTransaction(): ClientAccount {
    MeasurementConsumerReader.readMeasurementConsumerId(
      transactionContext,
      externalMeasurementConsumerId,
    )
      ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

    val clientAccountResult =
      ClientAccountReader()
        .readByMeasurementConsumer(
          transactionContext,
          externalMeasurementConsumerId,
          externalClientAccountId,
        )
        ?: throw ClientAccountNotFoundException(
          externalMeasurementConsumerId,
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
