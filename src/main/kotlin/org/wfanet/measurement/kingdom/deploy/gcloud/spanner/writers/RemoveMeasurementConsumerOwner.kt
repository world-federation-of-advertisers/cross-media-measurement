// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerOwnerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Remove an [Account] as a new owner of a [MeasurementConsumer] from the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * @throws [AccountNotFoundException] Account not found
 * @throws [PermissionDeniedException] Permission denied due to ownership of MeasurementConsumer
 */
class RemoveMeasurementConsumerOwner(
  private val externalAccountId: ExternalId,
  private val externalMeasurementConsumerId: ExternalId
) : SimpleSpannerWriter<MeasurementConsumer>() {

  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val measurementConsumerResult = readMeasurementConsumerResult(externalMeasurementConsumerId)
    val accountId = readAccountId(externalAccountId)

    if (
      MeasurementConsumerOwnerReader()
        .checkOwnershipExist(
          transactionContext,
          internalAccountId = accountId,
          externalMeasurementConsumerId = externalMeasurementConsumerId
        ) == null
    ) {
      throw PermissionDeniedException()
    }

    transactionContext.buffer(
      Mutation.delete(
        "MeasurementConsumerOwners",
        KeySet.singleKey(Key.of(accountId.value, measurementConsumerResult.measurementConsumerId))
      )
    )

    return measurementConsumerResult.measurementConsumer
  }

  private suspend fun TransactionScope.readAccountId(externalAccountId: ExternalId): InternalId =
    AccountReader().readByExternalAccountId(transactionContext, externalAccountId)?.accountId
      ?: throw AccountNotFoundException(externalAccountId)

  private suspend fun TransactionScope.readMeasurementConsumerResult(
    externalMeasurementConsumerId: ExternalId
  ): MeasurementConsumerReader.Result =
    MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(transactionContext, externalMeasurementConsumerId)
      ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)
}
