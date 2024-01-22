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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Add an [Account] as a new owner of a [MeasurementConsumer] in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * @throws [AccountNotFoundException] Account not found
 */
class AddMeasurementConsumerOwner(
  private val externalAccountId: ExternalId,
  private val externalMeasurementConsumerId: ExternalId,
) : SimpleSpannerWriter<MeasurementConsumer>() {

  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val measurementConsumerResult = readMeasurementConsumerResult(externalMeasurementConsumerId)
    val accountId = readAccountId(externalAccountId)

    transactionContext.bufferInsertMutation("MeasurementConsumerOwners") {
      set("MeasurementConsumerId" to measurementConsumerResult.measurementConsumerId)
      set("AccountId" to accountId)
    }

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
