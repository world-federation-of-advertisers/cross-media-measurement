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
import com.google.protobuf.Empty
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.BatchDeleteClientAccountsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Deletes ClientAccounts in a batch atomically within a single transaction.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] when the MeasurementConsumer is not found
 * @throws [ClientAccountNotFoundException] when a ClientAccount is not found
 */
class BatchDeleteClientAccounts(private val request: BatchDeleteClientAccountsRequest) :
  SimpleSpannerWriter<Empty>() {

  override suspend fun TransactionScope.runTransaction(): Empty {
    val externalMeasurementConsumerId = ExternalId(request.externalMeasurementConsumerId)
    MeasurementConsumerReader.readMeasurementConsumerId(
      transactionContext,
      externalMeasurementConsumerId,
    ) ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

    for (subRequest in request.requestsList) {
      val externalClientAccountId = ExternalId(subRequest.externalClientAccountId)
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
    }

    return Empty.getDefaultInstance()
  }
}
