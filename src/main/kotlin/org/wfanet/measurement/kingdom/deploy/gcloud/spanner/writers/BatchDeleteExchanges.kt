/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.internal.kingdom.BatchDeleteExchangesRequest
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeReader

/**
 * Permanently deletes [Exchange]s. Operation will fail for all [Exchange]s when one is not found.
 *
 * Throws the following [KingdomInternalException] type on [execute]:
 * * [ExchangeNotFoundException] when the Exchange is not found
 */
class BatchDeleteExchanges(private val requests: BatchDeleteExchangesRequest) :
  SimpleSpannerWriter<Empty>() {

  override suspend fun TransactionScope.runTransaction(): Empty {
    val keySet = KeySet.newBuilder()

    for (request in requests.requestsList) {
      val externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId)
      val result: Key =
        ExchangeReader.readKeyByExternalIds(
          transactionContext,
          externalRecurringExchangeId,
          request.date.toCloudDate(),
        )
          ?: throw ExchangeNotFoundException(externalRecurringExchangeId, request.date) {
            "Exchange with external RecurringExchange ID $externalRecurringExchangeId and date" +
              "${request.date} not found"
          }

      keySet.addKey(result)
    }

    transactionContext.buffer(Mutation.delete("Exchanges", keySet.build()))

    return Empty.getDefaultInstance()
  }
}
