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

import com.google.cloud.spanner.Mutation
import com.google.protobuf.ByteString
import com.google.type.Date
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails

class CreateExchange(
  private val recurringExchangeId: Long,
  private val externalRecurringExchangeId: ExternalId,
  private val date: Date,
  private val state: Exchange.State = Exchange.State.ACTIVE,
) : SpannerWriter<ExternalId, Exchange>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    // TODO: Set ExchangeDetails with proper Audit trail hash.
    val exchangeDetails = ExchangeDetails.getDefaultInstance()
    Mutation.newInsertBuilder("Exchanges")
      .set("RecurringExchangeId" to recurringExchangeId)
      .set("Date" to date.toCloudDate())
      .set("State" to state)
      .set("ExchangeDetails" to exchangeDetails)
      .setJson("ExchangeDetailsJson" to exchangeDetails)
      .build()
      .bufferTo(transactionContext)
    return externalRecurringExchangeId
  }

  override fun ResultScope<ExternalId>.buildResult(): Exchange {
    return Exchange.newBuilder()
      .setExternalRecurringExchangeId(externalRecurringExchangeId.value)
      .setDate(date)
      .setState(state)
      .setDetails(ExchangeDetails.newBuilder().setAuditTrailHash(ByteString.copyFromUtf8("")))
      .setSerializedRecurringExchange(ByteString.copyFromUtf8(""))
      .build()
  }
}
