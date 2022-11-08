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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.exchange

/** Reads [Exchange] protos from Spanner. */
class ExchangeReader : SpannerReader<ExchangeReader.Result>() {

  data class Result(val exchange: Exchange, val recurringExchangeId: Long)

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM Exchanges
    JOIN RecurringExchanges USING (RecurringExchangeId)
    LEFT JOIN ModelProviders USING (ModelProviderId)
    LEFT JOIN DataProviders USING (DataProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchange =
        exchange {
          externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
          date = struct.getDate("Date").toProtoDate()
          state = struct.getProtoEnum("State", Exchange.State::forNumber)
          details = struct.getProtoMessage("ExchangeDetails", ExchangeDetails.parser())
          serializedRecurringExchange =
            struct
              .getProtoMessage("RecurringExchangeDetails", RecurringExchangeDetails.parser())
              .externalExchangeWorkflow
        },
      recurringExchangeId = struct.getLong("RecurringExchangeId")
    )
  }

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "Exchanges.RecurringExchangeId",
        "Exchanges.Date",
        "Exchanges.State",
        "Exchanges.ExchangeDetails",
        "Exchanges.ExchangeDetailsJson",
        "RecurringExchanges.ExternalRecurringExchangeId",
        "RecurringExchanges.RecurringExchangeDetails"
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
