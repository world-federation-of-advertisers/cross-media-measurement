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
import com.google.type.Date
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails

/** Reads [Exchange] protos from Spanner. */
class ExchangeReader(index: Index = Index.NONE) : SpannerReader<ExchangeReader.Result>() {
  data class Result(
    val exchange: Exchange,
    val recurringExchangeId: Long,
    val dataProviderId: Long,
    val modelProviderId: Long,
    val recurringExchangeDetails: RecurringExchangeDetails
  )

  enum class Index(internal val sql: String) {
    NONE(""),
    DATA_PROVIDER_ID("@{FORCE_INDEX=RecurringExchangesByDataProviderId}"),
    MODEL_PROVIDER_ID("@{FORCE_INDEX=RecurringExchangesByModelProviderId}"),
    EXTERNAL_ID("@{FORCE_INDEX=RecurringExchangesByExternalId}")
  }

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM RecurringExchanges${index.sql}
    JOIN Exchanges USING (RecurringExchangeId)
    JOIN DataProviders USING (DataProviderId)
    JOIN ModelProviders USING (ModelProviderId)
    """.trimIndent()

  override val externalIdColumn: String = "RecurringExchanges.ExternalRecurringExchangeId"

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchange = buildExchange(struct),
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
      dataProviderId = struct.getLong("DataProviderId"),
      modelProviderId = struct.getLong("ModelProviderId"),
      recurringExchangeDetails = struct.getProtoMessage(
        "RecurringExchangeDetails",
        RecurringExchangeDetails.parser()
      )
    )
  }

  private fun buildExchange(struct: Struct): Exchange =
    Exchange.newBuilder()
      .apply {
        date = struct.getDate("Date").toProtoDate()
        externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
        details = struct.getProtoMessage("ExchangeDetails", ExchangeDetails.parser())
        state = struct.getProtoEnum("State", Exchange.State::forNumber)
      }
      .build()

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "RecurringExchanges.RecurringExchangeId",
        "RecurringExchanges.ExternalRecurringExchangeId",
        "RecurringExchanges.ModelProviderId",
        "RecurringExchanges.DataProviderId",
        "RecurringExchanges.RecurringExchangeDetails",
        "RecurringExchanges.RecurringExchangeDetailsJson",
        "DataProviders.ExternalDataProviderId",
        "ModelProviders.ExternalModelProviderId",
        "Exchanges.State",
        "Exchanges.Date",
        "Exchanges.ExchangeDetails",
        "Exchanges.ExchangeDetailsJson"
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }

  /** Converts Cloud Spanner Date to [Date]. */
  fun com.google.cloud.Date.toProtoDate(): Date =
    Date.newBuilder().apply {
      year = this.year
      month = this.month
      day = this.day
    }.build()
}
