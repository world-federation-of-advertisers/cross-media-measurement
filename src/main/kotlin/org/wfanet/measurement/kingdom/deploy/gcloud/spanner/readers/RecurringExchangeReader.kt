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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.recurringExchange

/** Reads [Exchange] protos from Spanner. */
class RecurringExchangeReader(recurringExchangesIndex: Index = Index.NONE) :
  SpannerReader<RecurringExchangeReader.Result>() {
  data class Result(
    val recurringExchange: RecurringExchange,
    val recurringExchangeId: Long,
    val modelProviderId: Long,
    val dataProviderId: Long
  )

  enum class Index(internal val sql: String) {
    NONE(""),
    DATA_PROVIDER_ID("@{FORCE_INDEX=RecurringExchangesByDataProviderId}"),
    MODEL_PROVIDER_ID("@{FORCE_INDEX=RecurringExchangesByModelProviderId}"),
    EXTERNAL_ID("@{FORCE_INDEX=RecurringExchangesByExternalId}"),
    NEXT_EXCHANGE_DATE("@{FORCE_INDEX=RecurringExchangesByNextExchangeDate}"),
  }

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM RecurringExchanges${recurringExchangesIndex.sql}
    JOIN ModelProviders USING (ModelProviderId)
    JOIN DataProviders USING (DataProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result {
    return Result(
      recurringExchange = buildRecurringExchange(struct),
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
      modelProviderId = struct.getLong("ModelProviderId"),
      dataProviderId = struct.getLong("DataProviderId")
    )
  }

  suspend fun readByExternalRecurringExchangeId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalRecurringExchangeId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE ExternalRecurringExchangeId = @externalRecurringExchangeId")
        bind("externalRecurringExchangeId").to(externalRecurringExchangeId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  private fun buildRecurringExchange(struct: Struct): RecurringExchange {
    return recurringExchange {
      externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
      externalModelProviderId = struct.getLong("ExternalModelProviderId")
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      state = struct.getProtoEnum("State", RecurringExchange.State::forNumber)
      nextExchangeDate = struct.getDate("NextExchangeDate").toProtoDate()
      details =
        struct.getProtoMessage("RecurringExchangeDetails", RecurringExchangeDetails.parser())
    }
  }

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "RecurringExchanges.RecurringExchangeId",
        "RecurringExchanges.ExternalRecurringExchangeId",
        "RecurringExchanges.ModelProviderId",
        "RecurringExchanges.DataProviderId",
        "RecurringExchanges.State",
        "RecurringExchanges.NextExchangeDate",
        "RecurringExchanges.RecurringExchangeDetails",
        "RecurringExchanges.RecurringExchangeDetailsJson",
        "DataProviders.ExternalDataProviderId",
        "ModelProviders.ExternalModelProviderId"
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
