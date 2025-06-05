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

import com.google.cloud.Date
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeDetails
import org.wfanet.measurement.internal.kingdom.exchange

/** Reads [Exchange] protos from Spanner. */
class ExchangeReader : SpannerReader<ExchangeReader.Result>() {

  data class Result(val exchange: Exchange, val recurringExchangeId: Long)

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM Exchanges
    JOIN RecurringExchanges USING (RecurringExchangeId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchange =
        exchange {
          externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
          date = struct.getDate("Date").toProtoDate()
          state = struct.getProtoEnum("State", Exchange.State::forNumber)
          details = struct.getProtoMessage("ExchangeDetails", ExchangeDetails.getDefaultInstance())
        },
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
    )
  }

  companion object {
    /** Returns a [Key] for the specified external recurring Exchange ID and date. */
    suspend fun readKeyByExternalIds(
      readContext: AsyncDatabaseClient.ReadContext,
      externalRecurringExchangeId: ExternalId,
      date: Date,
    ): Key? {
      val sql =
        """
        SELECT
          Exchanges.RecurringExchangeId AS recurringExchangeId,
          Exchanges.Date AS date
        FROM
          Exchanges
          JOIN RecurringExchanges USING (RecurringExchangeId)
        """
          .trimIndent()
      val statement =
        statement(sql) {
          appendClause(
            """
            WHERE
              ExternalRecurringExchangeId = @externalRecurringExchangeId
              AND Date = @date
            """
              .trimIndent()
          )
          bind("externalRecurringExchangeId").to(externalRecurringExchangeId.value)
          bind("date").to(date)
          appendClause("LIMIT 1")
        }

      val row: Struct =
        readContext
          .executeQuery(
            statement,
            Options.tag("reader=ExchangeReader,action=readExchangeKeyByExternalIds"),
          )
          .singleOrNull() ?: return null

      return Key.of(row.getInternalId("recurringExchangeId").value, row.getDate("date"))
    }

    private val SELECT_COLUMNS =
      listOf(
        "Exchanges.RecurringExchangeId",
        "Exchanges.Date",
        "Exchanges.State",
        "Exchanges.ExchangeDetails",
        "RecurringExchanges.ExternalRecurringExchangeId",
      )

    private val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
