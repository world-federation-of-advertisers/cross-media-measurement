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
import org.wfanet.measurement.internal.kingdom.ExchangeStep

/** Reads [ExchangeStep] protos from Spanner. */
class ExchangeStepReader(exchangeStepsIndex: Index = Index.NONE) :
  SpannerReader<ExchangeStepReader.Result>() {
  data class Result(
    val exchangeStep: ExchangeStep,
    val recurringExchangeId: Long,
    val modelProviderId: Long,
    val dataProviderId: Long,
  )

  enum class Index(internal val sql: String) {
    NONE(""),
    DATA_PROVIDER_ID("@{FORCE_INDEX=ExchangeStepsByDataProviderId}"),
    MODEL_PROVIDER_ID("@{FORCE_INDEX=ExchangeStepsByModelProviderId}"),
  }

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM ExchangeSteps${exchangeStepsIndex.sql}
    JOIN RecurringExchanges USING (RecurringExchangeId)
    JOIN ModelProviders USING (ModelProviderId)
    JOIN DataProviders USING (DataProviderId)
    """.trimIndent()

  override val externalIdColumn: String = "ExchangeSteps.ExternalRecurringExchangeId"

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchangeStep = buildExchangeStep(struct),
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
      modelProviderId = struct.getLong("ModelProviderId"),
      dataProviderId = struct.getLong("DataProviderId"),
    )
  }

  private fun buildExchangeStep(struct: Struct): ExchangeStep {
    return ExchangeStep.newBuilder()
      .apply {
        externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
        date = struct.getDate("Date").toProtoDate()
        stepIndex = struct.getLong("StepIndex").toInt()
        state = struct.getProtoEnum("State", ExchangeStep.State::forNumber)
        externalModelProviderId = struct.getLong("ExternalModelProviderId")
        externalDataProviderId = struct.getLong("ExternalDataProviderId")
      }
      .build()
  }

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "ExchangeSteps.RecurringExchangeId",
        "ExchangeSteps.ExternalRecurringExchangeId",
        "ExchangeSteps.Date",
        "ExchangeSteps.StepIndex",
        "ExchangeSteps.State",
        "ExchangeSteps.UpdateTime",
        "ExchangeSteps.ModelProviderId",
        "ExchangeSteps.DataProviderId",
        "ModelProviders.ExternalModelProviderId",
        "DataProviders.ExternalDataProviderId",
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
