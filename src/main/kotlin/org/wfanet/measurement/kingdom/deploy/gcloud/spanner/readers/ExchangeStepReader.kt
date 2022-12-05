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
import org.wfanet.measurement.gcloud.spanner.getNullableLong
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.exchangeStep

/** Reads [ExchangeStep] protos from Spanner. */
class ExchangeStepReader(exchangeStepsIndex: Index = Index.NONE) :
  SpannerReader<ExchangeStepReader.Result>() {
  data class Result(
    val exchangeStep: ExchangeStep,
    val recurringExchangeId: Long,
    val modelProviderId: Long?,
    val dataProviderId: Long?,
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
    JOIN ModelProviders ON (RecurringExchanges.ModelProviderId = ModelProviders.ModelProviderId)
    JOIN DataProviders ON (RecurringExchanges.DataProviderId = DataProviders.DataProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchangeStep = buildExchangeStep(struct),
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
      modelProviderId = struct.getNullableLong("StepModelProviderId"),
      dataProviderId = struct.getNullableLong("StepDataProviderId"),
    )
  }

  private fun buildExchangeStep(struct: Struct): ExchangeStep {
    return exchangeStep {
      externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
      date = struct.getDate("Date").toProtoDate()
      stepIndex = struct.getLong("StepIndex").toInt()
      state = struct.getProtoEnum("State", ExchangeStep.State::forNumber)
      provider = buildProvider(struct)
      updateTime = struct.getTimestamp("UpdateTime").toProto()
      serializedExchangeWorkflow =
        struct
          .getProtoMessage("RecurringExchangeDetails", RecurringExchangeDetails.parser())
          .externalExchangeWorkflow
    }
  }

  private fun buildProvider(struct: Struct): Provider {
    return when {
      !struct.isNull("StepModelProviderId") ->
        provider {
          externalId = struct.getLong("ExternalModelProviderId")
          type = Provider.Type.MODEL_PROVIDER
        }
      !struct.isNull("StepDataProviderId") ->
        provider {
          externalId = struct.getLong("ExternalDataProviderId")
          type = Provider.Type.DATA_PROVIDER
        }
      else -> error("No Provider found")
    }
  }

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "ExchangeSteps.RecurringExchangeId",
        "ExchangeSteps.Date",
        "ExchangeSteps.StepIndex",
        "ExchangeSteps.State",
        "ExchangeSteps.UpdateTime",
        "ExchangeSteps.ModelProviderId AS StepModelProviderId",
        "ExchangeSteps.DataProviderId AS StepDataProviderId",
        "ModelProviders.ExternalModelProviderId",
        "DataProviders.ExternalDataProviderId",
        "RecurringExchanges.ExternalRecurringExchangeId",
        "RecurringExchanges.RecurringExchangeDetails",
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")
  }
}
