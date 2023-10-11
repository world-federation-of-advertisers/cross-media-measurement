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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.getNullableLong
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.exchangeStep

/** Reads [ExchangeStep] protos from Spanner. */
class ExchangeStepReader() : SpannerReader<ExchangeStepReader.Result>() {
  data class Result(
    val exchangeStep: ExchangeStep,
    val recurringExchangeId: Long,
    val modelProviderId: Long?,
    val dataProviderId: Long?,
  )

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalRecurringExchangeId: ExternalId,
    date: Date,
    stepIndex: Int
  ): Result? {
    fillStatementBuilder {
      val clause =
        """
        WHERE
          ExternalRecurringExchangeId = @${Params.EXTERNAL_RECURRING_EXCHANGE_ID}
          AND ExchangeSteps.Date = @${Params.DATE}
          AND ExchangeSteps.StepIndex = @${Params.STEP_INDEX}
        """
          .trimIndent()
      appendClause(clause)
      bind(Params.EXTERNAL_RECURRING_EXCHANGE_ID to externalRecurringExchangeId)
      bind(Params.DATE).to(date.toCloudDate())
      bind(Params.STEP_INDEX).to(stepIndex.toLong())
    }
    return execute(readContext).singleOrNullIfEmpty()
  }

  override val baseSql: String =
    """
    SELECT
      ExchangeSteps.RecurringExchangeId,
      ExchangeSteps.Date,
      ExchangeSteps.StepIndex,
      ExchangeSteps.State,
      ExchangeSteps.UpdateTime,
      ExchangeSteps.ModelProviderId AS StepModelProviderId,
      ExchangeSteps.DataProviderId AS StepDataProviderId,
      ModelProviders.ExternalModelProviderId,
      DataProviders.ExternalDataProviderId,
      RecurringExchanges.ExternalRecurringExchangeId,
      RecurringExchanges.RecurringExchangeDetails
    FROM
      ExchangeSteps
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

  private object Params {
    const val EXTERNAL_RECURRING_EXCHANGE_ID = "externalRecurringExchangeId"
    const val DATE = "date"
    const val STEP_INDEX = "stepIndex"
  }
}
