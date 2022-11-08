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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import java.time.Clock
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.GetExchangeStepAttemptRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepAttemptNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RecurringExchangeNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepAttemptReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FinishExchangeStepAttempt

class SpannerExchangeStepAttemptsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ExchangeStepAttemptsCoroutineImplBase() {

  override suspend fun getExchangeStepAttempt(
    request: GetExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    val externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId)
    val result =
      ExchangeStepAttemptReader()
        .fillStatementBuilder {
          appendClause(
            """
            WHERE RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id
              AND ExchangeStepAttempts.Date = @date
              AND ExchangeStepAttempts.StepIndex = @step_index
              AND ExchangeStepAttempts.AttemptIndex = @attempt_index
            """
              .trimIndent()
          )
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
          when (request.provider.type) {
            Provider.Type.DATA_PROVIDER ->
              appendClause(
                """
                |  AND ExchangeSteps.DataProviderId = (
                |    SELECT DataProviderId
                |    FROM DataProviders
                |    WHERE ExternalDataProviderId = @external_provider_id
                |  )
                """
                  .trimMargin()
              )
            Provider.Type.MODEL_PROVIDER ->
              appendClause(
                """
                |  AND ExchangeSteps.ModelProviderId = (
                |    SELECT ModelProviderId
                |    FROM ModelProviders
                |    WHERE ExternalModelProviderId = @external_provider_id
                |  )
                """
                  .trimMargin()
              )
            Provider.Type.TYPE_UNSPECIFIED,
            Provider.Type.UNRECOGNIZED ->
              failGrpc(Status.INVALID_ARGUMENT) {
                "external_data_provider_id or external_model_provider_id must be provided."
              }
          }

          bind("external_recurring_exchange_id" to externalRecurringExchangeId)
          bind("date" to request.date.toCloudDate())
          bind("step_index" to request.stepIndex.toLong())
          bind("attempt_index" to request.attemptNumber.toLong())
          bind("external_provider_id" to request.provider.externalId)
          appendClause("LIMIT 1")
        }
        .execute(client.singleUse())
        .singleOrNull()
        ?: ExchangeStepAttemptNotFoundException(
            externalRecurringExchangeId,
            request.date,
            request.stepIndex,
            request.attemptNumber
          )
          .throwStatusRuntimeException(Status.NOT_FOUND)
    return result.exchangeStepAttempt
  }

  override suspend fun finishExchangeStepAttempt(
    request: FinishExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    grpcRequire(request.hasDate()) { "Date must be provided in the request." }
    val writer =
      FinishExchangeStepAttempt(
        provider = request.provider,
        externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId),
        exchangeDate = request.date,
        stepIndex = request.stepIndex,
        attemptNumber = request.attemptNumber,
        terminalState = request.state,
        debugLogEntries = request.debugLogEntriesList,
        clock = clock
      )
    try {
      return writer.execute(client, idGenerator)
    } catch (e: RecurringExchangeNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND)
    } catch (e: ExchangeStepNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND)
    } catch (e: ExchangeStepAttemptNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND)
    }
  }
}
