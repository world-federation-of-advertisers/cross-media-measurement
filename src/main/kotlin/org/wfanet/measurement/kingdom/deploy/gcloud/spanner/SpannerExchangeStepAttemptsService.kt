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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
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
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ExchangeStepAttemptsCoroutineImplBase(coroutineContext) {

  override suspend fun getExchangeStepAttempt(
    request: GetExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    val externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId)
    val result: ExchangeStepAttemptReader.Result =
      ExchangeStepAttemptReader()
        .readByExternalIds(
          client.singleUse(),
          externalRecurringExchangeId,
          request.date,
          request.stepIndex,
          request.attemptNumber,
        )
        ?: throw ExchangeStepAttemptNotFoundException(
            externalRecurringExchangeId,
            request.date,
            request.stepIndex,
            request.attemptNumber,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    return result.exchangeStepAttempt
  }

  override suspend fun finishExchangeStepAttempt(
    request: FinishExchangeStepAttemptRequest
  ): ExchangeStepAttempt {
    grpcRequire(request.hasDate()) { "Date must be provided in the request." }
    val writer =
      FinishExchangeStepAttempt(
        externalRecurringExchangeId = ExternalId(request.externalRecurringExchangeId),
        exchangeDate = request.date,
        stepIndex = request.stepIndex,
        attemptNumber = request.attemptNumber,
        terminalState = request.state,
        debugLogEntries = request.debugLogEntriesList,
        clock = clock,
      )
    try {
      return writer.execute(client, idGenerator)
    } catch (e: RecurringExchangeNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ExchangeStepNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ExchangeStepAttemptNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }
}
