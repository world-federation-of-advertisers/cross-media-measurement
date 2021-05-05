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

package org.wfanet.panelmatch.client.launcher

import com.google.protobuf.Timestamp
import java.time.Clock
import java.time.Instant
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FindReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.FindReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest

/** Finds an [ExchangeStep], validates it, and starts executing the work. */
class ExchangeStepLauncher(
  private val exchangeStepsClient: ExchangeStepsCoroutineStub,
  private val id: String,
  private val partyType: PartyType,
  private val clock: Clock = Clock.systemUTC()
) {

  /**
   * Finds a single ready Exchange Step and starts executing. If an Exchange Step is found,
   * validates it, and starts executing. If not found simply returns.
   */
  suspend fun findAndRunExchangeStep() {
    val exchangeStep = findExchangeStep() ?: return
    try {
      validateExchangeStep(exchangeStep)
    } catch (e: InvalidExchangeStepException) {
      // TODO(@yunyeng): Catch exceptions while creating the attempt.
      val attempt = createExchangeStepAttempt(exchangeStep)
      // TODO(@yunyeng): Catch exceptions while finishing the attempt.
      finishExchangeStepAttempt(
        FinishExchangeStepAttemptRequest.newBuilder()
          .apply {
            key = attempt.key
            finalState = ExchangeStepAttempt.State.FAILED_STEP
            addLogEntriesBuilder().apply {
              time = clock.instant().toProtoTime()
              message = e.message
            }
          }
          .build()
      )
    }
    runExchangeStep(exchangeStep)
  }

  /**
   * Finds a single Exchange Step from ExchangeSteps service.
   *
   * @return an [ExchangeStep] or null.
   */
  internal suspend fun findExchangeStep(): ExchangeStep? {
    val request: FindReadyExchangeStepRequest =
      FindReadyExchangeStepRequest.newBuilder()
        .apply {
          when (partyType) {
            PartyType.DATA_PROVIDER -> dataProviderBuilder.dataProviderId = id
            PartyType.MODEL_PROVIDER -> modelProviderBuilder.modelProviderId = id
          }
        }
        .build()
    // Call /ExchangeSteps.findReadyExchangeStep to a find work to do.
    val response: FindReadyExchangeStepResponse = exchangeStepsClient.findReadyExchangeStep(request)
    if (response.hasExchangeStep()) {
      return response.exchangeStep
    }
    return null
  }

  /**
   * Starts executing the given Exchange Step.
   *
   * @param exchangeStep [ExchangeStep].
   */
  internal fun runExchangeStep(exchangeStep: ExchangeStep) {
    // TODO(@yunyeng): Start JobStarter with the exchangeStep.
  }

  /**
   * Validates the given Exchange Step.
   *
   * @param exchangeStep [ExchangeStep].
   * @throws InvalidExchangeStepException if the Exchange Step is not valid.
   */
  internal fun validateExchangeStep(exchangeStep: ExchangeStep) {
    // Validate that this exchange step is legal, otherwise throw an error.
    // TODO(@yunyeng): Add validation logic.
  }

  /**
   * Creates an Exchange Step Attempt for the Exchange Step given.
   *
   * @param exchangeStep [ExchangeStep].
   * @return an [ExchangeStepAttempt] or null.
   */
  internal fun createExchangeStepAttempt(exchangeStep: ExchangeStep): ExchangeStepAttempt {
    // TODO(@yunyeng): Set ExchangeStepAttempt, call /ExchangeStepAttempts.createExchangeStepAttempt
    return ExchangeStepAttempt.getDefaultInstance()
  }

  /**
   * Finishes the Exchange Step Attempt with the given request.
   *
   * @param request [FinishExchangeStepAttemptRequest].
   */
  internal fun finishExchangeStepAttempt(request: FinishExchangeStepAttemptRequest) {
    // TODO(@yunyeng): Call /ExchangeStepAttempts.finishExchangeStepAttempt.
  }
}

/** Specifies the party type of the input id for [ExchangeStepLauncher]. */
enum class PartyType {
  /** Id belongs to a Data Provider. */
  DATA_PROVIDER,

  /** Id belongs to a Model Provider. */
  MODEL_PROVIDER,
}

/** Indicates that given Exchange Step is not valid to execute. */
class InvalidExchangeStepException(cause: Throwable) : Exception(cause)

// TODO(@yunyeng): Import from cross-media-measurement/ProtoUtils.
/** Converts Instant to Timestamp. */
fun Instant.toProtoTime(): Timestamp =
  Timestamp.newBuilder().setSeconds(epochSecond).setNanos(nano).build()
