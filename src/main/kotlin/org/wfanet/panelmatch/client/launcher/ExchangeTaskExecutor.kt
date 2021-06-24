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

import com.google.protobuf.ByteString
import java.time.Duration
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.getAndClearTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.storage.Storage

/**
 * Maps ExchangeWorkflow.Step to respective tasks. Retrieves necessary inputs. Executes step. Stores
 * outputs.
 *
 * @param ExchangeWorkflow.Step to execute.
 * @param input inputs needed by all [task]s.
 * @param storage the Storage class to store the intermediary steps
 * @return mapped output.
 */
class ExchangeTaskExecutor(
  private val apiClient: ApiClient,
  private val timeoutDuration: Duration = Duration.ofDays(1),
  private val sharedStorage: Storage,
  private val privateStorage: Storage,
  private val getExchangeTaskForStep: suspend (ExchangeWorkflow.Step) -> ExchangeTask
) {

  /**
   * Reads private and shared task input from different storage based on [exchangeKey] and
   * [ExchangeStep] and returns Map<String, ByteString> of different input
   */
  private suspend fun getAllInputForStep(
    sharedStorage: Storage,
    privateStorage: Storage,
    step: ExchangeWorkflow.Step
  ): Map<String, ByteString> = coroutineScope {
    val privateInputLabels = step.getPrivateInputLabelsMap()
    val sharedInputLabels = step.getSharedInputLabelsMap()
    awaitAll(
      async(start = CoroutineStart.DEFAULT) {
        privateStorage.batchRead(inputLabels = privateInputLabels)
      },
      async(start = CoroutineStart.DEFAULT) {
        sharedStorage.batchRead(inputLabels = sharedInputLabels)
      }
    )
      .reduce { a, b -> a.toMutableMap().apply { putAll(b) } }
  }

  /**
   * Writes private and shared task output [taskOutput] to different storage based on [exchangeKey]
   * and [ExchangeStep].
   */
  private suspend fun writeAllOutputForStep(
    sharedStorage: Storage,
    privateStorage: Storage,
    step: ExchangeWorkflow.Step,
    taskOutput: Map<String, ByteString>
  ) =
    coroutineScope<Unit> {
      val privateOutputLabels = step.getPrivateOutputLabelsMap()
      val sharedOutputLabels = step.getSharedOutputLabelsMap()
      async { privateStorage.batchWrite(outputLabels = privateOutputLabels, data = taskOutput) }
      async { sharedStorage.batchWrite(outputLabels = sharedOutputLabels, data = taskOutput) }
    }

  suspend fun execute(attemptKey: ExchangeStepAttemptKey, step: ExchangeWorkflow.Step) =
      coroutineScope {
    try {
      logger.addToTaskLog("Executing $step with attempt $attemptKey")
      val exchangeTask: ExchangeTask = getExchangeTaskForStep(step)
      withTimeout(timeoutDuration.toMillis()) {
        // TODO - get rid of this extra if and fold the INPUT_STEP into the normal process for all
        // steps
        if (step.getStepCase() == ExchangeWorkflow.Step.StepCase.INPUT_STEP) {
          exchangeTask.execute(emptyMap<String, ByteString>())
        } else {
          val taskInput: Map<String, ByteString> =
            getAllInputForStep(
              sharedStorage = sharedStorage,
              privateStorage = privateStorage,
              step = step
            )
          val taskOutput: Map<String, ByteString> = exchangeTask.execute(taskInput)
          writeAllOutputForStep(
            sharedStorage = sharedStorage,
            privateStorage = privateStorage,
            step = step,
            taskOutput = taskOutput
          )
        }
      }
      apiClient.finishExchangeStepAttempt(
        attemptKey,
        ExchangeStepAttempt.State.SUCCEEDED,
        logger.getAndClearTaskLog()
      )
    } catch (e: Exception) {
      logger.addToTaskLog(e.toString())
      apiClient.finishExchangeStepAttempt(
        attemptKey,
        ExchangeStepAttempt.State.FAILED,
        logger.getAndClearTaskLog()
      )
      throw e
    }
  }

  companion object {
    val logger by loggerFor()
  }
}
