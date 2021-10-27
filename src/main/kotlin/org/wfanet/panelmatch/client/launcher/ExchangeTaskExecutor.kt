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
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.getAndClearTaskLog
import org.wfanet.panelmatch.client.logger.loggerFor
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.storage.createBlob

const val DONE_TASKS_PATH: String = "done-tasks"

/**
 * Maps ExchangeWorkflow.Step to respective tasks. Retrieves necessary inputs. Executes step. Stores
 * outputs.
 */
class ExchangeTaskExecutor(
  private val apiClient: ApiClient,
  private val timeout: Timeout,
  private val privateStorageSelector: PrivateStorageSelector,
  private val getExchangeTaskForStep: suspend (Step, ExchangeStepAttemptKey) -> ExchangeTask
) : ExchangeStepExecutor {
  /** Reads inputs for [step], executes [step], and writes the outputs to [privateStorage]. */
  override suspend fun execute(attemptKey: ExchangeStepAttemptKey, exchangeStep: ExchangeStep) {
    withContext(CoroutineName(attemptKey.exchangeStepAttemptId)) {
      try {
        @Suppress("BlockingMethodInNonBlockingContext") // Proto parsing is lightweight
        val workflow = ExchangeWorkflow.parseFrom(exchangeStep.serializedExchangeWorkflow)
        tryExecute(attemptKey, workflow.getSteps(exchangeStep.stepIndex))
      } catch (e: Exception) {
        logger.addToTaskLog(e.toString())
        markAsFinished(attemptKey, ExchangeStepAttempt.State.FAILED)
        throw e
      }
    }
  }

  private fun readInputs(
    step: Step,
    privateStorage: StorageClient
  ): Map<String, StorageClient.Blob> {
    return step
      .inputLabelsMap
      .mapNotNull { (k, v) -> privateStorage.getBlob(v)?.let { k to it } }
      .toMap()
  }

  private suspend fun writeOutputs(
    step: Step,
    taskOutput: Map<String, Flow<ByteString>>,
    privateStorage: StorageClient
  ) {
    for ((genericLabel, flow) in taskOutput) {
      privateStorage.createBlob(step.outputLabelsMap.getValue(genericLabel), flow)
    }
  }

  private suspend fun markAsFinished(
    attemptKey: ExchangeStepAttemptKey,
    state: ExchangeStepAttempt.State
  ) {
    apiClient.finishExchangeStepAttempt(attemptKey, state, getAndClearTaskLog())
  }

  private suspend fun tryExecute(attemptKey: ExchangeStepAttemptKey, step: Step) {
    logger.addToTaskLog("Executing $step with attempt $attemptKey")
    val privateStorageClient: StorageClient = privateStorageSelector.getStorageClient(attemptKey)
    if (!isAlreadyComplete(step, privateStorageClient)) {
      runStep(attemptKey, step, privateStorageClient)
      writeDoneBlob(step, privateStorageClient)
    }
    markAsFinished(attemptKey, ExchangeStepAttempt.State.SUCCEEDED)
  }

  private suspend fun runStep(
    attemptKey: ExchangeStepAttemptKey,
    step: Step,
    privateStorage: StorageClient
  ) {
    timeout.runWithTimeout {
      val exchangeTask: ExchangeTask = getExchangeTaskForStep(step, attemptKey)

      val taskInput: Map<String, StorageClient.Blob> = readInputs(step, privateStorage)
      val taskOutput: Map<String, Flow<ByteString>> = exchangeTask.execute(taskInput)
      writeOutputs(step, taskOutput, privateStorage)
    }
  }

  private fun isAlreadyComplete(step: Step, privateStorage: StorageClient): Boolean {
    return privateStorage.getBlob("$DONE_TASKS_PATH/${step.stepId}") != null
  }

  private suspend fun writeDoneBlob(step: Step, privateStorage: StorageClient) {
    // TODO: consider writing the state into the file.
    //  This would then require reading the file and knowing when failures are permanent.
    privateStorage.createBlob("$DONE_TASKS_PATH/${step.stepId}", ByteString.EMPTY)
  }

  companion object {
    private val logger by loggerFor()
  }
}
