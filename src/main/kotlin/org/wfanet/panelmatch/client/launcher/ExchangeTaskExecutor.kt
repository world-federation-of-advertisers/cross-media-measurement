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
import java.util.logging.Level
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.measurement.storage.createBlob
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.exchangetasks.CustomIOExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep
import org.wfanet.panelmatch.client.logger.TaskLog
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.getAndClearTaskLog
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.storage.createOrReplaceBlob

private const val DONE_TASKS_PATH: String = "done-tasks"

/**
 * Executes the work required for [ValidatedExchangeStep]s.
 *
 * This involves finding the appropriate [ExchangeTask], reading the inputs, executing the
 * [ExchangeTask], and saving the outputs.
 */
class ExchangeTaskExecutor(
  private val apiClient: ApiClient,
  private val timeout: Timeout,
  private val privateStorageSelector: PrivateStorageSelector,
  private val exchangeTaskMapper: ExchangeTaskMapper
) : ExchangeStepExecutor {

  override suspend fun execute(step: ValidatedExchangeStep, attemptKey: ExchangeStepAttemptKey) {
    val name = "${step.step.stepId}@${attemptKey.toName()}"
    withContext(TaskLog(name)) {
      val context = ExchangeContext(attemptKey, step.date, step.workflow, step.step)
      try {
        context.tryExecute()
      } catch (e: Exception) {
        logger.addToTaskLog(e, Level.SEVERE)
        markAsFinished(attemptKey, ExchangeStepAttempt.State.FAILED)
        throw e
      }
    }
  }

  private fun readInputs(step: Step, privateStorage: StorageClient): Map<String, Blob> {
    return step.inputLabelsMap.mapValues { (label, blobKey) ->
      requireNotNull(privateStorage.getBlob(blobKey)) {
        "Missing blob key '$blobKey' for input label '$label'"
      }
    }
  }

  private suspend fun writeOutputs(
    step: Step,
    taskOutput: Map<String, Flow<ByteString>>,
    privateStorage: StorageClient
  ) {
    for ((genericLabel, flow) in taskOutput) {
      val blobKey =
        requireNotNull(step.outputLabelsMap[genericLabel]) {
          "Missing $genericLabel in outputLabels for step: $step"
        }
      privateStorage.createOrReplaceBlob(blobKey, flow)
    }
  }

  private suspend fun markAsFinished(
    attemptKey: ExchangeStepAttemptKey,
    state: ExchangeStepAttempt.State
  ) {
    logger.addToTaskLog("Marking attempt state: $state")
    apiClient.finishExchangeStepAttempt(attemptKey, state, getAndClearTaskLog())
  }

  private suspend fun ExchangeContext.tryExecute() {
    logger.addToTaskLog("Executing ${step.stepId} with attempt $attemptKey for $this")
    val privateStorageClient: StorageClient =
      privateStorageSelector.getStorageClient(exchangeDateKey)
    if (!isAlreadyComplete(step, privateStorageClient)) {
      runStep(privateStorageClient)
      writeDoneBlob(step, privateStorageClient)
    }
    // The Kingdom will be able to detect if it's handing out duplicate tasks because it will
    // attempt to transition an ExchangeStep from some terminal state to `SUCCEEDED`.
    markAsFinished(attemptKey, ExchangeStepAttempt.State.SUCCEEDED)
  }

  private suspend fun ExchangeContext.runStep(privateStorage: StorageClient) {
    timeout.runWithTimeout {
      val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(this@runStep)
      val taskInput: Map<String, Blob> =
        when (exchangeTask) {
          is CustomIOExchangeTask -> emptyMap()
          else -> readInputs(step, privateStorage)
        }
      val taskOutput: Map<String, Flow<ByteString>> = exchangeTask.execute(taskInput)
      writeOutputs(step, taskOutput, privateStorage)
    }
  }

  private fun isAlreadyComplete(step: Step, privateStorage: StorageClient): Boolean {
    return privateStorage.getBlob("$DONE_TASKS_PATH/${step.stepId}") != null
  }

  private suspend fun writeDoneBlob(step: Step, privateStorage: StorageClient) {
    // TODO: write the state into the blob.
    //   This will prevent re-execution of tasks that failed.
    privateStorage.createBlob("$DONE_TASKS_PATH/${step.stepId}", ByteString.EMPTY)
  }

  companion object {
    private val logger by loggerFor()
  }
}
