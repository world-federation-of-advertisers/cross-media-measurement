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
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskFailedException
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step
import org.wfanet.panelmatch.client.logger.TaskLog
import org.wfanet.panelmatch.client.logger.addToTaskLog
import org.wfanet.panelmatch.client.logger.getAndClearTaskLog
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.common.Timeout

private const val DONE_TASKS_PATH: String = "done-tasks"

/**
 * Validates and Executes the work required for [ApiClient.ClaimedExchangeStep]s.
 *
 * This involves finding the appropriate [ExchangeTask], reading the inputs, executing the
 * [ExchangeTask], and saving the outputs.
 */
class ExchangeTaskExecutor(
  private val apiClient: ApiClient,
  private val timeout: Timeout,
  private val privateStorageSelector: PrivateStorageSelector,
  private val exchangeTaskMapper: ExchangeTaskMapper,
  private val validator: ExchangeStepValidator,
) : ExchangeStepExecutor {

  override suspend fun execute(exchangeStep: ApiClient.ClaimedExchangeStep) {
    val attemptKey = exchangeStep.attemptKey
    withContext(CoroutineName(attemptKey.toString()) + TaskLog(attemptKey.toString())) {
      try {
        val validatedStep = validator.validate(exchangeStep)
        val context =
          ExchangeContext(
            attemptKey,
            validatedStep.date,
            validatedStep.workflow,
            validatedStep.step,
          )
        context.tryExecute()
      } catch (e: Exception) {
        logger.addToTaskLog("Caught Exception in task execution:", Level.SEVERE)
        logger.addToTaskLog(e, Level.SEVERE)
        val attemptState =
          when (e) {
            is ExchangeTaskFailedException -> e.attemptState
            else -> ExchangeStepAttempt.State.FAILED
          }
        markAsFinished(attemptKey, attemptState)
      }
    }
  }

  private suspend fun readInputs(step: Step, privateStorage: StorageClient): Map<String, Blob> {
    return step.inputLabelsMap.mapValues { (label, blobKey) ->
      requireNotNull(privateStorage.getBlob(blobKey)) {
        "Missing blob key '$blobKey' for input label '$label'"
      }
    }
  }

  private suspend fun writeOutputs(
    step: Step,
    taskOutput: Map<String, Flow<ByteString>>,
    privateStorage: StorageClient,
  ) {
    for ((genericLabel, flow) in taskOutput) {
      val blobKey =
        requireNotNull(step.outputLabelsMap[genericLabel]) {
          "Missing $genericLabel in outputLabels for step: $step"
        }
      privateStorage.writeBlob(blobKey, flow)
    }
  }

  private suspend fun markAsFinished(
    attemptKey: ExchangeStepAttemptKey,
    state: ExchangeStepAttempt.State,
  ) {
    logger.addToTaskLog("Marking attempt state: $state")
    apiClient.finishExchangeStepAttempt(attemptKey, state, getAndClearTaskLog())
  }

  private suspend fun ExchangeContext.tryExecute() {
    logger.addToTaskLog("Executing ${step.stepId} with attempt $attemptKey for $this")
    val privateStorageClient: StorageClient =
      privateStorageSelector.getStorageClient(exchangeDateKey)
    if (!isAlreadyComplete(step, privateStorageClient)) {
      logger.log(Level.FINE, "Running Step")
      runStep(privateStorageClient)
      logger.log(Level.FINE, "Writing Done Blob")
      writeDoneBlob(step, privateStorageClient)
    }
    // The Kingdom will be able to detect if it's handing out duplicate tasks because it will
    // attempt to transition an ExchangeStep from some terminal state to `SUCCEEDED`.
    markAsFinished(attemptKey, ExchangeStepAttempt.State.SUCCEEDED)
  }

  private suspend fun ExchangeContext.runStep(privateStorage: StorageClient) {
    timeout.runWithTimeout {
      val exchangeTask: ExchangeTask = exchangeTaskMapper.getExchangeTaskForStep(this@runStep)
      logger.addToTaskLog("Reading Inputs", Level.FINE)
      val taskInput: Map<String, Blob> =
        if (exchangeTask.skipReadInput()) emptyMap() else readInputs(step, privateStorage)
      logger.addToTaskLog("Executing", Level.FINE)
      val taskOutput: Map<String, Flow<ByteString>> = exchangeTask.execute(taskInput)
      logger.addToTaskLog("Writing Outputs", Level.FINE)
      writeOutputs(step, taskOutput, privateStorage)
    }
  }

  private suspend fun isAlreadyComplete(step: Step, privateStorage: StorageClient): Boolean {
    return privateStorage.getBlob("$DONE_TASKS_PATH/${step.stepId}") != null
  }

  private suspend fun writeDoneBlob(step: Step, privateStorage: StorageClient) {
    // TODO: write the state into the blob.
    //   This will prevent re-execution of tasks that failed.
    privateStorage.writeBlob("$DONE_TASKS_PATH/${step.stepId}", ByteString.EMPTY)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
